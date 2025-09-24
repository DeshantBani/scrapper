# scraper/aggregates.py
"""Aggregates scraper - extracts Engine and Frame groups for each vehicle (robust & per-row typing)."""
import logging
from typing import List, Optional

from playwright.async_api import Page, TimeoutError as PWTimeoutError

from .datamodel import Vehicle, TableIndex
from .browser import wait_for_selector_any, wait_for_datatables, evaluate

logger = logging.getLogger(__name__)
MODEL_READY_TIMEOUT = 45_000

# --- Ensure DataTables returns ALL rows (not just 25) ---
PREPARE_SHOW_ALL_JS = """
() => new Promise(resolve => {
  const $ = window.jQuery || window.$;
  if (!($ && $.fn && $.fn.DataTable)) { resolve(false); return; }
  const ids = ['#DataTables_Table_0', '#DataTables_Table_1', '#DataTables_Table_2', '#DataTables_Table_3'];
  let pending = 0;
  const done = () => { if (--pending <= 0) resolve(true); };

  ids.forEach(sel => {
    try {
      const dt = $(sel).DataTable();
      if (dt) {
        pending++;
        dt.one('draw.dt', () => setTimeout(done, 50));
        // -1 => "All" (client-side tables). If UI lacks "All", API still accepts it and returns all rows.
        dt.page.len(-1).draw(false);
      }
    } catch (e) {}
  });

  if (pending === 0) resolve(false);
});
"""

# --- (Optional) quick info for debugging counts/length ---
COUNTS_INFO_JS = """
() => {
  const $ = window.jQuery || window.$;
  if (!($ && $.fn && $.fn.DataTable)) return {};
  const probe = sel => {
    try {
      const dt = $(sel).DataTable();
      if (!dt) return null;
      const info = dt.page.info();
      return { recordsTotal: info.recordsTotal, recordsDisplay: info.recordsDisplay, length: info.length };
    } catch (_) { return null; }
  };
  return {
    '#DataTables_Table_0': probe('#DataTables_Table_0'),
    '#DataTables_Table_1': probe('#DataTables_Table_1'),
    '#DataTables_Table_2': probe('#DataTables_Table_2'),
    '#DataTables_Table_3': probe('#DataTables_Table_3'),
  };
}
"""

# --- Get ALL rows from a client-side DataTable (list view) ---
EXTRACT_LIST_VIEW_JS = """
(sel) => {
  const $ = window.jQuery || window.$;
  if (!document.querySelector(sel) || !($ && $.fn && $.fn.DataTable)) return null;
  const dt = $(sel).DataTable();

  // IMPORTANT: request ALL rows regardless of current pagination
  const nodes = dt.rows({ search: 'applied', page: 'all' }).nodes().toArray();

  return nodes.map(tr => {
    const tds = tr.querySelectorAll('td');
    const s_no     = (tds[0]?.innerText || '').trim();
    const table_no = (tds[1]?.innerText || '').trim();    // usually 'E-1' / 'F-1'
    const desc     = (tds[2]?.innerText || '').trim();
    const group_td = tr.querySelector('td.group-no-td');
    const group_code = group_td?.getAttribute('group-no') || '';
    // variant can sometimes be in onclick of the table link:
    const onclick = tds[1]?.querySelector('a')?.getAttribute('onclick') || '';
    const vm = onclick.match(/updateBomDetails\\(\\s*'[^']*'\\s*,\\s*''\\s*,\\s*'([^']*)'\\s*\\)/);
    const variant = vm ? vm[1] : null;
    return { s_no, table_no, desc, group_code, variant };
  });
}
"""

# --- Get ALL rows from a client-side DataTable (thumbnails fallback) ---
EXTRACT_THUMBNAILS_JS = """
(sel) => {
  const $ = window.jQuery || window.$;
  if (!document.querySelector(sel) || !($ && $.fn && $.fn.DataTable)) return null;
  const dt = $(sel).DataTable();

  // IMPORTANT: request ALL rows regardless of current pagination
  const nodes = dt.rows({ search: 'applied', page: 'all' }).nodes().toArray();

  return nodes.map(tr => {
    const link = tr.querySelector('.panel-heading a') || tr.querySelector('.panel-heading');
    const text = (link?.innerText || '').trim();  // e.g., "E-1 (SHROUD / FAN COVER)" or "F-1 (FRAME BODY)"
    const m = text.match(/^([^ \\t(]+)\\s*\\((.*)\\)\\s*$/);
    const table_no = m ? m[1] : text;
    const desc     = m ? m[2] : '';

    const onclick = link?.getAttribute('onclick') || '';
    const gm = onclick.match(/updateBomDetails\\(\\s*'([^']+)'/);
    const group_code = gm ? gm[1] : '';

    const vm = onclick.match(/updateBomDetails\\(\\s*'[^']*'\\s*,\\s*''\\s*,\\s*'([^']*)'\\s*\\)/);
    const variant = vm ? vm[1] : null;

    return { s_no: '', table_no, desc, group_code, variant };
  });
}
"""


def _infer_group_type(table_no: str, group_code: str, fallback: str) -> str:
    """ENGINE/FRAME decided per row, using the row's own tokens first."""
    tn = (table_no or "").strip().upper()
    gc = (group_code or "").strip().upper()
    if tn.startswith("E-") or gc.startswith("E-"):
        return "ENGINE"
    if tn.startswith("F-") or gc.startswith("F-"):
        return "FRAME"
    return fallback  # only if neither token is present


async def collect_indices(
        page: Page,
        vehicle: Vehicle,
        prev_model_code: Optional[str] = None) -> List[TableIndex]:
    model = vehicle.model_code
    logger.info(
        f"Collecting aggregates for vehicle: {vehicle.vehicle_name} ({model})")

    # Switch the SPA panel to this model
    await page.evaluate("(code) => window.loadModelAggregates('', code)",
                        model)

    # Wait for aggregates UI to appear
    selectors = [
        '#DataTables_Table_0', '#DataTables_Table_1', '#DataTables_Table_2',
        '#DataTables_Table_3'
    ]
    found = await wait_for_selector_any(page,
                                        selectors,
                                        timeout_ms=MODEL_READY_TIMEOUT)
    if not found:
        raise Exception(
            f"No aggregates table found for vehicle {vehicle.vehicle_name}")

    # Ensure at least one cell for THIS model is present (prevents most "previous model lag")
    try:
        await page.wait_for_selector(f".group-no-td[group-no$='_{model}']",
                                     timeout=MODEL_READY_TIMEOUT,
                                     state="visible")
    except PWTimeoutError:
        logger.warning(
            "No explicit model-suffixed group-no cells found; proceeding but will not filter by suffix."
        )

    await wait_for_datatables(page, selector=found)

    # Force DataTables to expose ALL rows (not just 25)
    try:
        await evaluate(page, PREPARE_SHOW_ALL_JS)
        counts = await evaluate(page, COUNTS_INFO_JS)
        logger.info(f"Aggregates DT info: {counts}")
    except Exception:
        pass

    aggregates_url = page.url
    all_indices: List[TableIndex] = []

    # Always try both list tables, then both thumbnail tables as fallback
    engine_rows = await evaluate(page, EXTRACT_LIST_VIEW_JS,
                                 '#DataTables_Table_0') or []
    frame_rows = await evaluate(page, EXTRACT_LIST_VIEW_JS,
                                '#DataTables_Table_1') or []

    if not engine_rows and not frame_rows:
        # thumbnails fallback
        engine_rows = await evaluate(page, EXTRACT_THUMBNAILS_JS,
                                     '#DataTables_Table_2') or []
        frame_rows = await evaluate(page, EXTRACT_THUMBNAILS_JS,
                                    '#DataTables_Table_3') or []

    # Build indices per row with robust group_type inference; do NOT drop on suffix mismatch
    def push(rows, table_hint):
        for it in rows or []:
            gc = it.get('group_code', '')
            tn = it.get('table_no', '')
            gd = it.get('desc', '')
            if not gc:
                continue
            gtype = _infer_group_type(tn, gc, fallback=table_hint)
            all_indices.append(
                TableIndex(
                    vehicle_id=vehicle.vehicle_id,
                    group_type=gtype,
                    s_no=it.get('s_no', ''),
                    table_no=tn,
                    group_desc=gd,
                    group_code=gc,
                    variant=(it.get('variant') or None),
                    aggregates_url=aggregates_url,
                ))

    push(engine_rows, "ENGINE")
    push(frame_rows, "FRAME")

    logger.info(
        f"Collected {len(all_indices)} total groups for {vehicle.vehicle_name} (ENGINE/FRAME combined)"
    )
    e_cnt = sum(1 for x in all_indices if x.group_type == "ENGINE")
    f_cnt = sum(1 for x in all_indices if x.group_type == "FRAME")
    logger.info(f"Groups breakdown: {e_cnt} ENGINE, {f_cnt} FRAME")

    return all_indices
