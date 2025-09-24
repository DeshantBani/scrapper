# scraper/parts.py
"""Parts scraper - extracts individual parts and diagram images (popup-aware)."""
import asyncio
import logging
from pathlib import Path
from typing import List, Tuple, Optional

import requests
from playwright.async_api import Page, TimeoutError as PWTimeoutError

from . import config
from .datamodel import Vehicle, TableIndex, PartsPage, PartRow, PARTS_HEADER_MAPPING
from .utils import clean_text, parse_datatable_info
from .session import download_image

logger = logging.getLogger(__name__)

# ---------- Small helpers (work on Page OR Frame) ----------

PARTS_SELECTORS = [
    "#bomPage", "#bomPage_wrapper", "#bomPage_info", "#image img"
]


async def _wait_for_any(page_like, selectors,
                        timeout_ms: int) -> Optional[str]:
    """Wait for any selector on a Page or Frame; return the one that appears, else None."""
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000.0)
    while asyncio.get_event_loop().time() < deadline:
        for sel in selectors:
            try:
                el = await page_like.query_selector(sel)
                if el:
                    return sel
            except Exception:
                pass
        await asyncio.sleep(0.15)
    return None


async def _extract_parts_table(page_like):
    """Run inside the parts host (popup page or same page) to pull DataTables rows."""
    return await page_like.evaluate("""
        () => {
          const $ = window.jQuery || window.$;
          const headers = Array.from(document.querySelectorAll('#bomPage thead th'))
            .map(th => th.innerText.trim());
          const dt = $('#bomPage').DataTable();
          const nodes = dt.rows().nodes().toArray();
          const rows = nodes.map(tr => Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim()));
          const info = document.querySelector('#bomPage_info')?.innerText || '';
          return { headers, rows, info };
        }
        """)


async def _get_image_src(page_like) -> Optional[str]:
    try:
        return await page_like.evaluate(
            "() => document.querySelector('#image img')?.src || null")
    except Exception:
        return None


# ---------- Main entry ----------


async def collect_parts(
        page: Page, vehicle: Vehicle, table_index: TableIndex,
        session: requests.Session) -> Tuple[PartsPage, List[PartRow], bool]:
    """Collect parts data and download diagram image for a specific group.

    Returns:
        (PartsPage, List[PartRow], image_saved: bool)
    """
    group_code = table_index.group_code
    variant = table_index.variant or ""
    nice_name = vehicle.vehicle_name
    logger.info(
        f"Collecting parts for {nice_name} - {table_index.group_type} {table_index.table_no}"
    )

    context = page.context
    popup = None
    parts_host = page  # will switch to popup or iframe when detected
    parts_wait_ms = 45_000

    # 1) Fire updateBomDetails and capture popup if it opens (NOTE: single arg object!)
    try:
        async with context.expect_page() as popup_info:
            await page.evaluate(
                "(arg) => window.updateBomDetails(arg.code, '', arg.variant || '')",
                {
                    "code": group_code,
                    "variant": variant
                },
            )
        popup = await popup_info.value
        parts_host = popup
        logger.debug(f"Popup captured for group {group_code}: {popup.url}")
    except PWTimeoutError:
        popup = None
        parts_host = page
        logger.debug(
            f"No popup for {group_code}; checking current page/frames for parts UI."
        )

    # 2) Wait for parts UI on popup/page (with iframe fallback)
    found_sel = await _wait_for_any(parts_host, PARTS_SELECTORS, parts_wait_ms)
    if not found_sel:
        try:
            for fr in getattr(parts_host, "frames", []):
                found_sel = await _wait_for_any(fr, PARTS_SELECTORS, 2_000)
                if found_sel:
                    parts_host = fr
                    break
        except Exception:
            pass

    if not found_sel:
        # Diagnostics
        try:
            current = popup.url if popup else page.url
        except Exception:
            current = page.url
        logger.error("Parts UI not found for %s. URL=%s", group_code, current)
        try:
            if popup:
                await popup.close()
        except Exception:
            pass
        raise Exception(
            f"Parts UI not found for {group_code} (waited {parts_wait_ms//1000}s)"
        )

    # ==== Verification it's the right vehicle/group (with retry logic) ====
    try:
        # Wait a bit for page to fully load
        await parts_host.wait_for_timeout(500)
        
        # 1) URL should include the group code
        url_ok = await (parts_host if parts_host != page else
                        (popup or page)).evaluate(
                            "(gc) => location.href.includes(gc)", group_code)
        # 2) The header usually contains group code or vehicle name (more lenient check)
        hdr_ok = await parts_host.evaluate(
            """
            (gc, titleFrag) => {
              const hdr =
                document.querySelector('.panel-title, .modal-title, .panel-heading')?.innerText || '';
              // More lenient: check if any part of group code is present, or if table exists
              return hdr.includes(gc) || hdr.includes(titleFrag) || 
                     !!document.querySelector('#bomPage') || !!document.querySelector('#bomPage_wrapper');
            }
            """,
            group_code,
            nice_name[:20],
        )
        # 3) Table exists
        table_ok = await parts_host.evaluate(
            "() => !!document.querySelector('#bomPage') || !!document.querySelector('#bomPage_wrapper')"
        )
        
        # More lenient validation - only retry if both URL and table are missing
        if not (url_ok or table_ok):
            raise RuntimeError("mismatch")
    except Exception:
        logger.warning(
            f"Parts popup mismatch (likely previous vehicle). Retrying {group_code} after short delay…"
        )
        try:
            if popup:
                await popup.close()
        except Exception:
            pass

        await page.wait_for_timeout(1000)  # let SPA catch up
        async with context.expect_page() as popup_info:
            await page.evaluate(
                "(arg) => window.updateBomDetails(arg.code, '', arg.variant || '')",
                {
                    "code": group_code,
                    "variant": variant
                },
            )
        popup = await popup_info.value
        parts_host = popup
        await _wait_for_any(parts_host, PARTS_SELECTORS, parts_wait_ms)

        # Check again, now hard-fail if still wrong
        url_str = popup.url if popup else await page.evaluate(
            "() => location.href")
        if group_code not in url_str:
            raise Exception(f"Popup URL mismatch for {group_code}: {url_str}")

    # 3) Extract parts table
    data = await _extract_parts_table(parts_host)
    headers = data.get("headers", [])
    rows = data.get("rows", [])
    info_text = data.get("info", "")
    logger.info(f"Found {len(rows)} parts rows for {group_code}")

    # Validate expected count if available
    try:
        expected = parse_datatable_info(info_text)
        if expected and len(rows) < expected:
            logger.warning(
                f"Parts count mismatch for {group_code}: found {len(rows)}, expected {expected}"
            )
    except Exception:
        pass

    # Map rows -> PartRow
    part_rows: List[PartRow] = []
    header_map = [h.strip() for h in headers]
    for r in rows:
        if len(r) < len(header_map):
            r = r + [""] * (len(header_map) - len(r))
        mapped = {}
        for i, hdr in enumerate(header_map):
            key = PARTS_HEADER_MAPPING.get(hdr, hdr.lower().replace(" ", "_"))
            mapped[key] = clean_text(r[i])
        part_rows.append(
            PartRow(
                vehicle_id=vehicle.vehicle_id,
                group_type=table_index.group_type,
                table_no=table_index.table_no,
                group_code=table_index.group_code,
                ref_no=mapped.get("ref_no", ""),
                part_no=mapped.get("part_no", ""),
                description=mapped.get("description", ""),
                remark=mapped.get("remark", ""),
                req_no=mapped.get("req_no", ""),
                moq=mapped.get("moq", ""),
                mrp=mapped.get("mrp", ""),
            ))

    # 4) Download diagram image
    image_path_str: Optional[str] = None
    image_saved: bool = False
    try:
        img_src = await _get_image_src(parts_host)
        if img_src:
            target_dir = config.IMAGES_DIR / vehicle.vehicle_id / table_index.group_type
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path_no_ext = target_dir / f"{table_index.table_no}"
            downloaded = download_image(session, img_src, target_path_no_ext)
            if downloaded:
                image_saved = True
                try:
                    abs_dl = Path(downloaded).resolve()
                    root = Path(getattr(config, "PROJECT_ROOT", ".")).resolve()
                    image_path_str = str(abs_dl.relative_to(root))
                except Exception:
                    image_path_str = str(Path(downloaded))
                logger.info(f"Downloaded diagram: {image_path_str}")
            else:
                logger.warning(f"Failed to download image: {img_src}")
        else:
            logger.warning(f"No image found for {group_code}")
    except Exception as e:
        logger.error(f"Error downloading image for {group_code}: {e}")

    # 5) Build PartsPage result
    try:
        parts_page_url = popup.url if popup else getattr(
            parts_host, "url", None) or page.url
    except Exception:
        parts_page_url = page.url

    parts_page = PartsPage(
        vehicle_id=vehicle.vehicle_id,
        group_type=table_index.group_type,
        table_no=table_index.table_no,
        group_code=table_index.group_code,
        parts_page_url=parts_page_url,
        image_path=image_path_str,
    )

    # 6) Close popup if one was opened
    try:
        if popup:
            await popup.close()
    except Exception:
        pass

    logger.info(f"Collected {len(part_rows)} parts for {group_code}")
    return parts_page, part_rows, image_saved
