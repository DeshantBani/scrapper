"""Main catalog scraper - extracts full vehicle list across all DataTables pages."""
import logging
from typing import List
from playwright.async_api import Page

from .datamodel import Vehicle
from .utils import slugify_name, parse_datatable_info
from .browser import goto, wait_for_selector, wait_for_datatables, evaluate

logger = logging.getLogger(__name__)

# JavaScript to extract vehicles from current page
EXTRACT_CURRENT_PAGE_JS = """
(() => {
  const parseModel = (el) => {
    const oc = (el?.getAttribute('onclick') || '');
    const m = oc.match(/loadModelAggregates\\(\\s*''\\s*,\\s*'([^']+)'\\s*\\)/);
    return m ? m[1] : '';
  };

  // Get ALL panel headings in the current DOM (3 columns per row)
  const headings = Array.from(
    document.querySelectorAll('#datatable-t2 .panel .panel-heading[onclick*="loadModelAggregates"]')
  );

  return headings.map(h => ({
    name: (h.innerText || '').trim(),
    modelCode: parseModel(h)
  })).filter(x => x.name && x.modelCode);
})()
"""

# JavaScript to get DataTables pagination info and navigate
GET_PAGINATION_INFO_JS = """
(() => {
  const $ = window.jQuery || window.$;
  if (!$ || !$.fn || !$.fn.dataTable) return null;
  
  const table = $('#datatable-t2');
  if (!table.length) return null;
  
  const dt = table.DataTable();
  const info = dt.page.info();
  
  return {
    currentPage: info.page,
    totalPages: info.pages,
    totalRecords: info.recordsTotal,
    recordsDisplay: info.recordsDisplay,
    start: info.start,
    end: info.end,
    length: info.length
  };
})()
"""

# JavaScript to navigate to specific page and wait for update
NAVIGATE_TO_PAGE_JS = """
async (pageNum) => {
  const $ = window.jQuery || window.$;
  const dt = $('#datatable-t2').DataTable();
  
  // Navigate to page
  dt.page(pageNum);
  
  // Force redraw and wait for it to complete
  return new Promise((resolve) => {
    dt.one('draw.dt', () => {
      // Additional small delay to ensure DOM is fully updated
      setTimeout(resolve, 100);
    });
    dt.draw(false); // false = don't reset page
  });
}
"""


async def collect_vehicles(page: Page, catalog_url: str) -> List[Vehicle]:
    """Collect all vehicles from the main catalog page across ALL DataTables pages."""
    logger.info(f"Collecting vehicles from catalog: {catalog_url}")

    # Navigate to catalog and wait for table
    await goto(page, catalog_url)
    if not await wait_for_selector(page, '#datatable-t2'):
        raise Exception("Main catalog table #datatable-t2 not found")

    await wait_for_datatables(page)

    vehicles_data = []

    # Check if DataTables is available and get pagination info
    pagination_info = await evaluate(page, GET_PAGINATION_INFO_JS)

    if pagination_info:
        total_pages = pagination_info['totalPages']
        logger.info(
            f"DataTables detected with {total_pages} pages (total records: {pagination_info['totalRecords']})"
        )

        for page_num in range(total_pages):
            logger.info(f"Processing page {page_num + 1}/{total_pages}")

            # Navigate to specific page
            if page_num > 0:  # Skip navigation for first page (already there)
                try:
                    await evaluate(page, NAVIGATE_TO_PAGE_JS, page_num)
                    # Additional wait to ensure page is fully loaded
                    await page.wait_for_timeout(200)
                except Exception as e:
                    logger.error(f"Failed to navigate to page {page_num}: {e}")
                    continue

            # Extract vehicles from current page
            try:
                page_vehicles = await evaluate(page, EXTRACT_CURRENT_PAGE_JS)
                logger.info(
                    f"Found {len(page_vehicles)} vehicles on page {page_num + 1}"
                )
                vehicles_data.extend(page_vehicles)
            except Exception as e:
                logger.error(
                    f"Failed to extract vehicles from page {page_num}: {e}")
                continue

    else:
        logger.warning(
            "DataTables not detected; falling back to single page extraction")
        vehicles_data = await evaluate(page, EXTRACT_CURRENT_PAGE_JS)

    if not vehicles_data:
        raise Exception("No vehicles found in catalog")

    # Convert to Vehicle objects and deduplicate
    seen_models = set()
    vehicles: List[Vehicle] = []

    for item in vehicles_data:
        name = (item.get('name') or '').strip()
        model_code = (item.get('modelCode') or '').strip()

        if not name or not model_code:
            logger.debug(f"Skipping invalid vehicle data: {item}")
            continue

        if model_code in seen_models:
            logger.debug(f"Skipping duplicate model_code: {model_code}")
            continue

        seen_models.add(model_code)

        vehicles.append(
            Vehicle(vehicle_id=slugify_name(name),
                    vehicle_name=name,
                    model_code=model_code,
                    source_url=catalog_url))

    # Verify count against DataTables info
    try:
        info_text = await evaluate(
            page,
            "document.querySelector('#datatable-t2_info')?.innerText || ''")
        expected_count = parse_datatable_info(info_text)

        if expected_count and len(vehicles) < expected_count:
            logger.warning(
                f"Vehicle count mismatch: found {len(vehicles)}, expected {expected_count}"
            )
        elif expected_count:
            logger.info(
                f"Vehicle count verified: {len(vehicles)}/{expected_count}")
    except Exception as e:
        logger.debug(f"Could not verify vehicle count: {e}")

    logger.info(f"Collected {len(vehicles)} unique vehicles")

    # Log first few vehicles for verification
    for i, vehicle in enumerate(vehicles[:3]):
        logger.info(
            f"Vehicle {i+1}: {vehicle.vehicle_name} (ID: {vehicle.vehicle_id}, Code: {vehicle.model_code})"
        )

    if len(vehicles) > 3:
        logger.info(f"... and {len(vehicles) - 3} more vehicles")

    return vehicles
