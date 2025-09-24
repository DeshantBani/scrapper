"""Playwright browser automation helpers."""
import asyncio
import logging
from typing import Any, List, Optional, Tuple
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

from . import config

logger = logging.getLogger(__name__)


async def launch_browser(
        headless: bool = True) -> Tuple[Browser, BrowserContext]:
    """Launch browser and create context.
    
    Returns:
        Tuple of (browser, context)
    """
    playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            '--no-sandbox', '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security', '--disable-features=VizDisplayCompositor'
        ])

    context = await browser.new_context(
        viewport={
            'width': 1920,
            'height': 1080
        },
        user_agent=
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    return browser, context


async def new_page(context: BrowserContext) -> Page:
    """Create a new page from context."""
    return await context.new_page()


async def goto(page: Page,
               url: str,
               wait_until: str = "domcontentloaded") -> None:
    """Navigate to URL with error handling."""
    try:
        logger.info(f"Navigating to: {url}")
        await page.goto(url,
                        wait_until=wait_until,
                        timeout=config.PAGE_LOAD_TIMEOUT)
        await asyncio.sleep(1)  # Brief pause for any dynamic content
    except PlaywrightTimeoutError:
        logger.error(f"Timeout loading page: {url}")
        raise
    except Exception as e:
        logger.error(f"Failed to load page {url}: {e}")
        raise


async def wait_for_datatables(page: Page,
                              selector: str = "#datatable-t2",
                              timeout_ms: int = 15000) -> None:
    """Wait until a DataTables-enhanced table is ready.

    Strategy:
    - First wait for either the table element or its `_wrapper` to appear.
    - Then wait until either the wrapper exists or the table has tbody rows.
    """
    try:
        # Wait for either the table or the DT-generated wrapper
        wrapper_selector = f"{selector}_wrapper"
        found = await wait_for_selector_any(page, [selector, wrapper_selector], timeout_ms=timeout_ms)
        if not found:
            raise PlaywrightTimeoutError(f"Neither {selector} nor {wrapper_selector} appeared within {timeout_ms}ms")

        # Now wait until wrapper exists or the table has rows
        await page.wait_for_function(
            "(sel) => !!document.querySelector(`${sel}_wrapper`) || (document.querySelector(sel)?.tBodies && Array.from(document.querySelector(sel).tBodies).some(tb => tb.rows && tb.rows.length > 0))",
            arg=selector,
            timeout=timeout_ms,
        )
        logger.debug("Table is ready (wrapper or rows present)")
    except PlaywrightTimeoutError:
        logger.error("Timeout waiting for table to be ready")
        raise


async def evaluate(page: Page, js: str, *args) -> Any:
    """Evaluate JavaScript with error handling."""
    try:
        result = await page.evaluate(js, *args)
        return result
    except Exception as e:
        logger.error(f"JavaScript evaluation failed: {e}")
        logger.debug(f"JS code: {js}")
        raise


async def wait_for_selector_any(page: Page,
                                selectors: List[str],
                                timeout_ms: int = 15000) -> Optional[str]:
    """Wait for any of the given selectors to appear.
    
    Returns:
        The first selector that appears, or None if timeout
    """
    try:
        tasks = []
        for selector in selectors:
            task = asyncio.create_task(
                page.wait_for_selector(selector, timeout=timeout_ms))
            tasks.append((selector, task))

        done, pending = await asyncio.wait([task for _, task in tasks],
                                           return_when=asyncio.FIRST_COMPLETED,
                                           timeout=timeout_ms / 1000)

        # Cancel pending tasks
        for task in pending:
            task.cancel()

        # Find which selector completed first
        if done:
            completed_task = next(iter(done))
            for selector, task in tasks:
                if task == completed_task:
                    logger.debug(f"Found selector: {selector}")
                    return selector

        logger.warning(f"None of the selectors found: {selectors}")
        return None

    except Exception as e:
        logger.error(f"Error waiting for selectors {selectors}: {e}")
        return None


async def wait_for_selector(page: Page,
                            selector: str,
                            timeout_ms: int = 15000) -> bool:
    """Wait for a single selector with error handling."""
    try:
        await page.wait_for_selector(selector, timeout=timeout_ms)
        logger.debug(f"Found selector: {selector}")
        return True
    except PlaywrightTimeoutError:
        logger.warning(f"Timeout waiting for selector: {selector}")
        return False
    except Exception as e:
        logger.error(f"Error waiting for selector {selector}: {e}")
        return False


async def scroll_and_wait(page: Page, delay: float = 1.0) -> None:
    """Scroll page and wait for content to load."""
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(delay)
    await page.evaluate("window.scrollTo(0, 0)")


async def get_page_info(page: Page) -> dict:
    """Get basic page information for debugging."""
    try:
        return {
            'url':
            page.url,
            'title':
            await page.title(),
            'has_jquery':
            await page.evaluate("typeof window.$ !== 'undefined'"),
            'has_datatables':
            await page.evaluate(
                "typeof window.$ !== 'undefined' && typeof window.$.fn.dataTable !== 'undefined'"
            )
        }
    except Exception as e:
        logger.error(f"Failed to get page info: {e}")
        return {'url': page.url, 'error': str(e)}
