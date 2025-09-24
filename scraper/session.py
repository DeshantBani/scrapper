"""HTTP session management with retry logic."""
import requests
import logging
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional

from . import config
from .utils import get_file_extension_from_content_type

logger = logging.getLogger(__name__)


def make_session() -> requests.Session:
    """Create a requests session with headers and retry configuration."""
    session = requests.Session()

    # Set headers
    session.headers.update({
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept':
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })

    if config.BASE_URL:
        session.headers['Referer'] = config.BASE_URL

    # Configure retries for the session
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry_strategy = Retry(
        total=config.MAX_RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"HEAD", "GET", "OPTIONS"},
        backoff_factor=config.RETRY_DELAY,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


@retry(stop=stop_after_attempt(config.MAX_RETRIES),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(
           (requests.RequestException, requests.Timeout)))
def download_image(session: requests.Session, url: str,
                   dst_path: Path) -> Optional[str]:
    """Download image from URL and save to destination path.
    
    Args:
        session: HTTP session
        url: Image URL
        dst_path: Destination directory path
        
    Returns:
        Final file path if successful, None if failed
    """
    if not url or not url.strip():
        logger.warning("Empty image URL provided")
        return None

    try:
        logger.info(f"Downloading image: {url}")

        response = session.get(url,
                               timeout=config.REQUEST_TIMEOUT,
                               stream=True)
        response.raise_for_status()

        # Get content type and determine extension
        content_type = response.headers.get('content-type', '').strip()
        extension = get_file_extension_from_content_type(content_type)

        # Create final path with proper extension
        final_path = dst_path.with_suffix(extension)

        # Ensure parent directory exists
        final_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file
        with open(final_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        file_size = final_path.stat().st_size
        if file_size == 0:
            logger.warning(f"Downloaded image is empty: {final_path}")
            final_path.unlink(missing_ok=True)
            return None

        logger.info(f"Image saved: {final_path} ({file_size} bytes)")
        return str(final_path)

    except requests.RequestException as e:
        logger.error(f"Failed to download image {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading image {url}: {e}")
        return None
