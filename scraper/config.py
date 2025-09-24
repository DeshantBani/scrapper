"""Configuration settings for Hero scraper."""
import os
from pathlib import Path

# Base settings
BASE_URL = ""  # Will be set from CLI args
HEADLESS = True
MAX_CONCURRENT_GROUPS = 2
RATE_LIMIT_REQUESTS_PER_SEC = 1.5

# Directory paths
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data"
CSV_OUTPUT = OUTPUT_DIR / "csv" / "parts_master.csv"
PARQUET_OUTPUT = OUTPUT_DIR / "parquet" / "parts_master.parquet"
SQLITE_PATH = OUTPUT_DIR / "sqlite" / "hero_catalogue.sqlite"
IMAGES_DIR = OUTPUT_DIR / "images"

# Browser settings
BROWSER_TIMEOUT = 30000  # milliseconds
PAGE_LOAD_TIMEOUT = 30000

# HTTP settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2


# Ensure directories exist
def ensure_directories():
    """Create necessary directories if they don't exist."""
    dirs_to_create = [
        OUTPUT_DIR / "csv",
        OUTPUT_DIR / "parquet",
        OUTPUT_DIR / "sqlite",
        IMAGES_DIR,
    ]

    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
