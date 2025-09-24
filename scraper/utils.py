"""Utility functions for Hero scraper."""
import re
import asyncio
from decimal import Decimal, InvalidOperation
from typing import Optional
from slugify import slugify


def slugify_name(name: str) -> str:
    """Convert name to URL-safe slug."""
    return slugify(name, max_length=50)


def parse_int(s: str) -> Optional[int]:
    """Parse integer, handling commas and whitespace."""
    if not s or not s.strip():
        return None
    try:
        # Remove commas and whitespace
        clean_str = s.replace(",", "").strip()
        return int(clean_str)
    except (ValueError, AttributeError):
        return None


def parse_decimal(s: str) -> Optional[Decimal]:
    """Parse decimal, handling commas and whitespace."""
    if not s or not s.strip():
        return None
    try:
        # Remove commas and whitespace
        clean_str = s.replace(",", "").strip()
        return Decimal(clean_str)
    except (InvalidOperation, ValueError, AttributeError):
        return None


def extract_model_code(onclick: str) -> str:
    """Extract model code from loadModelAggregates onclick attribute."""
    if not onclick:
        return ""

    pattern = r"loadModelAggregates\(\s*''\s*,\s*'([^']+)'\s*\)"
    match = re.search(pattern, onclick)
    return match.group(1) if match else ""


def extract_variant_from_update(onclick: str) -> Optional[str]:
    """Extract variant from updateBomDetails onclick attribute."""
    if not onclick:
        return None

    pattern = r"updateBomDetails\(\s*'[^']*'\s*,\s*''\s*,\s*'([^']*)'\s*\)"
    match = re.search(pattern, onclick)

    if match:
        variant = match.group(1).strip()
        return variant if variant else None
    return None


def extract_group_code_from_update(onclick: str) -> str:
    """Extract group code from updateBomDetails onclick attribute."""
    if not onclick:
        return ""

    pattern = r"updateBomDetails\(\s*'([^']+)'"
    match = re.search(pattern, onclick)
    return match.group(1) if match else ""


async def wait_with_rate_limit(delay_seconds: float = 1.0):
    """Wait for rate limiting purposes."""
    await asyncio.sleep(delay_seconds)


def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    return text.strip()


def get_file_extension_from_content_type(content_type: str) -> str:
    """Get file extension from HTTP Content-Type header."""
    if not content_type:
        return ".bin"

    content_type = content_type.lower().split(';')[0].strip()

    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/svg+xml": ".svg",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp"
    }

    return mapping.get(content_type, ".bin")


def parse_datatable_info(info_text: str) -> Optional[int]:
    """Parse DataTables info text to get total count.
    
    Example: "Showing 1 to 25 of 92 entries" -> 92
    """
    if not info_text:
        return None

    pattern = r"of\s+(\d+)\s+entries"
    match = re.search(pattern, info_text, re.IGNORECASE)
    return int(match.group(1)) if match else None
