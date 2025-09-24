# scraper/__init__.py
"""Public API for the Hero e-catalogue scraper."""

__all__ = [
    "config",
    "browser",
    "session",
    "datamodel",
    "store",
    # expose both spellings for convenience
    "catalogue",
    "catalog",
    "aggregates",
    "parts",
    "pipeline",
    "ScrapingPipeline",
]

__version__ = "0.1.0"

from .pipeline import ScrapingPipeline  # convenient import: from scraper import ScrapingPipeline

# Backwards-compat alias: allow `from scraper import catalog` to work
# even though the module file is named `catalogue.py`.
from . import catalogue as catalog
