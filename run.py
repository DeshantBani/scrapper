#!/usr/bin/env python3
"""
CLI entrypoint for the Hero e-catalogue scraper.

Usage examples:
  python run.py --catalog-url "https://ecatalogue.heromotocorp.biz/sol_dealer/dealer/" 
  python run.py --catalog-url "https://..." --no-headless --force --parquet --log-level DEBUG
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional
from pathlib import Path

# Ensure package imports work when running as a script
# (repo layout: hero_scraper/ with scraper/ package and this run.py)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from scraper.pipeline import ScrapingPipeline
from scraper import config


def _setup_logging(level: str, log_file: Optional[str] = None) -> None:
    """Configure logging for console (and optional file)."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Console handler
    handlers = [logging.StreamHandler(sys.stdout)]

    # Optional file handler
    if log_file:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        handlers.append(file_handler)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
        handlers=handlers,
    )

    # Reduce noisy third-party loggers if needed
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=
        "Hero e-catalogue web scraper (vehicles → groups → parts + images).")
    parser.add_argument(
        "--catalog-url",
        required=True,
        help=
        "Main catalogue URL (landing page that shows the vehicle cards grid).",
    )
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        default=True,
        help="Run browser in headless mode (default).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser with a visible window (useful for debugging).",
    )
    parser.add_argument(
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Reprocess groups even if checkpoint says 'done'.",
    )
    parser.add_argument(
        "--parquet",
        dest="parquet",
        action="store_true",
        default=False,
        help="In addition to CSV, also write a Parquet file for parts.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("LOG_FILE", ""),
        help="Optional path to write logs to a file as well.",
    )
    # Optional: allow overriding some config at runtime
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Base output directory (images/, csv/, sqlite/) [default: data]",
    )

    return parser.parse_args()


async def _amain(args: argparse.Namespace) -> None:
    # Apply runtime config overrides
    config.HEADLESS = bool(args.headless)
    if args.output_dir:
        # If your config.py exposes OUTPUT_DIR and ensure_directories() uses it, set it here:
        try:
            config.OUTPUT_DIR = Path(args.output_dir)
            # Recompute derived paths to stay consistent with new OUTPUT_DIR
            try:
                config.CSV_OUTPUT = config.OUTPUT_DIR / "csv" / "parts_master.csv"
                config.PARQUET_OUTPUT = config.OUTPUT_DIR / "parquet" / "parts_master.parquet"
                config.SQLITE_PATH = config.OUTPUT_DIR / "sqlite" / "hero_catalogue.sqlite"
                config.IMAGES_DIR = config.OUTPUT_DIR / "images"
            except Exception:
                # If these attributes don't exist in config, ignore
                pass
        except Exception:
            pass

    _setup_logging(args.log_level, args.log_file or None)

    logging.getLogger(__name__).info(
        f"Headless={config.HEADLESS} | Force={args.force} | Parquet={args.parquet}"
    )

    pipeline = ScrapingPipeline(force_reprocess=args.force,
                                save_parquet=args.parquet)
    await pipeline.run(args.catalog_url)


def main() -> None:
    # Windows: use Proactor loop policy for Playwright/async
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(
                asyncio.WindowsProactorEventLoopPolicy(
                ))  # type: ignore[attr-defined]
        except Exception:
            # Older Python may not have WindowsProactorEventLoopPolicy
            pass

    args = parse_args()
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        logging.getLogger(__name__).exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
