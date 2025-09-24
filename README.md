# Hero E-Catalogue Scraper

A Python web scraper for extracting vehicle parts data from Hero MotoCorp's e-catalogue website. This scraper collects vehicle information, part groups, individual parts data, and associated images.

## Features

- **Vehicle Catalogue Scraping**: Automatically discovers and processes all vehicles from the Hero e-catalogue
- **Parts Data Extraction**: Extracts detailed parts information including part numbers, descriptions, MRP, MOQ, etc.
- **Image Download**: Downloads and saves parts diagrams and images
- **Data Storage**: Supports both CSV and Parquet output formats
- **SQLite Checkpointing**: Robust checkpoint system to resume interrupted scraping sessions
- **DataTables Integration**: Handles client-side DataTables pagination automatically
- **Headless/GUI Mode**: Can run in both headless and visible browser modes for debugging

## Requirements

- Python 3.8+
- Playwright browser automation
- uv package manager (recommended)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Scrapper
```

2. Create and activate virtual environment using uv:
```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
uv pip install -r requirements.txt
```

4. Install Playwright browsers:
```bash
playwright install
```

## Usage

### Basic Usage

```bash
source .venv/bin/activate
python run.py --catalog-url "https://ecatalogue.heromotocorp.biz:8080/Hero/index.html"
```

### Command Line Options

```bash
python run.py --help
```

- `--catalog-url`: Main catalogue URL (required)
- `--headless`: Run browser in headless mode (default)
- `--no-headless`: Run browser with visible window (useful for debugging)
- `--force`: Reprocess groups even if checkpoint says 'done'
- `--parquet`: Also write a Parquet file for parts data
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)
- `--log-file`: Optional path to write logs to a file
- `--output-dir`: Base output directory (default: data)

### Examples

```bash
# Run with visible browser for debugging
python run.py --catalog-url "https://ecatalogue.heromotocorp.biz:8080/Hero/index.html" --no-headless

# Force reprocess with debug logging
python run.py --catalog-url "https://ecatalogue.heromotocorp.biz:8080/Hero/index.html" --force --log-level DEBUG

# Save both CSV and Parquet formats
python run.py --catalog-url "https://ecatalogue.heromotocorp.biz:8080/Hero/index.html" --parquet
```

## Project Structure

```
Scrapper/
├── scraper/                 # Main scraper package
│   ├── __init__.py
│   ├── aggregates.py       # Group/table extraction logic
│   ├── browser.py          # Browser automation utilities
│   ├── catalogue.py        # Vehicle catalogue scraping
│   ├── config.py           # Configuration settings
│   ├── datamodel.py        # Data models and schemas
│   ├── parts.py            # Individual parts scraping
│   ├── pipeline.py         # Main scraping pipeline
│   ├── session.py          # HTTP session management
│   ├── store.py            # Data storage (SQLite, CSV, Parquet)
│   └── utils.py            # Utility functions
├── data/                   # Output directory (gitignored)
│   ├── csv/               # CSV output files
│   ├── images/            # Downloaded part diagrams
│   ├── parquet/           # Parquet output files
│   └── sqlite/            # SQLite checkpoint database
├── requirements.txt        # Python dependencies
├── run.py                 # CLI entry point
└── README.md              # This file
```

## Output Data

The scraper generates several types of output:

### CSV/Parquet Files
- `data/csv/parts_master.csv`: Complete parts database
- `data/parquet/parts_master.parquet`: Same data in Parquet format (if enabled)

### Images
- `data/images/{vehicle-id}/{group-type}/{table-no}.jpg`: Parts diagrams

### SQLite Database
- `data/sqlite/hero_catalogue.sqlite`: Checkpoint and metadata storage

## Data Schema

Each row in the output contains:
- Vehicle information (ID, name, model code)
- Group information (type, table number, group code, description)
- Part details (reference number, part number, description, remark, MRP, MOQ)
- File paths and URLs

## Checkpointing System

The scraper uses SQLite-based checkpointing to:
- Resume interrupted scraping sessions
- Skip already processed groups
- Track success/failure status
- Prevent duplicate data extraction

Use `--force` flag to override checkpoints and reprocess all data.

## Troubleshooting

### Common Issues

1. **ModuleNotFoundError: No module named 'playwright'**
   - Make sure you've activated the virtual environment
   - Install dependencies: `uv pip install -r requirements.txt`
   - Install browsers: `playwright install`

2. **Page.wait_for_function() TypeError**
   - This has been fixed in the latest version
   - Make sure you're using the updated aggregates.py

3. **Missing groups/parts data**
   - The scraper now handles DataTables pagination automatically
   - Use `--no-headless` to debug browser behavior
   - Check logs for timeout or connection issues

### Debug Mode

Run with visible browser and debug logging:
```bash
python run.py --catalog-url "https://ecatalogue.heromotocorp.biz:8080/Hero/index.html" --no-headless --log-level DEBUG
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is for educational and research purposes. Please respect the website's terms of service and robots.txt when using this scraper.

## Changelog

### Recent Fixes
- Fixed Playwright `wait_for_function()` API compatibility
- Improved DataTables pagination handling
- Enhanced group detection (increased from 12 to 25+ groups per vehicle)
- Added robust error handling and retry logic
- Improved checkpoint system reliability
