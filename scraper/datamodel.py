"""Data models for Hero scraper."""
from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class Vehicle:
    """Represents a vehicle from the main catalogue."""
    vehicle_id: str  # slug from name
    vehicle_name: str
    model_code: str  # from loadModelAggregates('', '<MODEL_CODE>')
    source_url: str


@dataclass
class TableIndex:
    """Represents an Engine/Frame row on the aggregates page."""
    vehicle_id: str
    group_type: Literal["ENGINE", "FRAME"]
    s_no: str
    table_no: str
    group_desc: str
    group_code: str  # e.g., E-1_XOOM_XTECH_AAWY
    variant: Optional[str]  # 3rd arg in updateBomDetails, may be None
    aggregates_url: str


@dataclass
class PartsPage:
    """Represents a parts page with metadata."""
    vehicle_id: str
    group_type: Literal["ENGINE", "FRAME"]
    table_no: str
    group_code: str
    parts_page_url: str
    image_path: Optional[str]


@dataclass
class PartRow:
    """Represents a single parts row."""
    vehicle_id: str
    group_type: Literal["ENGINE", "FRAME"]
    table_no: str
    group_code: str
    ref_no: str
    part_no: str
    description: str
    remark: str
    req_no: str
    moq: str
    mrp: str


# Schema constants for CSV headers
PART_ROW_HEADERS = [
    "vehicle_id", "vehicle_name", "model_code", "group_type", "table_no",
    "group_code", "group_desc", "ref_no", "part_no", "description", "remark",
    "req_no", "moq", "mrp", "image_path", "parts_page_url", "source_url"
]

# Header mapping for parts table extraction
PARTS_HEADER_MAPPING = {
    "Ref No.": "ref_no",
    "Part Number": "part_no",
    "Description": "description",
    "Remark": "remark",
    "Req. No.": "req_no",
    "MOQ": "moq",
    "MRP(Rs.)": "mrp"
}
