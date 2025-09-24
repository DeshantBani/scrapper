"""Data storage management - SQLite checkpoints and CSV/Parquet output."""
import sqlite3
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from . import config
from .datamodel import Vehicle, TableIndex, PartsPage, PartRow, PART_ROW_HEADERS

logger = logging.getLogger(__name__)


class DataStore:
    """Manages SQLite checkpoints and data output, plus deduped parts storage."""

    def __init__(self, sqlite_path: Path = None):
        self.sqlite_path = sqlite_path or config.SQLITE_PATH
        self._init_database()

    # --------------------------- DB INIT ---------------------------------

    def _init_database(self):
        """Initialize SQLite database with required tables and migrate if needed."""
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        def column_names(conn, table):
            cur = conn.execute(f"PRAGMA table_info({table})")
            return [r[1] for r in cur.fetchall()]

        with sqlite3.connect(self.sqlite_path) as conn:
            # --- checkpoints (unchanged) ---
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                vehicle_id TEXT,
                group_type TEXT,
                table_no   TEXT,
                group_code TEXT,
                status     TEXT,      -- 'pending' | 'done' | 'error'
                row_count  INTEGER,
                image_saved INTEGER,   -- 0/1
                last_error TEXT,
                updated_at TEXT,
                PRIMARY KEY (vehicle_id, group_type, table_no, group_code)
                )
            """)

        # --- vehicles (unchanged) ---
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vehicles (
                vehicle_id   TEXT PRIMARY KEY,
                vehicle_name TEXT,
                model_code   TEXT,
                source_url   TEXT,
                created_at   TEXT
            )
            """)

        # --- parts_pages (ensure full schema) ---
            conn.execute("""
            CREATE TABLE IF NOT EXISTS parts_pages (
                vehicle_id     TEXT,
                group_type     TEXT,
                table_no       TEXT,
                group_code     TEXT,
                parts_page_url TEXT,
                image_path     TEXT,
                updated_at     TEXT,
                PRIMARY KEY (vehicle_id, group_type, table_no, group_code)
            )
            """)

        # Migrate parts_pages if it exists w/out updated_at
            cols = column_names(conn, "parts_pages")
            if "updated_at" not in cols:
                conn.execute("ALTER TABLE parts_pages ADD COLUMN updated_at TEXT")

        # --- parts (ensure full schema used by save_part_rows) ---
            conn.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                vehicle_id     TEXT,
                vehicle_name   TEXT,
                model_code     TEXT,
                group_type     TEXT,
                table_no       TEXT,
                group_code     TEXT,
                group_desc     TEXT,
                ref_no         TEXT,
                part_no        TEXT,
                description    TEXT,
                remark         TEXT,
                req_no         TEXT,
                moq            TEXT,
                mrp            TEXT,
                parts_page_url TEXT,
                image_path     TEXT,
                source_url     TEXT,
                created_at     TEXT
            )
            """)

        # Migrate parts table if it was created with fewer columns
            parts_cols = set(column_names(conn, "parts"))
            needed = {
                "vehicle_name", "model_code", "group_desc", "parts_page_url",
                "image_path", "source_url", "created_at"
            }
            add_map = {
            "vehicle_name":   "ALTER TABLE parts ADD COLUMN vehicle_name TEXT",
            "model_code":     "ALTER TABLE parts ADD COLUMN model_code TEXT",
            "group_desc":     "ALTER TABLE parts ADD COLUMN group_desc TEXT",
            "parts_page_url": "ALTER TABLE parts ADD COLUMN parts_page_url TEXT",
            "image_path":     "ALTER TABLE parts ADD COLUMN image_path TEXT",
            "source_url":     "ALTER TABLE parts ADD COLUMN source_url TEXT",
            "created_at":     "ALTER TABLE parts ADD COLUMN created_at TEXT",
            }
            for col in needed:
                if col not in parts_cols:
                    conn.execute(add_map[col])

                    # Ensure a unique constraint that matches our ON CONFLICT target
        # (vehicle_id, group_type, table_no, group_code, ref_no, part_no)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS parts_unique_idx
                ON parts (vehicle_id, group_type, table_no, group_code, ref_no, part_no)
            """)


            conn.commit()


    # ----------------------- CHECKPOINTS ---------------------------------

    def checkpoint_status(self, key: Tuple[str, str, str, str]) -> Optional[str]:
        """Get checkpoint status for (vehicle_id, group_type, table_no, group_code)."""
        vehicle_id, group_type, table_no, group_code = key
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM checkpoints WHERE vehicle_id=? AND group_type=? AND table_no=? AND group_code=?",
                (vehicle_id, group_type, table_no, group_code),
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def checkpoint_mark_done(self, key: Tuple[str, str, str, str], row_count: int, image_saved: bool):
        """Mark checkpoint as done."""
        vehicle_id, group_type, table_no, group_code = key
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints
                (vehicle_id, group_type, table_no, group_code, status, row_count, image_saved, last_error, updated_at)
                VALUES (?, ?, ?, ?, 'done', ?, ?, NULL, ?)
            """, (vehicle_id, group_type, table_no, group_code, row_count, int(image_saved), datetime.now().isoformat()))
            conn.commit()
        logger.info(f"Checkpoint marked done: {key} ({row_count} rows, image_saved={image_saved})")

    def checkpoint_mark_error(self, key: Tuple[str, str, str, str], error: str):
        """Mark checkpoint as error."""
        vehicle_id, group_type, table_no, group_code = key
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints
                (vehicle_id, group_type, table_no, group_code, status, row_count, image_saved, last_error, updated_at)
                VALUES (?, ?, ?, ?, 'error', NULL, 0, ?, ?)
            """, (vehicle_id, group_type, table_no, group_code, error, datetime.now().isoformat()))
            conn.commit()
        logger.error(f"Checkpoint marked error: {key} - {error}")

    def checkpoint_mark_pending(self, key: Tuple[str, str, str, str]):
        """Mark checkpoint as pending."""
        vehicle_id, group_type, table_no, group_code = key
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints
                (vehicle_id, group_type, table_no, group_code, status, row_count, image_saved, last_error, updated_at)
                VALUES (?, ?, ?, ?, 'pending', NULL, 0, NULL, ?)
            """, (vehicle_id, group_type, table_no, group_code, datetime.now().isoformat()))
            conn.commit()

    # ----------------------- VEHICLES -----------------------------------

    def save_vehicle(self, vehicle: Vehicle):
        """Save vehicle to database."""
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO vehicles
                (vehicle_id, vehicle_name, model_code, source_url, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (vehicle.vehicle_id, vehicle.vehicle_name, vehicle.model_code, vehicle.source_url, datetime.now().isoformat()))
            conn.commit()

    def get_vehicles(self) -> List[Vehicle]:
        """Get all saved vehicles."""
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT vehicle_id, vehicle_name, model_code, source_url FROM vehicles")
            vehicles = []
            for row in cursor.fetchall():
                vehicles.append(Vehicle(vehicle_id=row[0], vehicle_name=row[1], model_code=row[2], source_url=row[3]))
            return vehicles

    # ------------------- UPSERT PARTS & PAGES ---------------------------

    def save_parts_page(self, parts_page: PartsPage):
        """Backwards-compatible name used by pipeline: upsert into parts_pages."""
        return self.upsert_parts_page(parts_page)

    def upsert_parts_page(self, parts_page: PartsPage):
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
                INSERT INTO parts_pages (vehicle_id, group_type, table_no, group_code, parts_page_url, image_path, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vehicle_id, group_type, table_no, group_code)
                DO UPDATE SET parts_page_url=excluded.parts_page_url,
                              image_path=excluded.image_path,
                              updated_at=excluded.updated_at
            """, (
                parts_page.vehicle_id, parts_page.group_type, parts_page.table_no,
                parts_page.group_code, parts_page.parts_page_url,
                parts_page.image_path or '', datetime.now().isoformat()
            ))
            conn.commit()

    def upsert_part_rows(self, dict_rows: List[dict]) -> int:
        """UPSERT many part rows; returns count submitted (duplicates ignored)."""
        if not dict_rows:
            return 0
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.executemany("""
                INSERT INTO parts (
                  vehicle_id, group_type, table_no, group_code,
                  ref_no, part_no, description, remark, req_no, moq, mrp,
                  image_path, parts_page_url, source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vehicle_id, group_type, table_no, group_code, ref_no, part_no)
                DO UPDATE SET description=excluded.description,
                              remark=excluded.remark,
                              req_no=excluded.req_no,
                              moq=excluded.moq,
                              mrp=excluded.mrp,
                              image_path=excluded.image_path,
                              parts_page_url=excluded.parts_page_url,
                              source_url=excluded.source_url
            """, [(
                d['vehicle_id'], d['group_type'], d['table_no'], d['group_code'],
                d.get('ref_no',''), d.get('part_no',''),
                d.get('description',''), d.get('remark',''),
                d.get('req_no',''), d.get('moq',''), d.get('mrp',''),
                d.get('image_path',''), d.get('parts_page_url',''), d.get('source_url','')
            ) for d in dict_rows])
            conn.commit()
        return len(dict_rows)

    # ---------------- CSV / Parquet (group-scoped write) ----------------

    def _fetch_group_df(self, vehicle_id: str, group_type: str, table_no: str, group_code: str) -> pd.DataFrame:
        """Read the canonical rows for a group from SQLite."""
        with sqlite3.connect(self.sqlite_path) as conn:
            df = pd.read_sql_query("""
                SELECT vehicle_id, group_type, table_no, group_code,
                       ref_no, part_no, description, remark, req_no, moq, mrp,
                       image_path, parts_page_url, source_url
                FROM parts
                WHERE vehicle_id=? AND group_type=? AND table_no=? AND group_code=?
                ORDER BY CAST(NULLIF(ref_no, '') AS INT) NULLS LAST, ref_no
            """, conn, params=(vehicle_id, group_type, table_no, group_code))
        # Ensure column order matches schema used elsewhere
        df = df.reindex(columns=[
            'vehicle_id','group_type','table_no','group_code','ref_no','part_no',
            'description','remark','req_no','moq','mrp','image_path','parts_page_url','source_url'
        ])
        return df

    def _overwrite_group_in_csv(self, df_group: pd.DataFrame):
        """Write/refresh a group's rows into the CSV without creating duplicates."""
        config.CSV_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        if config.CSV_OUTPUT.exists():
            existing = pd.read_csv(config.CSV_OUTPUT, dtype=str).fillna('')
            mask = (
                (existing['vehicle_id'] == df_group['vehicle_id'].iloc[0]) &
                (existing['group_type'] == df_group['group_type'].iloc[0]) &
                (existing['table_no'] == df_group['table_no'].iloc[0]) &
                (existing['group_code'] == df_group['group_code'].iloc[0])
            )
            existing = existing.loc[~mask]
            out = pd.concat([existing, df_group], ignore_index=True)
        else:
            out = df_group
        # Stable column order
        desired_cols = [
            'vehicle_id','vehicle_name','model_code','group_type','table_no','group_code','group_desc',
            'ref_no','part_no','description','remark','req_no','moq','mrp',
            'image_path','parts_page_url','source_url'
        ]
        # If file already had the wider schema from convert_parts_to_dict_list, try to align
        if set(desired_cols).issubset(out.columns):
            out = out[desired_cols]
        out.to_csv(config.CSV_OUTPUT, index=False)

    def _overwrite_group_in_parquet(self, df_group: pd.DataFrame):
        config.PARQUET_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        try:
            existing = pd.read_parquet(config.PARQUET_OUTPUT)
            mask = (
                (existing['vehicle_id'] == df_group['vehicle_id'].iloc[0]) &
                (existing['group_type'] == df_group['group_type'].iloc[0]) &
                (existing['table_no'] == df_group['table_no'].iloc[0]) &
                (existing['group_code'] == df_group['group_code'].iloc[0])
            )
            existing = existing.loc[~mask]
            out = pd.concat([existing, df_group], ignore_index=True)
        except Exception:
            out = df_group
        out.to_parquet(config.PARQUET_OUTPUT, index=False)

    # Public API used by pipeline after each table:
    def append_parts_rows(
        self,
        vehicle: Vehicle,
        table_index: TableIndex,
        parts_page: PartsPage,
        part_rows: List[PartRow],
        write_csv: bool = True,
        save_parquet: bool = False,
    ) -> int:
        """
        UPSERT part rows for a group and immediately write that group's rows to CSV (and Parquet if requested),
        replacing previous rows for the group to avoid duplicates on re-runs.
        """
        # Convert to dict list for DB write
        dict_rows = self.convert_parts_to_dict_list(vehicle, table_index, parts_page, part_rows)

        # UPSERT into DB (dedup canonical storage)
        upserted = self.upsert_part_rows(dict_rows)

        if write_csv or save_parquet:
            # Re-fetch canonical rows for this group from DB (ensures we write exactly what's in DB)
            df_group = self._fetch_group_df(vehicle.vehicle_id, table_index.group_type, table_index.table_no, table_index.group_code)

            # Enrich with vehicle & group_desc if available in the converted dicts
            if not df_group.empty:
                # Try to add vehicle_name/model_code/group_desc from the converted rows (all are same per group)
                vname = dict_rows[0].get('vehicle_name', vehicle.vehicle_name) if dict_rows else vehicle.vehicle_name
                mcode = dict_rows[0].get('model_code', vehicle.model_code) if dict_rows else vehicle.model_code
                gdesc = dict_rows[0].get('group_desc', getattr(table_index, 'group_desc', ''))
                df_group.insert(1, 'vehicle_name', vname)
                df_group.insert(2, 'model_code', mcode)
                df_group.insert(6, 'group_desc', gdesc)

            if write_csv:
                self._overwrite_group_in_csv(df_group)
                logger.info(f"CSV updated for group {table_index.group_type} {table_index.table_no} ({len(df_group)} rows)")

            if save_parquet:
                self._overwrite_group_in_parquet(df_group)
                logger.info(f"Parquet updated for group {table_index.group_type} {table_index.table_no} ({len(df_group)} rows)")

        return upserted

    # --------------------- Legacy writers / helpers ----------------------

    def get_checkpoint_stats(self) -> dict:
        with sqlite3.connect(self.sqlite_path) as conn:
            cursor = conn.cursor()
            stats = {}
            cursor.execute("SELECT status, COUNT(*) FROM checkpoints GROUP BY status")
            for status, count in cursor.fetchall():
                stats[f"{status}_count"] = count
            cursor.execute("SELECT COUNT(*) FROM checkpoints")
            stats['total_checkpoints'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM vehicles")
            stats['total_vehicles'] = cursor.fetchone()[0]
            return stats

    # (Kept for compatibility if used elsewhere)
    def write_parts_csv(self, parts_data: List[dict], append: bool = True):
        """Legacy: writes arbitrary rows; prefer append_parts_rows() per-group instead."""
        if not parts_data:
            return
        config.CSV_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(parts_data)
        df = df.reindex(columns=PART_ROW_HEADERS, fill_value='')
        mode = 'a' if (append and config.CSV_OUTPUT.exists()) else 'w'
        df.to_csv(config.CSV_OUTPUT, mode=mode, header=(mode == 'w'), index=False)
        logger.info(f"Wrote {len(parts_data)} parts to CSV: {config.CSV_OUTPUT}")

    def write_parts_parquet(self, parts_data: List[dict], append: bool = True):
        """Legacy: writes arbitrary rows; prefer append_parts_rows() per-group instead."""
        if not parts_data:
            return
        config.PARQUET_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(parts_data)
        df = df.reindex(columns=PART_ROW_HEADERS, fill_value='')
        if append and config.PARQUET_OUTPUT.exists():
            existing_df = pd.read_parquet(config.PARQUET_OUTPUT)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(config.PARQUET_OUTPUT, index=False)
        else:
            df.to_parquet(config.PARQUET_OUTPUT, index=False)
        logger.info(f"Wrote {len(parts_data)} parts to Parquet: {config.PARQUET_OUTPUT}")

    def convert_parts_to_dict_list(self, vehicle: Vehicle, table_index: TableIndex, parts_page: PartsPage, part_rows: List[PartRow]) -> List[dict]:
        """Convert parts data to dictionary list for CSV/Parquet output."""
        result = []
        for part_row in part_rows:
            result.append({
                'vehicle_id'     : vehicle.vehicle_id,
                'vehicle_name'   : vehicle.vehicle_name,
                'model_code'     : vehicle.model_code,
                'group_type'     : table_index.group_type,
                'table_no'       : table_index.table_no,
                'group_code'     : table_index.group_code,
                'group_desc'     : getattr(table_index, 'group_desc', ''),
                'ref_no'         : part_row.ref_no,
                'part_no'        : part_row.part_no,
                'description'    : part_row.description,
                'remark'         : part_row.remark,
                'req_no'         : part_row.req_no,
                'moq'            : part_row.moq,
                'mrp'            : part_row.mrp,
                'image_path'     : parts_page.image_path or '',
                'parts_page_url' : parts_page.parts_page_url,
                'source_url'     : vehicle.source_url
            })
        return result
