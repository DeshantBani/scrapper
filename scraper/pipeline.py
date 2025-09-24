"""Main pipeline orchestrator."""
import logging
from typing import List, Tuple, Optional

from . import config
from .browser import launch_browser, new_page
from .session import make_session
from .store import DataStore
from . import catalogue as catalog, aggregates, parts
from .datamodel import Vehicle, TableIndex

logger = logging.getLogger(__name__)


class ScrapingPipeline:
    """Main scraping pipeline coordinator."""

    def __init__(self,
                 force_reprocess: bool = False,
                 save_parquet: bool = False):
        self.force_reprocess = force_reprocess
        self.save_parquet = save_parquet
        self.store = DataStore()

        # run stats
        self.total_vehicles_seen: int = 0
        self.total_groups_seen: int = 0
        self.total_groups_done: int = 0
        self.total_groups_skipped: int = 0
        self.total_groups_failed: int = 0
        self.total_parts_rows: int = 0
        self.total_images_saved: int = 0

        # milestones (resume visibility)
        self._vehicles_processed: int = 0
        self._milestone_batch: int = 0
        self._milestone_every: int = 30  # write a milestone every N vehicles

    async def run(self, catalog_url: str):
        """Run the complete scraping pipeline."""
        config.BASE_URL = catalog_url
        config.ensure_directories()

        logger.info("Starting Hero scraper pipeline…")
        logger.info(f"Catalog URL: {catalog_url}")
        logger.info(f"Force reprocess: {self.force_reprocess}")
        logger.info(f"Save Parquet: {self.save_parquet}")

        session = make_session()
        browser, context = await launch_browser(headless=config.HEADLESS)

        try:
            page = await new_page(context)

            # Step 1: Collect vehicles
            vehicles = await self._collect_vehicles(page, catalog_url)

            # Step 2: Process each vehicle
            await self._process_vehicles(page, vehicles, session)

            # Step 3: Show final statistics
            self._show_final_stats()

        finally:
            try:
                await browser.close()
            finally:
                session.close()

        logger.info("Pipeline completed successfully!")

    async def _collect_vehicles(self, page, catalog_url: str) -> List[Vehicle]:
        """Collect all vehicles from catalog (with pagination)."""
        logger.info("=== Step 1: Collecting Vehicles ===")
        vehicles = await catalog.collect_vehicles(page, catalog_url)
        self.total_vehicles_seen = len(vehicles)

        for vehicle in vehicles:
            self.store.save_vehicle(vehicle)

        logger.info(f"Saved {len(vehicles)} vehicles to database")
        return vehicles

    async def _process_vehicles(self, page, vehicles: List[Vehicle], session):
        """Process all vehicles to extract parts data."""
        logger.info("=== Step 2: Processing Vehicles ===")

        prev_model: Optional[
            str] = None  # track previous to help detect stale DOM

        for idx, vehicle in enumerate(vehicles, 1):
            logger.info(
                f"[{idx}/{len(vehicles)}] Vehicle: {vehicle.vehicle_name} ({vehicle.model_code})"
            )

            try:
                # 2a) Collect Engine/Frame group indices for this vehicle
                indices: List[TableIndex] = await aggregates.collect_indices(
                    page, vehicle, prev_model_code=prev_model)

                # --- DO NOT FILTER HERE ---
                # No endswith(_model) filter, no dedupe, no slicing.

                # Small debug to ensure counts match what aggregates reported
                engine_count = sum(1 for x in indices
                                   if x.group_type == "ENGINE")
                frame_count = sum(1 for x in indices
                                  if x.group_type == "FRAME")
                logger.info(
                    f"[debug] indices raw from aggregates: total={len(indices)} (ENGINE={engine_count}, FRAME={frame_count})"
                )

                if not indices:
                    logger.warning(
                        f"No Engine/Frame groups found for vehicle: {vehicle.vehicle_name}"
                    )
                    prev_model = vehicle.model_code
                    # milestone tracking even if skipped due to 0 indices
                    self._after_vehicle(vehicle_id=vehicle.vehicle_id)
                    continue

                # Vehicle-level completeness gate:
                # If ALL groups for this vehicle are already 'done' (and not forcing), skip whole vehicle.
                if not self.force_reprocess and self.store.is_vehicle_complete(
                        vehicle.vehicle_id, expected_groups=len(indices)):
                    logger.info(
                        f"[skip vehicle done] {vehicle.vehicle_name} ({vehicle.model_code}) — all {len(indices)} groups already done."
                    )
                    prev_model = vehicle.model_code
                    self._after_vehicle(vehicle_id=vehicle.vehicle_id)
                    continue

                self.total_groups_seen += len(indices)
                logger.info(
                    f"Found {len(indices)} groups for {vehicle.vehicle_name}")

                # 2b) Iterate each group exactly as returned
                for j, ti in enumerate(indices, 1):
                    key = (vehicle.vehicle_id, ti.group_type, ti.table_no,
                           ti.group_code)
                    pretty = f"{vehicle.vehicle_name} | {ti.group_type} | {ti.table_no} | {ti.group_code}"

                    status = self._checkpoint_status(key)
                    if status == "done" and not self.force_reprocess:
                        self.total_groups_skipped += 1
                        logger.info(f"  - [skip done] {pretty}")
                        continue

                    # mark pending early (so resume-incomplete can find it if we crash)
                    self._checkpoint_mark_pending(key)

                    logger.info(f"  - [{j}/{len(indices)}] Scraping: {pretty}")

                    try:
                        # Scrape parts page
                        parts_page, part_rows, image_saved = await parts.collect_parts(
                            page=page,
                            vehicle=vehicle,
                            table_index=ti,
                            session=session)

                        # Save metadata & rows (UPSERT, dedup)
                        if parts_page is not None:
                            self.store.save_parts_page(parts_page)

                        if part_rows:
                            self.store.append_parts_rows(
                                vehicle=vehicle,
                                table_index=ti,
                                parts_page=parts_page,
                                part_rows=part_rows,
                                write_csv=True,
                                save_parquet=self.save_parquet,
                            )
                            self.total_parts_rows += len(part_rows)

                        if image_saved:
                            self.total_images_saved += 1

                        # Mark as done only after successful writes
                        self._checkpoint_mark_done(
                            key,
                            row_count=len(part_rows or []),
                            image_saved=bool(image_saved))
                        self.total_groups_done += 1

                        logger.info(
                            f"    ✓ done: rows={len(part_rows or [])}, image_saved={bool(image_saved)}"
                        )

                    except Exception as e:
                        self._checkpoint_mark_error(key, str(e))
                        self.total_groups_failed += 1
                        logger.exception(
                            f"    ✗ error scraping group: {pretty} :: {e}")

            except Exception as e:
                logger.exception(
                    f"Failed processing vehicle '{vehicle.vehicle_name}': {e}")

            # update previous model for the next iteration
            prev_model = vehicle.model_code

            # milestone / progress record every N vehicles
            self._after_vehicle(vehicle_id=vehicle.vehicle_id)

    # ----------------- small wrappers around store.py --------------------

    def _after_vehicle(self, vehicle_id: str):
        """Increment vehicle counter and write a milestone every N vehicles."""
        self._vehicles_processed += 1
        if self._vehicles_processed % self._milestone_every == 0:
            self._milestone_batch += 1
            try:
                self.store.mark_milestone(self._milestone_batch, vehicle_id)
            except Exception as e:
                logger.warning(
                    f"Could not write milestone batch {self._milestone_batch}: {e}"
                )
            logger.info(
                f"[milestone] Processed {self._vehicles_processed} vehicles; milestone batch {self._milestone_batch} recorded."
            )

    def _checkpoint_status(self, key: Tuple[str, str, str,
                                            str]) -> Optional[str]:
        if hasattr(self.store, "checkpoint_status"):
            return self.store.checkpoint_status(key)
        if hasattr(self.store, "get_checkpoint_status"):
            return self.store.get_checkpoint_status(key)
        return None

    def _checkpoint_mark_done(self, key: Tuple[str, str, str, str],
                              row_count: int, image_saved: bool):
        if hasattr(self.store, "checkpoint_mark_done"):
            return self.store.checkpoint_mark_done(key,
                                                   row_count=row_count,
                                                   image_saved=image_saved)
        if hasattr(self.store, "mark_done"):
            return self.store.mark_done(key,
                                        row_count=row_count,
                                        image_saved=image_saved)

    def _checkpoint_mark_error(self, key: Tuple[str, str, str, str],
                               error: str):
        if hasattr(self.store, "checkpoint_mark_error"):
            return self.store.checkpoint_mark_error(key, error=error)
        if hasattr(self.store, "mark_error"):
            return self.store.mark_error(key, error=error)

    def _checkpoint_mark_pending(self, key: Tuple[str, str, str, str]):
        if hasattr(self.store, "checkpoint_mark_pending"):
            return self.store.checkpoint_mark_pending(key)

    def _show_final_stats(self):
        logger.info("=== Step 3: Final Statistics ===")
        logger.info(f"Vehicles seen         : {self.total_vehicles_seen}")
        logger.info(f"Groups discovered     : {self.total_groups_seen}")
        logger.info(f"Groups completed      : {self.total_groups_done}")
        logger.info(f"Groups skipped (done) : {self.total_groups_skipped}")
        logger.info(f"Groups failed         : {self.total_groups_failed}")
        logger.info(f"Parts rows collected  : {self.total_parts_rows}")
        logger.info(f"Images saved          : {self.total_images_saved}")
