"""
Process orphaned marks (marks without corresponding analysis documents)
These are marks that were missed in the main ETL process
Author: Assistant
"""

import gzip
import logging
import sys
import time
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import mongo_connection

# =====================
# CONFIG
# =====================
NUM_WORKERS = 20  # Number of parallel workers
BATCH_SIZE = 1000  # Marks per TTL file
OUTPUT_DIR = Path("ttl_output_orphaned")  # Separate output directory
CHECKPOINT_DIR = Path("checkpoints_orphaned")
LOG_FILE = "etl_orphaned.log"
GZIP_COMPRESSION_LEVEL = 6

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"  # Or "localhost"
MONGO_PORT = 27017  # Or 27018
MONGO_DB = "camic"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR.mkdir(exist_ok=True)

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_orphaned_analysis_ids():
    """Find all analysis_ids that exist in marks but not in analysis collection"""
    orphaned_ids = []

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        logger.info("Finding orphaned analysis_ids...")

        # Get all unique analysis_ids from marks
        unique_analysis_ids = db.mark.distinct("analysis_id")

        # Check which don't have analysis documents
        for aid in unique_analysis_ids:
            if aid and aid != "":  # Skip null/empty
                exists = db.analysis.count_documents({"_id": aid}, limit=1) > 0
                if not exists:
                    orphaned_ids.append(aid)

        logger.info(f"Found {len(orphaned_ids):,} orphaned analysis_ids")

    return orphaned_ids


def create_minimal_ttl_header(analysis_id, batch_num):
    """Create minimal TTL header for orphaned marks"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    ttl = f"""# Orphaned Marks from analysis_id: {analysis_id}
# Generated: {timestamp}
# Batch: {batch_num}

@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix camic: <https://grlc.io/api/MathurMihir/camic-apis/> .

# Orphaned marks collection for analysis {analysis_id}
<https://grlc.io/api/MathurMihir/camic-apis/analysis/{analysis_id}/orphaned_batch_{batch_num}>
    a geo:FeatureCollection ;
    prov:generatedAtTime "{timestamp}"^^xsd:dateTime ;
    geo:hasFeature """

    return ttl


def polygon_to_wkt(geometry, default_width=40000, default_height=40000):
    """Convert MongoDB polygon to WKT with default dimensions"""
    try:
        if not geometry or geometry.get("type") != "Polygon":
            return None

        coords = geometry.get("coordinates", [[]])[0]
        if not coords:
            return None

        # Use default dimensions since we don't have analysis doc
        wkt_coords = []
        for x, y in coords:
            px = x * default_width
            py = y * default_height
            wkt_coords.append(f"{px:.2f} {py:.2f}")

        # Close polygon if needed
        if wkt_coords and wkt_coords[0] != wkt_coords[-1]:
            wkt_coords.append(wkt_coords[0])

        return f"POLYGON (({', '.join(wkt_coords)}))"
    except:
        return None


def process_orphaned_marks(analysis_id):
    """Process all marks for an orphaned analysis_id"""

    try:
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            # Count marks for this analysis
            mark_count = db.mark.count_documents({"analysis_id": analysis_id})
            logger.info(
                f"Processing {mark_count:,} marks for orphaned analysis {analysis_id}"
            )

            if mark_count == 0:
                return ("empty", analysis_id, 0)

            # Process marks in batches
            marks_cursor = db.mark.find(
                {"analysis_id": analysis_id}, batch_size=BATCH_SIZE * 2
            )

            batch_num = 1
            batch_marks = 0
            processed = 0
            ttl_content = create_minimal_ttl_header(analysis_id, batch_num)
            is_first_feature = True

            for mark in marks_cursor:
                try:
                    # Extract geometry
                    geometry = (
                        mark.get("geometries", {})
                        .get("features", [{}])[0]
                        .get("geometry")
                    )
                    wkt = polygon_to_wkt(geometry)

                    if not wkt:
                        continue

                    # Add mark to TTL
                    mark_id = str(mark.get("_id", f"mark_{processed}"))

                    if not is_first_feature:
                        ttl_content += " ,\n        "

                    ttl_content += f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mark_id}>"""
                    is_first_feature = False

                    batch_marks += 1
                    processed += 1

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        # Close feature collection
                        ttl_content += " .\n"

                        # Write file
                        output_file = (
                            OUTPUT_DIR / analysis_id / f"batch_{batch_num:06d}.ttl.gz"
                        )
                        output_file.parent.mkdir(parents=True, exist_ok=True)

                        with gzip.open(
                            output_file,
                            "wt",
                            encoding="utf-8",
                            compresslevel=GZIP_COMPRESSION_LEVEL,
                        ) as f:
                            f.write(ttl_content)

                        logger.info(
                            f"Wrote batch {batch_num} for {analysis_id}: {batch_marks} marks"
                        )

                        # Start new batch
                        batch_num += 1
                        batch_marks = 0
                        ttl_content = create_minimal_ttl_header(analysis_id, batch_num)
                        is_first_feature = True

                except Exception as e:
                    logger.error(f"Error processing mark: {e}")
                    continue

            # Write final batch
            if batch_marks > 0:
                ttl_content += " .\n"
                output_file = OUTPUT_DIR / analysis_id / f"batch_{batch_num:06d}.ttl.gz"
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with gzip.open(
                    output_file,
                    "wt",
                    encoding="utf-8",
                    compresslevel=GZIP_COMPRESSION_LEVEL,
                ) as f:
                    f.write(ttl_content)

                logger.info(
                    f"Wrote final batch {batch_num} for {analysis_id}: {batch_marks} marks"
                )

            marks_cursor.close()
            return ("completed", analysis_id, processed)

    except Exception as e:
        logger.error(f"Failed processing {analysis_id}: {e}")
        return ("failed", analysis_id, 0, str(e))


def process_worker(args):
    """Worker function for multiprocessing"""
    analysis_id = args
    return process_orphaned_marks(analysis_id)


def main():
    """Main function to process all orphaned marks"""

    logger.info("=" * 60)
    logger.info("ORPHANED MARKS ETL PROCESS")
    logger.info("=" * 60)

    # Get orphaned analysis IDs
    orphaned_ids = get_orphaned_analysis_ids()

    if not orphaned_ids:
        logger.info("No orphaned marks found!")
        return

    # Load checkpoint if exists
    checkpoint_file = CHECKPOINT_DIR / "completed_orphaned.txt"
    completed = set()

    if checkpoint_file.exists():
        with open(checkpoint_file, "r") as f:
            for line in f:
                completed.add(line.strip())

    # Filter out already processed
    to_process = [aid for aid in orphaned_ids if aid not in completed]
    logger.info(f"Need to process {len(to_process):,} orphaned analysis IDs")

    if not to_process:
        logger.info("All orphaned marks already processed!")
        return

    # Process in parallel
    total_marks = 0
    start_time = time.time()

    with Pool(processes=NUM_WORKERS) as pool:
        for i, result in enumerate(pool.imap_unordered(process_worker, to_process), 1):
            status = result[0]
            analysis_id = result[1]

            if status == "completed":
                mark_count = result[2]
                total_marks += mark_count

                # Save checkpoint
                with open(checkpoint_file, "a") as f:
                    f.write(f"{analysis_id}\n")

                logger.info(
                    f"Completed {i}/{len(to_process)}: {analysis_id} - {mark_count:,} marks"
                )

                # Progress report
                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = total_marks / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {i}/{len(to_process)} - Total marks: {total_marks:,} - Rate: {rate:.0f} marks/sec"
                    )

            elif status == "failed":
                error = result[3] if len(result) > 3 else "Unknown error"
                logger.error(f"Failed {analysis_id}: {error}")

    # Final stats
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Orphaned marks ETL complete!")
    logger.info(f"Total orphaned marks processed: {total_marks:,}")
    logger.info(f"Time elapsed: {elapsed/3600:.2f} hours")
    logger.info(f"Average rate: {total_marks/elapsed:.0f} marks/sec")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
