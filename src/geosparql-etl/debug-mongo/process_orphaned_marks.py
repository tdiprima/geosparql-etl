"""
Process orphaned marks (marks without corresponding analysis documents)
OPTIMIZED VERSION for 24-core machine
Author: Assistant
"""

import gzip
import logging
import sys
import time
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import mongo_connection

# =====================
# CONFIG - OPTIMIZED FOR 24 CORES
# =====================
NUM_WORKERS = 24  # Use all available cores (was 20)
BATCH_SIZE = 1000  # Marks per TTL file
OUTPUT_DIR = Path("ttl_output_orphaned")
CHECKPOINT_DIR = Path("checkpoints_orphaned")
LOG_FILE = "etl_orphaned.log"
GZIP_COMPRESSION_LEVEL = 6

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
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

        # OPTIMIZATION: Check if we should use execution_id instead
        # Try to detect the correct field to use
        sample = db.mark.find_one()

        use_execution_id = False
        if sample:
            if "analysis_id" not in sample and "provenance" in sample:
                if "analysis" in sample["provenance"]:
                    if "execution_id" in sample["provenance"]["analysis"]:
                        use_execution_id = True
                        logger.info(
                            "Using provenance.analysis.execution_id (indexed field)"
                        )

        if use_execution_id:
            # Use indexed field!
            unique_analysis_ids = db.mark.distinct("provenance.analysis.execution_id")
            field_name = "provenance.analysis.execution_id"
        else:
            # Fall back to analysis_id (hopefully indexed by now!)
            unique_analysis_ids = db.mark.distinct("analysis_id")
            field_name = "analysis_id"

        logger.info(f"Using field: {field_name}")
        logger.info(f"Found {len(unique_analysis_ids):,} unique values")

        # Check which don't have analysis documents
        # OPTIMIZATION: Use batch checking for better performance
        chunk_size = 1000
        for i in range(0, len(unique_analysis_ids), chunk_size):
            chunk = unique_analysis_ids[i : i + chunk_size]

            # Batch check existence
            existing_ids = set(
                doc["_id"]
                for doc in db.analysis.find(
                    {"_id": {"$in": [aid for aid in chunk if aid and aid != ""]}},
                    {"_id": 1},
                )
            )

            # Find orphaned in this chunk
            for aid in chunk:
                if aid and aid != "" and aid not in existing_ids:
                    orphaned_ids.append(aid)

            if (i + chunk_size) % 10000 == 0:
                logger.info(
                    f"Checked {i+chunk_size:,} IDs, found {len(orphaned_ids):,} orphaned"
                )

        logger.info(f"Found {len(orphaned_ids):,} orphaned analysis_ids")

    return orphaned_ids, field_name


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


def process_orphaned_marks(args):
    """Process all marks for an orphaned analysis_id"""

    analysis_id, field_name = args

    try:
        # Create new connection for this worker
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            # Count marks for this analysis
            query = {field_name: analysis_id}
            mark_count = db.mark.count_documents(query)
            logger.info(
                f"Processing {mark_count:,} marks for orphaned analysis {analysis_id}"
            )

            if mark_count == 0:
                return ("empty", analysis_id, 0)

            # OPTIMIZATION: Build TTL in memory first, write once
            current_batch = []

            # Process marks in batches
            marks_cursor = db.mark.find(query, batch_size=BATCH_SIZE * 2)

            batch_num = 1
            processed = 0

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

                    # Add mark to current batch
                    mark_id = str(mark.get("_id", f"mark_{processed}"))
                    current_batch.append(mark_id)

                    processed += 1

                    # Write batch when full
                    if len(current_batch) >= BATCH_SIZE:
                        # Build TTL for this batch
                        ttl_content = create_minimal_ttl_header(analysis_id, batch_num)

                        for i, mid in enumerate(current_batch):
                            if i > 0:
                                ttl_content += " ,\n        "
                            ttl_content += f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mid}>"""

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

                        # Start new batch
                        batch_num += 1
                        current_batch = []

                except Exception as e:
                    logger.error(f"Error processing mark: {e}")
                    continue

            # Write final batch
            if current_batch:
                ttl_content = create_minimal_ttl_header(analysis_id, batch_num)

                for i, mid in enumerate(current_batch):
                    if i > 0:
                        ttl_content += " ,\n        "
                    ttl_content += (
                        f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mid}>"""
                    )

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

            marks_cursor.close()
            return ("completed", analysis_id, processed)

    except Exception as e:
        logger.error(f"Failed processing {analysis_id}: {e}")
        return ("failed", analysis_id, 0, str(e))


def main():
    """Main function to process all orphaned marks"""

    logger.info("=" * 60)
    logger.info("ORPHANED MARKS ETL PROCESS (OPTIMIZED)")
    logger.info("=" * 60)
    logger.info(f"Using {NUM_WORKERS} worker processes")
    logger.info(f"Available CPU cores: {cpu_count()}")

    # Get orphaned analysis IDs
    orphaned_ids, field_name = get_orphaned_analysis_ids()

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
    to_process = [(aid, field_name) for aid in orphaned_ids if aid not in completed]
    logger.info(f"Need to process {len(to_process):,} orphaned analysis IDs")

    if not to_process:
        logger.info("All orphaned marks already processed!")
        return

    # Process in parallel
    total_marks = 0
    completed_count = 0
    failed_count = 0
    start_time = time.time()

    # OPTIMIZATION: Use chunksize for better load balancing
    chunksize = max(1, len(to_process) // (NUM_WORKERS * 4))

    with Pool(processes=NUM_WORKERS) as pool:
        for i, result in enumerate(
            pool.imap_unordered(
                process_orphaned_marks, to_process, chunksize=chunksize
            ),
            1,
        ):
            status = result[0]
            analysis_id = result[1]

            if status == "completed":
                mark_count = result[2]
                total_marks += mark_count
                completed_count += 1

                # Save checkpoint
                with open(checkpoint_file, "a") as f:
                    f.write(f"{analysis_id}\n")

                # Progress report
                if i % 10 == 0 or i == len(to_process):
                    elapsed = time.time() - start_time
                    rate = total_marks / elapsed if elapsed > 0 else 0
                    percent = (i / len(to_process)) * 100
                    eta = ((len(to_process) - i) / (i / elapsed)) / 60 if i > 0 else 0

                    logger.info(
                        f"Progress: {i}/{len(to_process)} ({percent:.1f}%) - "
                        f"Marks: {total_marks:,} - "
                        f"Rate: {rate:.0f} marks/s - "
                        f"ETA: {eta:.1f} min"
                    )

            elif status == "failed":
                failed_count += 1
                error = result[3] if len(result) > 3 else "Unknown error"
                logger.error(f"Failed {analysis_id}: {error}")

    # Final stats
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Orphaned marks ETL complete!")
    logger.info(f"Completed: {completed_count:,} / {len(to_process):,}")
    logger.info(f"Failed: {failed_count:,}")
    logger.info(f"Total orphaned marks processed: {total_marks:,}")
    logger.info(f"Time elapsed: {elapsed/3600:.2f} hours ({elapsed/60:.1f} minutes)")
    logger.info(f"Average rate: {total_marks/elapsed:.0f} marks/sec")
    logger.info(f"Workers used: {NUM_WORKERS}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
