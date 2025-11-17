"""
Process marks with null or empty analysis_id
PARALLELIZED VERSION for multi-core machines
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
# CONFIG
# =====================
NUM_WORKERS = 24  # Number of parallel workers
BATCH_SIZE = 10000  # Larger batches for efficiency
OUTPUT_DIR = Path("ttl_output_null_marks")
LOG_FILE = "etl_null_marks.log"
GZIP_COMPRESSION_LEVEL = 6

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def create_null_marks_ttl_header(batch_num, mark_type):
    """Create TTL header for null/empty analysis_id marks"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    ttl = f"""# Marks with {mark_type} analysis_id
# Generated: {timestamp}
# Batch: {batch_num}

@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix camic: <https://grlc.io/api/MathurMihir/camic-apis/> .

# Collection of marks without analysis_id
<https://grlc.io/api/MathurMihir/camic-apis/{mark_type}_marks/batch_{batch_num}>
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

        wkt_coords = []
        for x, y in coords:
            px = x * default_width
            py = y * default_height
            wkt_coords.append(f"{px:.2f} {py:.2f}")

        if wkt_coords and wkt_coords[0] != wkt_coords[-1]:
            wkt_coords.append(wkt_coords[0])

        return f"POLYGON (({', '.join(wkt_coords)}))"
    except:
        return None


def get_id_range_query(query_base, range_start, range_end):
    """Create query for a specific _id range"""
    # MongoDB ObjectIds are strings that can be compared lexicographically
    query = dict(query_base)
    query["_id"] = {"$gte": range_start, "$lt": range_end}
    return query


def create_id_ranges(num_ranges):
    """
    Create _id ranges to partition the collection
    ObjectIds are 24-char hex strings, we can partition by first char
    """
    # Hex chars: 0-9, a-f (16 chars)
    hex_chars = "0123456789abcdef"

    # Create ranges based on first character of _id
    ranges = []
    chars_per_range = max(1, len(hex_chars) // num_ranges)

    for i in range(0, len(hex_chars), chars_per_range):
        start_char = hex_chars[i]
        end_char = hex_chars[min(i + chars_per_range, len(hex_chars) - 1)]

        # ObjectIds are 24 chars long
        range_start = start_char + "0" * 23
        range_end = end_char + "f" * 23

        ranges.append((range_start, range_end))

        if len(ranges) >= num_ranges:
            break

    # Ensure we cover the full range
    if ranges:
        ranges[-1] = (ranges[-1][0], "f" * 24)

    return ranges


def process_mark_range(args):
    """Process a range of marks (for parallel processing)"""

    query_base, range_start, range_end, mark_type, worker_id = args

    try:
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            # Create query for this range
            query = get_id_range_query(query_base, range_start, range_end)

            # Count marks in this range
            mark_count = db.mark.count_documents(query)
            logger.info(
                f"Worker {worker_id}: Processing {mark_count:,} {mark_type} marks in range {range_start[:4]}...{range_end[:4]}"
            )

            if mark_count == 0:
                return ("empty", worker_id, 0)

            # Process marks
            marks_cursor = db.mark.find(query, batch_size=BATCH_SIZE * 2)

            batch_num = 1
            batch_marks = []
            processed = 0
            valid_marks = 0

            for mark in marks_cursor:
                try:
                    # Extract geometry
                    geometry = None
                    if "geometries" in mark and "features" in mark["geometries"]:
                        features = mark["geometries"]["features"]
                        if features and len(features) > 0:
                            geometry = features[0].get("geometry")
                    elif "geometry" in mark:
                        geometry = mark["geometry"]

                    wkt = polygon_to_wkt(geometry)

                    if not wkt:
                        processed += 1
                        continue

                    # Store mark info
                    mark_id = str(mark.get("_id", f"{mark_type}_mark_{valid_marks}"))
                    classification = mark.get("properties", {}).get(
                        "classification", ""
                    )

                    batch_marks.append((mark_id, wkt, classification))
                    valid_marks += 1
                    processed += 1

                    # Write batch when full
                    if len(batch_marks) >= BATCH_SIZE:
                        write_batch(batch_marks, mark_type, worker_id, batch_num)
                        logger.info(
                            f"Worker {worker_id}: Wrote batch {batch_num}: {len(batch_marks)} marks"
                        )

                        batch_num += 1
                        batch_marks = []

                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error processing mark: {e}")
                    processed += 1
                    continue

            # Write final batch
            if batch_marks:
                write_batch(batch_marks, mark_type, worker_id, batch_num)
                logger.info(
                    f"Worker {worker_id}: Wrote final batch {batch_num}: {len(batch_marks)} marks"
                )

            marks_cursor.close()

            logger.info(
                f"Worker {worker_id}: Completed - {valid_marks:,} valid marks from {processed:,} total"
            )
            return ("completed", worker_id, valid_marks)

    except Exception as e:
        logger.error(f"Worker {worker_id}: Failed - {e}")
        return ("failed", worker_id, 0, str(e))


def write_batch(batch_marks, mark_type, worker_id, batch_num):
    """Write a batch of marks to TTL file"""

    # Create TTL content
    ttl_content = create_null_marks_ttl_header(f"{worker_id}_{batch_num}", mark_type)

    for i, (mark_id, wkt, classification) in enumerate(batch_marks):
        if i > 0:
            ttl_content += " ,\n        "

        ttl_content += f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mark_id}> [
            a geo:Feature ;
            geo:hasGeometry [
                a geo:Geometry ;
                geo:asWKT "{wkt}"^^geo:wktLiteral
            ]"""

        if classification:
            ttl_content += f""" ;
            camic:classification "{classification}" """

        ttl_content += "\n        ]"

    ttl_content += " .\n"

    # Write file
    output_file = (
        OUTPUT_DIR / mark_type / f"batch_{worker_id:02d}_{batch_num:06d}.ttl.gz"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(
        output_file, "wt", encoding="utf-8", compresslevel=GZIP_COMPRESSION_LEVEL
    ) as f:
        f.write(ttl_content)


def process_null_marks_parallel():
    """Process all marks with null or empty analysis_id in parallel"""

    logger.info("=" * 60)
    logger.info("NULL/EMPTY ANALYSIS_ID MARKS PROCESSING (PARALLEL)")
    logger.info("=" * 60)
    logger.info(f"Using {NUM_WORKERS} worker processes")
    logger.info(f"Available CPU cores: {cpu_count()}")

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        # Count null and empty marks
        null_count = db.mark.count_documents({"analysis_id": None})
        empty_count = db.mark.count_documents({"analysis_id": ""})
        total_to_process = null_count + empty_count

        logger.info(f"Marks with null analysis_id: {null_count:,}")
        logger.info(f"Marks with empty analysis_id: {empty_count:,}")
        logger.info(f"Total to process: {total_to_process:,}")

        if total_to_process == 0:
            logger.info("No null/empty marks to process!")
            return

    start_time = time.time()

    # Create ID ranges for parallel processing
    id_ranges = create_id_ranges(NUM_WORKERS)
    logger.info(f"Created {len(id_ranges)} ID ranges for parallel processing")

    # Process null marks
    total_marks = 0
    if null_count > 0:
        logger.info("\nProcessing null analysis_id marks in parallel...")

        # Create work items
        work_items = [
            ({"analysis_id": None}, start, end, "null", i)
            for i, (start, end) in enumerate(id_ranges)
        ]

        with Pool(processes=NUM_WORKERS) as pool:
            for result in pool.imap_unordered(process_mark_range, work_items):
                status = result[0]
                worker_id = result[1]

                if status == "completed":
                    mark_count = result[2]
                    total_marks += mark_count
                    logger.info(
                        f"Worker {worker_id} completed: {mark_count:,} null marks"
                    )
                elif status == "failed":
                    error = result[3] if len(result) > 3 else "Unknown"
                    logger.error(f"Worker {worker_id} failed: {error}")

    # Process empty marks
    if empty_count > 0:
        logger.info("\nProcessing empty analysis_id marks in parallel...")

        work_items = [
            ({"analysis_id": ""}, start, end, "empty", i)
            for i, (start, end) in enumerate(id_ranges)
        ]

        with Pool(processes=NUM_WORKERS) as pool:
            for result in pool.imap_unordered(process_mark_range, work_items):
                status = result[0]
                worker_id = result[1]

                if status == "completed":
                    mark_count = result[2]
                    total_marks += mark_count
                    logger.info(
                        f"Worker {worker_id} completed: {mark_count:,} empty marks"
                    )
                elif status == "failed":
                    error = result[3] if len(result) > 3 else "Unknown"
                    logger.error(f"Worker {worker_id} failed: {error}")

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info(f"Total marks processed: {total_marks:,}")
    logger.info(f"Time elapsed: {elapsed/60:.2f} minutes")
    logger.info(f"Average rate: {total_marks/elapsed:.0f} marks/sec")
    logger.info(f"Workers used: {NUM_WORKERS}")
    logger.info("=" * 60)


if __name__ == "__main__":
    process_null_marks_parallel()
