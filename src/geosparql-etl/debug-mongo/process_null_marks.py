"""
Process marks with null or empty execution_id
CORRECTED for actual schema (uses provenance.analysis.execution_id)

NOTE: Based on schema analysis, your database has NO analysis_id field.
All marks use provenance.analysis.execution_id instead.

This script processes marks that have null/missing execution_id values.
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


def check_if_needed():
    """Check if there are actually any marks with null/missing execution_id"""

    logger.info("=" * 60)
    logger.info("NULL/MISSING EXECUTION_ID CHECK")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # Check for marks without execution_id field
        logger.info("Checking for marks without execution_id field...")

        # Sample to see if any exist
        sample_missing = db.mark.find_one(
            {"provenance.analysis.execution_id": {"$exists": False}}
        )

        sample_null = db.mark.find_one({"provenance.analysis.execution_id": None})

        sample_empty = db.mark.find_one({"provenance.analysis.execution_id": ""})

        if not sample_missing and not sample_null and not sample_empty:
            logger.info("✅ All marks have valid execution_id values!")
            logger.info("   No null/missing marks to process.")
            logger.info("   This script is not needed for your database.")
            return False

        logger.info("⚠️  Found marks with null/missing execution_id")

        if sample_missing:
            logger.info("  - Some marks missing execution_id field entirely")
        if sample_null:
            logger.info("  - Some marks have execution_id = null")
        if sample_empty:
            logger.info("  - Some marks have execution_id = empty string")

        return True


def create_null_marks_ttl_header(batch_num, mark_type):
    """Create TTL header for null/empty execution_id marks"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    ttl = f"""# Marks with {mark_type} execution_id
# Generated: {timestamp}
# Batch: {batch_num}

@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix camic: <https://grlc.io/api/MathurMihir/camic-apis/> .

# Collection of marks without execution_id
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
    query = dict(query_base)
    query["_id"] = {"$gte": range_start, "$lt": range_end}
    return query


def create_id_ranges(num_ranges):
    """Create _id ranges to partition the collection"""
    hex_chars = "0123456789abcdef"
    ranges = []
    chars_per_range = max(1, len(hex_chars) // num_ranges)

    for i in range(0, len(hex_chars), chars_per_range):
        start_char = hex_chars[i]
        end_char = hex_chars[min(i + chars_per_range, len(hex_chars) - 1)]

        range_start = start_char + "0" * 23
        range_end = end_char + "f" * 23

        ranges.append((range_start, range_end))

        if len(ranges) >= num_ranges:
            break

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
                        batch_num += 1
                        batch_marks = []

                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error processing mark: {e}")
                    processed += 1
                    continue

            # Write final batch
            if batch_marks:
                write_batch(batch_marks, mark_type, worker_id, batch_num)

            marks_cursor.close()

            if valid_marks > 0:
                logger.info(
                    f"Worker {worker_id}: Completed - {valid_marks:,} valid marks"
                )

            return ("completed", worker_id, valid_marks)

    except Exception as e:
        logger.error(f"Worker {worker_id}: Failed - {e}")
        return ("failed", worker_id, 0, str(e))


def write_batch(batch_marks, mark_type, worker_id, batch_num):
    """Write a batch of marks to TTL file"""

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

    output_file = (
        OUTPUT_DIR / mark_type / f"batch_{worker_id:02d}_{batch_num:06d}.ttl.gz"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(
        output_file, "wt", encoding="utf-8", compresslevel=GZIP_COMPRESSION_LEVEL
    ) as f:
        f.write(ttl_content)


def process_null_marks_parallel():
    """Process all marks with null or empty execution_id in parallel"""

    logger.info("=" * 60)
    logger.info("NULL/EMPTY EXECUTION_ID MARKS PROCESSING")
    logger.info("=" * 60)
    logger.info(f"Using {NUM_WORKERS} worker processes")

    # First check if this script is even needed
    if not check_if_needed():
        logger.info("\n✅ Script completed - nothing to process")
        return

    start_time = time.time()
    id_ranges = create_id_ranges(NUM_WORKERS)

    total_marks = 0

    # Process marks missing execution_id field
    logger.info("\nProcessing marks missing execution_id field...")
    work_items = [
        (
            {"provenance.analysis.execution_id": {"$exists": False}},
            start,
            end,
            "missing",
            i,
        )
        for i, (start, end) in enumerate(id_ranges)
    ]

    with Pool(processes=NUM_WORKERS) as pool:
        for result in pool.imap_unordered(process_mark_range, work_items):
            if result[0] == "completed":
                total_marks += result[2]

    # Process null execution_id
    logger.info("\nProcessing marks with null execution_id...")
    work_items = [
        ({"provenance.analysis.execution_id": None}, start, end, "null", i)
        for i, (start, end) in enumerate(id_ranges)
    ]

    with Pool(processes=NUM_WORKERS) as pool:
        for result in pool.imap_unordered(process_mark_range, work_items):
            if result[0] == "completed":
                total_marks += result[2]

    # Process empty execution_id
    logger.info("\nProcessing marks with empty execution_id...")
    work_items = [
        ({"provenance.analysis.execution_id": ""}, start, end, "empty", i)
        for i, (start, end) in enumerate(id_ranges)
    ]

    with Pool(processes=NUM_WORKERS) as pool:
        for result in pool.imap_unordered(process_mark_range, work_items):
            if result[0] == "completed":
                total_marks += result[2]

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info(f"Total marks processed: {total_marks:,}")
    logger.info(f"Time elapsed: {elapsed/60:.2f} minutes")
    logger.info("=" * 60)


if __name__ == "__main__":
    process_null_marks_parallel()
