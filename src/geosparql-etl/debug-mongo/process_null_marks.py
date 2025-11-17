"""
Process marks with null or empty execution_id
CORRECTED for actual schema and proper URI structure
"""

import gzip
import hashlib
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
NUM_WORKERS = 24
BATCH_SIZE = 1000  # Match main ETL
OUTPUT_DIR = Path("ttl_output_null_marks")
LOG_FILE = "etl_null_marks.log"
GZIP_COMPRESSION_LEVEL = 6

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"

# SNOMED code for nuclear material
NUCLEAR_MATERIAL_SNOMED = "68841002"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_image_hash(image_id):
    """Generate SHA-256 hash for image ID (matches main ETL)"""
    return hashlib.sha256(image_id.encode()).hexdigest()


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


def create_null_marks_ttl_header(
    batch_num, mark_type, slide_id=None, default_width=40000, default_height=40000
):
    """Create TTL header matching main ETL structure"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    # Generate image hash (use slide_id if available, otherwise generic)
    if slide_id:
        image_hash = get_image_hash(slide_id)
        case_id = slide_id
    else:
        image_hash = get_image_hash(f"null_marks_{mark_type}")
        case_id = f"null-marks-{mark_type}"

    # TTL header with proper prefixes (matching main ETL)
    ttl_content = """@prefix dc:   <http://purl.org/dc/terms/> .
@prefix exif: <http://www.w3.org/2003/12/exif/ns#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix hal:  <https://halcyon.is/ns/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sno:  <http://snomed.info/id/> .
@prefix so:   <https://schema.org/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

"""

    # Add image object (using urn:sha256: format)
    ttl_content += f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{case_id}";
        exif:height  "{default_height}"^^xsd:int;
        exif:width   "{default_width}"^^xsd:int .

"""

    # Start feature collection with <> as subject (self-reference)
    ttl_content += f"""<>      a                    geo:FeatureCollection;
        dc:creator           "http://orcid.org/0000-0003-4165-4062";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Marks with {mark_type} execution_id - batch {batch_num}";
        dc:publisher         <https://ror.org/01882y777> , <https://ror.org/05qghxh33>;
        dc:title             "null-execution-id-marks";
        hal:executionId      "null-or-missing";
        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:{image_hash}>
                             ];
"""

    return ttl_content, default_width, default_height


def polygon_to_wkt(geometry, image_width, image_height):
    """Convert MongoDB polygon to WKT (matches main ETL)"""
    try:
        if not geometry or geometry.get("type") != "Polygon":
            return None

        coords = geometry.get("coordinates", [[]])[0]
        if not coords:
            return None

        # Denormalize and format
        wkt_coords = []
        for x, y in coords:
            px = x * image_width
            py = y * image_height
            wkt_coords.append(f"{px:.2f} {py:.2f}")

        # Close polygon
        if wkt_coords and wkt_coords[0] != wkt_coords[-1]:
            wkt_coords.append(wkt_coords[0])

        return f"POLYGON (({', '.join(wkt_coords)}))"
    except:
        return None


def add_mark_to_ttl(mark, image_width, image_height, is_first_feature):
    """Add mark to TTL string (matches main ETL with inline blank nodes)"""
    try:
        # Get geometry
        geometries = mark.get("geometries", {})
        features = geometries.get("features", [])
        if not features:
            return "", False

        geometry = features[0].get("geometry", {})
        wkt = polygon_to_wkt(geometry, image_width, image_height)
        if not wkt:
            return "", False

        # Get properties
        properties = features[0].get("properties", {})

        # Build feature TTL with inline blank node
        feature_ttl = ""

        # Add separator for multiple features
        if not is_first_feature:
            feature_ttl += ";\n"

        # Start feature with inline blank node (no explicit URI)
        snomed_id = NUCLEAR_MATERIAL_SNOMED
        feature_ttl += f"""        rdfs:member          [ a                   geo:Feature;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}" ];
                               hal:classification  sno:{snomed_id}"""

        # Add optional properties
        if "AreaInPixels" in properties:
            area_pixels = properties["AreaInPixels"]
            feature_ttl += f""";
                               hal:areaInPixels    "{int(area_pixels)}"^^xsd:int"""

        if "PhysicalSize" in properties:
            physical_size = properties["PhysicalSize"]
            feature_ttl += f""";
                               hal:physicalSize    "{float(physical_size):.6f}"^^xsd:float"""

        # Add measurement
        feature_ttl += """;
                               hal:measurement     [ hal:hasProbability  "1.0"^^xsd:float ]
                             ]"""

        return feature_ttl, True

    except Exception:
        return "", False


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
            processed = 0
            valid_marks = 0

            # Start TTL content
            ttl_content, img_width, img_height = create_null_marks_ttl_header(
                f"{worker_id}_{batch_num}", mark_type
            )
            is_first_feature = True
            batch_marks = 0

            for mark in marks_cursor:
                try:
                    # Add mark to TTL
                    mark_ttl, success = add_mark_to_ttl(
                        mark, img_width, img_height, is_first_feature
                    )

                    if success:
                        ttl_content += mark_ttl
                        is_first_feature = False
                        batch_marks += 1
                        valid_marks += 1

                    processed += 1

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        # Close feature collection
                        ttl_content += " .\n"

                        # Write file
                        output_file = (
                            OUTPUT_DIR
                            / mark_type
                            / f"batch_{worker_id:02d}_{batch_num:06d}.ttl.gz"
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
                        batch_marks = 0
                        ttl_content, img_width, img_height = (
                            create_null_marks_ttl_header(
                                f"{worker_id}_{batch_num}", mark_type
                            )
                        )
                        is_first_feature = True

                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error processing mark: {e}")
                    processed += 1
                    continue

            # Write final batch
            if batch_marks > 0:
                ttl_content += " .\n"

                output_file = (
                    OUTPUT_DIR
                    / mark_type
                    / f"batch_{worker_id:02d}_{batch_num:06d}.ttl.gz"
                )
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with gzip.open(
                    output_file,
                    "wt",
                    encoding="utf-8",
                    compresslevel=GZIP_COMPRESSION_LEVEL,
                ) as f:
                    f.write(ttl_content)

            marks_cursor.close()

            if valid_marks > 0:
                logger.info(
                    f"Worker {worker_id}: Completed - {valid_marks:,} valid marks"
                )

            return ("completed", worker_id, valid_marks)

    except Exception as e:
        logger.error(f"Worker {worker_id}: Failed - {e}")
        return ("failed", worker_id, 0, str(e))


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
