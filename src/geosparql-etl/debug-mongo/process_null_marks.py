"""
Process marks with null or empty analysis_id
These marks were missed because they don't have a valid analysis_id
Author: Assistant
"""

import gzip
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import mongo_connection

# =====================
# CONFIG
# =====================
BATCH_SIZE = 10000  # Larger batches since we process sequentially
OUTPUT_DIR = Path("ttl_output_null_marks")
LOG_FILE = "etl_null_marks.log"
GZIP_COMPRESSION_LEVEL = 6

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"  # Or "localhost"
MONGO_PORT = 27017  # Or 27018
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


def create_null_marks_ttl_header(batch_num):
    """Create TTL header for null/empty analysis_id marks"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    ttl = f"""# Marks with null or empty analysis_id
# Generated: {timestamp}
# Batch: {batch_num}

@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix camic: <https://grlc.io/api/MathurMihir/camic-apis/> .

# Collection of marks without analysis_id
<https://grlc.io/api/MathurMihir/camic-apis/null_marks/batch_{batch_num}>
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


def process_null_marks():
    """Process all marks with null or empty analysis_id"""

    logger.info("=" * 60)
    logger.info("NULL/EMPTY ANALYSIS_ID MARKS PROCESSING")
    logger.info("=" * 60)

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

        # Process null marks first
        if null_count > 0:
            logger.info("\nProcessing null analysis_id marks...")
            processed = process_mark_set(db, {"analysis_id": None}, "null")
            logger.info(f"Processed {processed:,} null marks")

        # Process empty marks
        if empty_count > 0:
            logger.info("\nProcessing empty analysis_id marks...")
            processed = process_mark_set(db, {"analysis_id": ""}, "empty")
            logger.info(f"Processed {processed:,} empty marks")

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info(f"Time elapsed: {elapsed/60:.2f} minutes")
        logger.info("=" * 60)


def process_mark_set(db, query, mark_type):
    """Process a set of marks matching the query"""

    marks_cursor = db.mark.find(query, batch_size=BATCH_SIZE * 2)

    batch_num = 1
    batch_marks = 0
    total_processed = 0
    valid_marks = 0

    ttl_content = create_null_marks_ttl_header(batch_num)
    is_first_feature = True

    for mark in marks_cursor:
        try:
            # Try to extract geometry
            geometry = None

            # Try different possible locations for geometry
            if "geometries" in mark and "features" in mark["geometries"]:
                features = mark["geometries"]["features"]
                if features and len(features) > 0:
                    geometry = features[0].get("geometry")
            elif "geometry" in mark:
                geometry = mark["geometry"]

            wkt = polygon_to_wkt(geometry)

            if not wkt:
                total_processed += 1
                continue  # Skip marks without valid geometry

            # Create mark entry
            mark_id = str(mark.get("_id", f"{mark_type}_mark_{valid_marks}"))

            if not is_first_feature:
                ttl_content += " ,\n        "

            # Add mark with its geometry
            ttl_content += f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mark_id}> [
            a geo:Feature ;
            geo:hasGeometry [
                a geo:Geometry ;
                geo:asWKT "{wkt}"^^geo:wktLiteral
            ]"""

            # Add any available properties
            if "properties" in mark and mark["properties"]:
                props = mark["properties"]
                if "classification" in props:
                    ttl_content += f""" ;
            camic:classification "{props['classification']}" """

            ttl_content += "\n        ]"

            is_first_feature = False
            batch_marks += 1
            valid_marks += 1
            total_processed += 1

            # Write batch when full
            if batch_marks >= BATCH_SIZE:
                # Close feature collection
                ttl_content += " .\n"

                # Write file
                output_file = OUTPUT_DIR / mark_type / f"batch_{batch_num:06d}.ttl.gz"
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with gzip.open(
                    output_file,
                    "wt",
                    encoding="utf-8",
                    compresslevel=GZIP_COMPRESSION_LEVEL,
                ) as f:
                    f.write(ttl_content)

                logger.info(
                    f"Wrote batch {batch_num} ({mark_type}): {batch_marks} marks"
                )

                # Start new batch
                batch_num += 1
                batch_marks = 0
                ttl_content = create_null_marks_ttl_header(batch_num)
                is_first_feature = True

        except Exception as e:
            logger.error(f"Error processing mark: {e}")
            total_processed += 1
            continue

    # Write final batch
    if batch_marks > 0:
        ttl_content += " .\n"
        output_file = OUTPUT_DIR / mark_type / f"batch_{batch_num:06d}.ttl.gz"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with gzip.open(
            output_file, "wt", encoding="utf-8", compresslevel=GZIP_COMPRESSION_LEVEL
        ) as f:
            f.write(ttl_content)

        logger.info(f"Wrote final batch {batch_num} ({mark_type}): {batch_marks} marks")

    marks_cursor.close()

    logger.info(f"Total {mark_type} marks processed: {total_processed:,}")
    logger.info(f"Valid {mark_type} marks with geometry: {valid_marks:,}")

    return total_processed


if __name__ == "__main__":
    process_null_marks()
