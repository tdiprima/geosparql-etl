"""
Process "orphaned" marks - CORRECTED for actual schema
❌ execution_id does NOT match analysis._id - so there are no "orphaned" marks in the traditional sense

IMPORTANT: Based on schema analysis, your database structure shows:
- Marks use provenance.analysis.execution_id (NOT analysis_id)
- execution_id does NOT match analysis._id
- Therefore, there are NO "orphaned marks" in the traditional sense

This script is likely NOT NEEDED for your database.

Your execution_ids are probably algorithm/workflow names like:
  - "CNN_synthetic_n_real"
  - "HoVer-Net"
  - "StarDist"

NOT references to analysis documents.

If you still want to find marks with execution_ids that don't match
any analysis document, this script will do that, but it's likely that
ALL your marks will be "orphaned" because execution_id serves a
different purpose in your schema.
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
NUM_WORKERS = 24
BATCH_SIZE = 1000
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


def check_if_orphaned_concept_applies():
    """
    Check if the concept of "orphaned marks" even applies to this database
    """

    logger.info("=" * 60)
    logger.info("CHECKING IF 'ORPHANED MARKS' CONCEPT APPLIES")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # Get sample execution_ids
        logger.info("Sampling execution_ids from marks...")
        sample_marks = list(
            db.mark.find(
                {"provenance.analysis.execution_id": {"$exists": True}},
                {"provenance.analysis.execution_id": 1},
            ).limit(10)
        )

        if not sample_marks:
            logger.info("✗ No marks with execution_id found")
            return False

        exec_ids = [m["provenance"]["analysis"]["execution_id"] for m in sample_marks]
        unique_exec_ids = list(set(exec_ids))

        logger.info("\nSample execution_ids found in marks:")
        for eid in unique_exec_ids[:5]:
            logger.info(f"  - {eid}")

        # Check if these match analysis._id
        logger.info("\nChecking if execution_ids match analysis._id...")
        matches = 0

        for eid in unique_exec_ids:
            exists = db.analysis.find_one({"_id": eid}) is not None
            if exists:
                matches += 1
                logger.info(f"  ✓ {eid} matches an analysis document")
            else:
                logger.info(f"  ✗ {eid} does NOT match any analysis document")

        logger.info(
            f"\nResult: {matches}/{len(unique_exec_ids)} execution_ids match analysis documents"
        )

        if matches == 0:
            logger.info(
                """
⚠️  IMPORTANT FINDING:
    
    NONE of your execution_ids match analysis._id values!
    
    This means execution_id serves a different purpose - it's likely:
    - Algorithm name (e.g., "CNN_synthetic_n_real")
    - Workflow identifier
    - Model version
    
    NOT a reference to analysis documents!
    
    Therefore, the concept of "orphaned marks" doesn't apply to your database.
    
    Your marks are organized by:
    - execution_id (algorithm/workflow name) - INDEXED
    - source (human/computer) - INDEXED  
    - slide (image reference) - INDEXED
    
    NOT by analysis document references.
            """
            )
            return False

        elif matches == len(unique_exec_ids):
            logger.info(
                """
✓ All execution_ids match analysis documents
  This script MAY be useful if you have marks with execution_ids
  that don't have corresponding analysis documents.
            """
            )
            return True

        else:
            logger.info(
                f"""
⚠️  Mixed results: {matches}/{len(unique_exec_ids)} match
   Some execution_ids are references, others are not.
   Proceed with caution.
            """
            )
            return True


def get_orphaned_execution_ids():
    """
    Find execution_ids that exist in marks but not in analysis collection
    """
    orphaned_ids = []

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        logger.info("Finding execution_ids that don't match analysis documents...")
        logger.info("Getting unique execution_ids (this uses index - fast)...")

        # Uses index - FAST!
        unique_execution_ids = db.mark.distinct("provenance.analysis.execution_id")
        logger.info(f"Found {len(unique_execution_ids):,} unique execution_ids")

        # Check in batches
        chunk_size = 1000
        for i in range(0, len(unique_execution_ids), chunk_size):
            chunk = unique_execution_ids[i : i + chunk_size]

            # Batch check existence (uses _id index - fast!)
            existing_ids = set(
                doc["_id"]
                for doc in db.analysis.find(
                    {"_id": {"$in": [eid for eid in chunk if eid]}}, {"_id": 1}
                )
            )

            for eid in chunk:
                if eid and eid not in existing_ids:
                    orphaned_ids.append(eid)

            if (i + chunk_size) % 10000 == 0:
                logger.info(
                    f"Checked {i+chunk_size:,} IDs, found {len(orphaned_ids):,} orphaned"
                )

        logger.info(
            f"Found {len(orphaned_ids):,} execution_ids without analysis documents"
        )

    return orphaned_ids


def create_minimal_ttl_header(execution_id, batch_num):
    """Create minimal TTL header"""
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    ttl = f"""# Marks with execution_id: {execution_id}
# Generated: {timestamp}
# Batch: {batch_num}

@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix camic: <https://grlc.io/api/MathurMihir/camic-apis/> .

<https://grlc.io/api/MathurMihir/camic-apis/execution_id/{execution_id}/batch_{batch_num}>
    a geo:FeatureCollection ;
    prov:generatedAtTime "{timestamp}"^^xsd:dateTime ;
    geo:hasFeature """

    return ttl


def polygon_to_wkt(geometry, default_width=40000, default_height=40000):
    """Convert MongoDB polygon to WKT"""
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


def process_execution_id(execution_id):
    """Process all marks for an execution_id"""

    try:
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            # Count marks - uses index, FAST!
            query = {"provenance.analysis.execution_id": execution_id}

            # Get marks cursor
            marks_cursor = db.mark.find(query, batch_size=BATCH_SIZE * 2)

            batch_num = 1
            batch_marks = []
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

                    mark_id = str(mark.get("_id"))
                    batch_marks.append(mark_id)
                    processed += 1

                    # Write batch when full
                    if len(batch_marks) >= BATCH_SIZE:
                        ttl_content = create_minimal_ttl_header(execution_id, batch_num)

                        for i, mid in enumerate(batch_marks):
                            if i > 0:
                                ttl_content += " ,\n        "
                            ttl_content += f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mid}>"""

                        ttl_content += " .\n"

                        output_file = (
                            OUTPUT_DIR / execution_id / f"batch_{batch_num:06d}.ttl.gz"
                        )
                        output_file.parent.mkdir(parents=True, exist_ok=True)

                        with gzip.open(
                            output_file,
                            "wt",
                            encoding="utf-8",
                            compresslevel=GZIP_COMPRESSION_LEVEL,
                        ) as f:
                            f.write(ttl_content)

                        batch_num += 1
                        batch_marks = []

                except Exception:
                    continue

            # Write final batch
            if batch_marks:
                ttl_content = create_minimal_ttl_header(execution_id, batch_num)

                for i, mid in enumerate(batch_marks):
                    if i > 0:
                        ttl_content += " ,\n        "
                    ttl_content += (
                        f"""<https://grlc.io/api/MathurMihir/camic-apis/mark/{mid}>"""
                    )

                ttl_content += " .\n"

                output_file = (
                    OUTPUT_DIR / execution_id / f"batch_{batch_num:06d}.ttl.gz"
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
            return ("completed", execution_id, processed)

    except Exception as e:
        logger.error(f"Failed processing {execution_id}: {e}")
        return ("failed", execution_id, 0, str(e))


def main():
    """Main function"""

    logger.info("=" * 60)
    logger.info("'ORPHANED' MARKS PROCESSOR (CORRECTED)")
    logger.info("=" * 60)

    # First check if this concept even applies
    if not check_if_orphaned_concept_applies():
        logger.info("\n" + "=" * 60)
        logger.info("✅ Script not needed for your database schema")
        logger.info("=" * 60)
        return

    # Get orphaned execution IDs
    orphaned_ids = get_orphaned_execution_ids()

    if not orphaned_ids:
        logger.info("No orphaned execution_ids found!")
        return

    # Load checkpoint
    checkpoint_file = CHECKPOINT_DIR / "completed_orphaned.txt"
    completed = set()

    if checkpoint_file.exists():
        with open(checkpoint_file, "r") as f:
            for line in f:
                completed.add(line.strip())

    to_process = [eid for eid in orphaned_ids if eid not in completed]
    logger.info(f"Need to process {len(to_process):,} execution_ids")

    if not to_process:
        logger.info("All already processed!")
        return

    # Process
    total_marks = 0
    start_time = time.time()

    with Pool(processes=NUM_WORKERS) as pool:
        for i, result in enumerate(
            pool.imap_unordered(process_execution_id, to_process), 1
        ):
            if result[0] == "completed":
                execution_id = result[1]
                mark_count = result[2]
                total_marks += mark_count

                with open(checkpoint_file, "a") as f:
                    f.write(f"{execution_id}\n")

                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Progress: {i}/{len(to_process)} - Marks: {total_marks:,}"
                    )

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(
        f"Complete! Processed {total_marks:,} marks in {elapsed/60:.1f} minutes"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
