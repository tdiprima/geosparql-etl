"""
Parallel MongoDB → GeoSPARQL ETL for 24 cores
Processes multiple analyses simultaneously for massive speedup
Optimized for ~4 billion marks
Author: Bear 🐻
"""

import gzip
import hashlib
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from multiprocessing import Pool
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import mongo_connection

# =====================
# 📧 CONFIG - OPTIMIZED FOR PARALLEL PROCESSING
# =====================
NUM_WORKERS = 20  # Use 20 cores for processing, leave 4 for system/MongoDB
BATCH_SIZE = 1000  # Marks per TTL file
OUTPUT_DIR = Path("ttl_output")
CHECKPOINT_DIR = Path("checkpoints")  # Multiple checkpoint files
LOG_FILE = "etl_parallel.log"
LOG_MAX_BYTES = 50 * 1024 * 1024  # 50MB log files
LOG_BACKUP_COUNT = 10
GZIP_COMPRESSION_LEVEL = 6  # 1=fastest, 9=best compression

# MongoDB connection settings
# IMPORTANT: Update these based on where you run the script!
# MONGO_HOST = "localhost"  # Change to "172.18.0.2" if running on server
# MONGO_PORT = 27018  # Change to 27017 if connecting directly
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR.mkdir(exist_ok=True)

# SNOMED code for nuclear material (only hard-coded value as requested)
NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"


# =====================
# 🪵 LOGGER SETUP
# =====================
def setup_worker_logger(worker_id):
    """Setup logger for each worker process.
    Each worker logs to its own rotating file *and* to the console
    so we can see progress in real time.
    """
    logger = logging.getLogger(f"Worker-{worker_id}")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    # File handler for this worker
    file_handler = RotatingFileHandler(
        f"etl_worker_{worker_id}.log",
        maxBytes=LOG_MAX_BYTES,
        backupCount=2,
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler so worker logs also show up in stdout
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_handler)

    return logger


# Main logger
main_logger = logging.getLogger("ETL-Main")
main_logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
main_logger.addHandler(handler)

# Console handler
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(message)s", "%H:%M:%S"))
main_logger.addHandler(console)


# =====================
# 🔍 PARALLEL CHECKPOINT MANAGER
# =====================
class ParallelCheckpointManager:
    """Checkpoint manager that works across multiple processes"""

    def __init__(self, checkpoint_dir):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.completed_file = self.checkpoint_dir / "completed_analyses.txt"
        self.failed_file = self.checkpoint_dir / "failed_analyses.txt"
        self.in_progress_file = self.checkpoint_dir / "in_progress.txt"

        # Load completed and failed sets
        self.completed = self._load_set(self.completed_file)
        self.failed = self._load_set(self.failed_file)

        # Clear in-progress file on start (in case of previous crash)
        if self.in_progress_file.exists():
            self.in_progress_file.unlink()

    def _load_set(self, filepath):
        """Load a set of IDs from a file"""
        ids = set()
        if filepath.exists():
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        ids.add(line)
        return ids

    def is_completed(self, analysis_id):
        """Check if analysis is already completed"""
        return str(analysis_id) in self.completed

    def is_failed(self, analysis_id):
        """Check if analysis previously failed"""
        return str(analysis_id) in self.failed

    def should_process(self, analysis_id):
        """Check if we should process this analysis"""
        aid = str(analysis_id)
        return aid not in self.completed and aid not in self.failed

    def mark_completed(self, analysis_id):
        """Mark analysis as completed (thread-safe append)"""
        with open(self.completed_file, "a") as f:
            f.write(f"{analysis_id}\n")
            f.flush()

    def mark_failed(self, analysis_id, error=None):
        """Mark analysis as failed"""
        with open(self.failed_file, "a") as f:
            f.write(f"{analysis_id}|{error}\n")
            f.flush()

    def mark_in_progress(self, analysis_id, worker_id):
        """Mark as being processed by a worker"""
        with open(self.in_progress_file, "a") as f:
            f.write(f"{analysis_id}|worker_{worker_id}|{datetime.now().isoformat()}\n")
            f.flush()

    def get_stats(self):
        """Get processing statistics"""
        return {"completed": len(self.completed), "failed": len(self.failed)}


# =====================
# 📦 HELPER FUNCTIONS
# =====================


def get_image_hash(image_id):
    """Generate SHA-256 hash for image ID."""
    return hashlib.sha256(image_id.encode()).hexdigest()


def polygon_to_wkt(geometry, image_width, image_height):
    """Convert MongoDB polygon to WKT"""
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


def create_ttl_header(analysis_doc, batch_num):
    """Create TTL header as string (manual building for clean output)"""
    analysis = analysis_doc["analysis"]
    image = analysis_doc["image"]
    params = analysis["algorithm_params"]

    # Get dimensions from algorithm params
    image_width = int(params.get("image_width", 40000))
    image_height = int(params.get("image_height", 40000))

    # Get identifiers
    image_hash = get_image_hash(image["imageid"])
    image_id = image["imageid"]
    subject_id = image.get("subject", "")
    study = image.get("study", "")
    slide = image.get("slide", "")

    # case_id is crucial - get it from params or use imageid as fallback
    case_id = params.get("case_id") or image_id

    # Get analysis details
    execution_id = analysis["execution_id"]
    analysis_type = analysis.get("type", "computer")
    computation = analysis.get("computation", "segmentation")
    submit_date = analysis.get("submit_date", datetime.now(tz=timezone.utc))
    analysis_randval = analysis.get("randval")  # Get randval from analysis

    # Convert submit_date to ISO format if it's not already a string
    if hasattr(submit_date, "isoformat"):
        submit_timestamp = submit_date.isoformat()
    else:
        submit_timestamp = str(submit_date)

    # Create a descriptive title from execution_id and case_id
    title = f"{execution_id} - {case_id} (Batch {batch_num})"

    # Build description
    description = f"Nuclear segmentation results for {case_id}"
    if study:
        description += f" from study {study}"
    if subject_id:
        description += f", subject {subject_id}"
    if slide:
        description += f", slide {slide}"
    description += f" - Batch {batch_num}"

    # TTL header with prefixes
    ttl_content = """@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix exif: <http://www.w3.org/2003/12/exif/ns#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix hal:  <https://halcyon.is/ns/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sno:  <http://snomed.info/id/> .
@prefix so:   <https://schema.org/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

"""

    # Add image object (prov:Entity)
    ttl_content += f"""<urn:sha256:{image_hash}>
        a                so:ImageObject, prov:Entity ;
        dcterms:identifier "{case_id}" ;"""

    if subject_id:
        # Use a domain-specific property for patient/subject ID instead of dcterms:subject
        ttl_content += f"""
        hal:subjectId    "{subject_id}" ;"""

    if study:
        ttl_content += f"""
        dcterms:isPartOf "{study}" ;"""

    ttl_content += f"""
        exif:height      "{image_height}"^^xsd:int ;
        exif:width       "{image_width}"^^xsd:int .

"""

    # Start feature collection with proper Dublin Core and PROV-O terms
    ttl_content += f"""<>      a                    geo:FeatureCollection, prov:Entity ;
        dcterms:identifier   "{str(analysis_doc.get('_id', ''))}" ;
        dcterms:created      "{submit_timestamp}"^^xsd:dateTime ;
        dcterms:type         "{analysis_type}/{computation}" ;
        dcterms:title        "{title}" ;
        dcterms:description  "{description}" ;
        hal:caseId           "{case_id}" ;"""

    # Add execution-specific metadata
    ttl_content += f"""
        hal:executionId      "{execution_id}" ;"""

    if study:
        ttl_content += f"""
        hal:study            "{study}" ;"""

    # Add analysis-level randval if available
    if analysis_randval is not None:
        ttl_content += f"""
        hal:analysisRandval  "{float(analysis_randval):.10f}"^^xsd:float ;"""

    # Add proper PROV-O provenance chain
    ttl_content += f"""
        prov:wasGeneratedBy  <urn:analysis:{execution_id}> ;
        prov:wasDerivedFrom  <urn:sha256:{image_hash}> ;
"""

    # Add the analysis activity details
    ttl_content += f"""
<urn:analysis:{execution_id}>
        a                    prov:Activity ;
        prov:used            <urn:sha256:{image_hash}> ;"""

    # Add ALL algorithm parameters as individual properties
    if params:
        # Add each parameter with appropriate data typing
        for param_key, param_value in params.items():
            # Skip only truly redundant params (image dimensions are in the image object)
            if param_key in ["image_width", "image_height"]:
                continue

            # Create a safe property name
            safe_key = param_key.replace("_", "")

            # Determine the data type and format value appropriately
            if param_value is None or param_value == "":
                continue
            elif isinstance(param_value, bool):
                ttl_content += f"""
        hal:{safe_key}       "{str(param_value).lower()}"^^xsd:boolean ;"""
            elif isinstance(param_value, int):
                ttl_content += f"""
        hal:{safe_key}       "{param_value}"^^xsd:int ;"""
            elif isinstance(param_value, float):
                ttl_content += f"""
        hal:{safe_key}       "{param_value}"^^xsd:float ;"""
            else:
                # String or other type - escape quotes in the value
                escaped_value = str(param_value).replace('"', '\\"')
                ttl_content += f"""
        hal:{safe_key}       "{escaped_value}" ;"""

    # Add start time if available
    if submit_timestamp:
        ttl_content += f"""
        prov:startedAtTime   "{submit_timestamp}"^^xsd:dateTime ;"""

    # Close the analysis activity
    ttl_content += """
        .

<>      """  # Return to the feature collection subject

    return ttl_content, image_width, image_height


def add_mark_to_ttl(mark, image_width, image_height, is_first_feature):
    """Add mark to TTL string (manual building for clean inline blank nodes)"""
    try:
        # Get geometry from mark structure
        geometries = mark.get("geometries", {})
        features = geometries.get("features", [])
        if not features:
            return "", False

        geometry = features[0].get("geometry", {})
        wkt = polygon_to_wkt(geometry, image_width, image_height)
        if not wkt:
            return "", False

        # Get properties - check both in features and in mark.properties
        feature_props = features[0].get("properties", {})
        mark_props = mark.get("properties", {}).get("annotations", {})

        # Build feature TTL

        # Start feature with inline blank node - this mark is a prov:Entity that was generated by the analysis
        feature_ttl = f"""        rdfs:member          [ a                   geo:Feature, prov:Entity ;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}" ] ;
                               hal:classification  <{NUCLEAR_MATERIAL_SNOMED}>"""

        # Add area in pixels if available
        area_pixels = mark_props.get("AreaInPixels") or feature_props.get(
            "AreaInPixels"
        )
        if area_pixels:
            feature_ttl += f""" ;
                               hal:areaInPixels    "{int(area_pixels)}"^^xsd:int"""

        # Add physical size if available
        physical_size = mark_props.get("PhysicalSize") or feature_props.get(
            "PhysicalSize"
        )
        if physical_size:
            feature_ttl += f""" ;
                               hal:physicalSize    "{float(physical_size):.6f}"^^xsd:float"""

        # Add object type if available
        object_type = mark.get("object_type")
        if object_type:
            feature_ttl += f""" ;
                               dcterms:type        "{object_type}" """

        # Add randval if available
        randval = mark.get("randval")
        if randval is not None:
            feature_ttl += f""" ;
                               hal:randval         "{float(randval):.10f}"^^xsd:float"""

        # Add batch_id and tag_id from provenance if available
        provenance = mark.get("provenance", {})
        if provenance:
            batch_id = provenance.get("batch_id")
            if batch_id:
                feature_ttl += f""" ;
                               hal:batchId         "{batch_id}" """

            tag_id = provenance.get("tag_id")
            if tag_id:
                feature_ttl += f""" ;
                               hal:tagId           "{tag_id}" """

        # Add provenance relationship - this mark was generated by the analysis
        provenance = mark.get("provenance", {})
        if provenance and provenance.get("analysis", {}).get("execution_id"):
            exec_id = provenance["analysis"]["execution_id"]
            feature_ttl += f""" ;
                               prov:wasGeneratedBy <urn:analysis:{exec_id}>"""

        # Add measurement (keeping the probability as before)
        feature_ttl += """ ;
                               hal:measurement     [ hal:hasProbability  "1.0"^^xsd:float ]
                             ]"""

        return feature_ttl, True

    except Exception:
        return "", False


# =====================
# 👷 WORKER PROCESS FUNCTION
# =====================
def process_analysis_worker(args):
    """Worker function that processes a single analysis.

    This version:
    - Uses an index-friendly query (slide + execution_id) when possible.
    - Drops the upfront count_documents() and just counts as it streams.
    - Emits detailed logging so we can see where time is going.
    """
    worker_id, analysis_doc, checkpoint_dir = args

    logger = setup_worker_logger(worker_id)

    analysis = analysis_doc.get("analysis", {})
    image = analysis_doc.get("image", {})

    exec_id = analysis.get("execution_id")
    img_id = image.get("imageid")
    slide = image.get("slide")
    analysis_id = str(analysis_doc.get("_id"))

    logger.info("Starting %s:%s (analysis_id=%s)", exec_id, img_id, analysis_id)

    try:
        start_time = time.time()
        logger.info("Connecting to MongoDB for %s:%s", exec_id, img_id)

        # Initialize checkpoint manager
        checkpoint = ParallelCheckpointManager(checkpoint_dir)
        checkpoint.mark_in_progress(analysis_id, worker_id)

        # Connect to MongoDB
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

            # Query for marks using the indexed fields
            query = {
                "provenance.analysis.execution_id": exec_id,
                "provenance.image.imageid": img_id,
            }

            # Add slide filter if available (helps with index selectivity)
            if slide:
                query["provenance.image.slide"] = slide

            logger.info(
                "Streaming marks for %s:%s with query: %s", exec_id, img_id, query
            )

            # Stream marks with cursor
            marks_cursor = db.mark.find(query, batch_size=5000, no_cursor_timeout=False)

            try:
                batch_num = 1
                batch_marks = 0
                processed = 0
                skipped = 0
                is_first_feature = True

                # Start first batch
                ttl_content, img_width, img_height = create_ttl_header(
                    analysis_doc, batch_num
                )

                for mark in marks_cursor:
                    mark_ttl, success = add_mark_to_ttl(
                        mark, img_width, img_height, is_first_feature
                    )
                    if success:
                        # Add semicolon after previous mark if this isn't the first
                        if not is_first_feature:
                            ttl_content += " ;\n"

                        ttl_content += mark_ttl
                        batch_marks += 1
                        processed += 1
                        is_first_feature = False
                    else:
                        skipped += 1

                    # Check if batch is full
                    if batch_marks >= BATCH_SIZE:
                        # Close the last mark and the feature collection (no semicolon on last item)
                        ttl_content += " .\n"

                        # Write batch file
                        output_file = (
                            OUTPUT_DIR
                            / str(exec_id)
                            / str(img_id)
                            / f"batch_{batch_num:06d}.ttl.gz"
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
                            "Wrote batch %d for %s:%s → %s (%s marks, %s total)",
                            batch_num,
                            exec_id,
                            img_id,
                            output_file.name,
                            f"{batch_marks:,}",
                            f"{processed:,}",
                        )

                        batch_num += 1
                        batch_marks = 0

                        # Start new TTL content with new header
                        ttl_content, img_width, img_height = create_ttl_header(
                            analysis_doc, batch_num
                        )
                        is_first_feature = True

                # After loop: flush any remaining marks
                if batch_marks > 0:
                    # Close the last mark and the feature collection (no semicolon on last item)
                    ttl_content += " .\n"

                    output_file = (
                        OUTPUT_DIR
                        / str(exec_id)
                        / str(img_id)
                        / f"batch_{batch_num:06d}.ttl.gz"
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
                        "Wrote FINAL batch %d for %s:%s → %s (%s total processed marks)",
                        batch_num,
                        exec_id,
                        img_id,
                        output_file,
                        f"{processed:,}",
                    )

            finally:
                try:
                    marks_cursor.close()
                except Exception:
                    pass

            elapsed = time.time() - start_time
            logger.info(
                "✅ Completed %s:%s – %s processed marks in %d batches (%.2f seconds)",
                exec_id,
                img_id,
                f"{processed:,}",
                batch_num,
                elapsed,
            )
            return ("completed", analysis_id, processed, batch_num)

    except Exception as e:
        logger.error(
            "Failed processing %s:%s (analysis_id=%s): %s",
            exec_id,
            img_id,
            analysis_id,
            e,
            exc_info=True,
        )
        return ("failed", analysis_id, 0, 0, str(e))


# =====================
# 🚀 MAIN PARALLEL CONTROLLER
# =====================
def main():
    """Main function - parallel processing with 24 cores"""
    main_logger.info("=" * 60)
    main_logger.info(f"PARALLEL ETL - Using {NUM_WORKERS} cores")
    main_logger.info(f"MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    main_logger.info("Output: Compressed TTL files (.ttl.gz)")
    main_logger.info("=" * 60)

    # Initialize checkpoint manager
    checkpoint = ParallelCheckpointManager(CHECKPOINT_DIR)
    initial_stats = checkpoint.get_stats()
    main_logger.info(
        f"Resuming from checkpoint - Already completed: {initial_stats['completed']}, Failed: {initial_stats['failed']}"
    )

    # Get list of analyses to process
    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        total_analyses = db.analysis.count_documents({})
        main_logger.info(f"Found {total_analyses:,} total analyses in database")

        # Get IDs of analyses to process
        analyses_to_process = []
        for doc in db.analysis.find(
            {}, {"_id": 1, "analysis.execution_id": 1, "image.imageid": 1}
        ):
            if checkpoint.should_process(str(doc["_id"])):
                analyses_to_process.append(doc["_id"])

        main_logger.info(f"Need to process {len(analyses_to_process):,} analyses")

        if not analyses_to_process:
            main_logger.info("Nothing to process!")
            return

        # Process in chunks to avoid loading all documents at once
        chunk_size = NUM_WORKERS * 10  # Process 10 rounds of work at a time
        total_processed = 0
        total_marks = 0
        start_time = time.time()

        # Create process pool
        with Pool(processes=NUM_WORKERS) as pool:
            try:
                for chunk_start in range(0, len(analyses_to_process), chunk_size):
                    chunk_ids = analyses_to_process[
                        chunk_start : chunk_start + chunk_size
                    ]

                    # Fetch full documents for this chunk
                    chunk_docs = []
                    for analysis_doc in db.analysis.find({"_id": {"$in": chunk_ids}}):
                        chunk_docs.append(analysis_doc)

                    if not chunk_docs:
                        continue

                    main_logger.info(
                        f"Processing chunk {chunk_start // chunk_size + 1} ({len(chunk_docs)} analyses)"
                    )

                    # Prepare worker arguments
                    worker_args = []
                    for i, doc in enumerate(chunk_docs):
                        worker_id = i % NUM_WORKERS
                        worker_args.append((worker_id, doc, CHECKPOINT_DIR))

                    chunk_index = chunk_start // chunk_size + 1
                    main_logger.info(
                        "Dispatching %d analyses to workers for chunk %d",
                        len(worker_args),
                        chunk_index,
                    )

                    # Process in parallel and stream results as they finish
                    for i, result in enumerate(
                        pool.imap_unordered(
                            process_analysis_worker, worker_args, chunksize=1
                        ),
                        1,
                    ):
                        if not result:
                            continue

                        status = result[0]

                        if status == "completed":
                            _, analysis_id, mark_count, batch_count = result[:4]
                            checkpoint.mark_completed(analysis_id)
                            total_processed += 1
                            total_marks += mark_count

                            main_logger.info(
                                "Chunk %d: completed analysis %s – %s marks in %d batches "
                                "(total processed: %s / %s analyses)",
                                chunk_index,
                                analysis_id,
                                f"{mark_count:,}",
                                batch_count,
                                f"{total_processed:,}",
                                f"{len(analyses_to_process):,}",
                            )

                        elif status == "failed":
                            _, analysis_id, _, _, error = result
                            checkpoint.mark_failed(analysis_id, error)
                            main_logger.error(
                                "Chunk %d: FAILED analysis %s – %s",
                                chunk_index,
                                analysis_id,
                                error,
                            )

                        # Throttled progress report every 50 completed analyses
                        if total_processed and total_processed % 50 == 0:
                            elapsed = time.time() - start_time
                            rate = total_marks / elapsed if elapsed > 0 else 0
                            eta_hours = (
                                (len(analyses_to_process) - total_processed)
                                * (elapsed / total_processed)
                                / 3600
                                if total_processed > 0
                                else 0
                            )

                            main_logger.info(
                                f"""
    Progress Report:
      Processed: {total_processed:,} / {len(analyses_to_process):,} analyses
      Total marks: {total_marks:,}
      Rate: {rate:.0f} marks/sec
      Estimated time remaining: {eta_hours:.1f} hours
    """
                            )

            except KeyboardInterrupt:
                main_logger.warning("⚠️ Interrupted by user - checkpoint saved")
                pool.terminate()
                pool.join()

    # Final statistics
    final_stats = checkpoint.get_stats()
    elapsed_hours = (time.time() - start_time) / 3600

    main_logger.info("=" * 60)
    main_logger.info(
        f"""
ETL Complete!
  Total completed: {final_stats['completed']:,}
  Total failed: {final_stats['failed']:,}
  Total marks processed: {total_marks:,}
  Time elapsed: {elapsed_hours:.2f} hours
  Average rate: {total_marks/(elapsed_hours*3600):.0f} marks/sec
"""
    )
    main_logger.info("=" * 60)


if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, signal.default_int_handler)
    main()
