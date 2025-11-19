"""
Parallel MongoDB → GeoSPARQL ETL for 24 cores
Processes multiple analyses simultaneously for massive speedup
Optimized for ~4 billion marks
Author: Bear 🐻
"""

import gzip
import hashlib
import logging
import os
import signal
import sys
import threading
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

# Thread-safe file lock for checkpoint operations
checkpoint_lock = threading.Lock()


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
# 🔍 PARALLEL CHECKPOINT MANAGER (WITH FIXES)
# =====================
class ParallelCheckpointManager:
    """Checkpoint manager that works across multiple processes with thread-safe operations"""

    def __init__(self, checkpoint_dir):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.completed_file = self.checkpoint_dir / "completed_analyses.txt"
        self.failed_file = self.checkpoint_dir / "failed_analyses.txt"
        self.in_progress_file = self.checkpoint_dir / "in_progress.txt"

        # Load completed and failed sets
        self.completed = self._load_set(self.completed_file)
        self.failed = self._load_set(self.failed_file)

        # Ensure in_progress file exists before any worker tries to use it
        self.in_progress_file.touch(exist_ok=True)

    def _load_set(self, filepath):
        """Load a set of IDs from a file"""
        ids = set()
        if filepath.exists():
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        ids.add(line.split("|")[0])  # Handle both formats
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
        """Mark analysis as completed (thread-safe append with fsync)"""
        with checkpoint_lock:
            # Ensure directory exists
            self.checkpoint_dir.mkdir(exist_ok=True)

            with open(self.completed_file, "a") as f:
                f.write(f"{analysis_id}\n")
                f.flush()
                os.fsync(f.fileno())  # Force write to disk

    def mark_failed(self, analysis_id, error=None):
        """Mark analysis as failed (thread-safe)"""
        with checkpoint_lock:
            # Ensure directory exists
            self.checkpoint_dir.mkdir(exist_ok=True)

            with open(self.failed_file, "a") as f:
                f.write(f"{analysis_id}|{error}\n")
                f.flush()
                os.fsync(f.fileno())

    def mark_in_progress(self, analysis_id, worker_id):
        """Mark as being processed by a worker (thread-safe with file existence check)"""
        with checkpoint_lock:
            # Ensure file exists
            if not self.in_progress_file.exists():
                self.in_progress_file.touch()

            with open(self.in_progress_file, "a") as f:
                f.write(
                    f"{analysis_id}|worker_{worker_id}|{datetime.now().isoformat()}\n"
                )
                f.flush()
                os.fsync(f.fileno())  # Force write to disk

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
    exec_id = analysis["execution_id"]
    analysis_id = str(analysis_doc["_id"])

    # Build TTL string manually
    ttl_lines = [
        "# GeoSPARQL representation of pathology image analysis",
        f"# Analysis ID: {analysis_id}",
        f"# Execution: {exec_id}",
        f"# Image: {image_id}",
        f"# Batch: {batch_num:06d}",
        "",
        "@prefix geo: <http://www.opengis.net/ont/geosparql#> .",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "@prefix snomed: <http://snomed.info/id/> .",
        "@prefix loinc: <http://loinc.org/rdf/> .",
        "@prefix camic: <http://example.org/camic#> .",
        "",
    ]

    # Image description
    ttl_lines.extend(
        [
            f"camic:image_{image_hash}",
            "    a camic:PathologyImage ;",
            f'    camic:imageId "{image_id}" ;',
        ]
    )

    if case_id:
        ttl_lines.append(f'    camic:caseId "{case_id}" ;')
    if subject_id:
        ttl_lines.append(f'    camic:subjectId "{subject_id}" ;')
    if study:
        ttl_lines.append(f'    camic:studyId "{study}" ;')
    if slide:
        ttl_lines.append(f'    camic:slideId "{slide}" ;')

    ttl_lines.extend(
        [
            f"    camic:imageWidth {image_width} ;",
            f"    camic:imageHeight {image_height} ;",
            f'    camic:analysisId "{analysis_id}" ;',
            "    geo:hasGeometry [",
            f'        geo:asWKT "POLYGON ((0 0, {image_width} 0, {image_width} {image_height}, 0 {image_height}, 0 0))"^^geo:wktLiteral',
            "    ] ;",
            "    camic:hasFeatureCollection [",
            "        a geo:FeatureCollection ;",
            "        geo:hasMember",
        ]
    )

    return "\n".join(ttl_lines), image_width, image_height


def add_mark_to_ttl(mark, image_width, image_height, is_first_feature):
    """
    Convert a mark document to TTL string format.
    Returns (ttl_string, success_bool)
    """
    try:
        mark_id = str(mark["_id"])
        provenance = mark.get("provenance", {})
        analysis = provenance.get("analysis", {})
        exec_id = analysis.get("execution_id", "unknown")

        # Get geometry and coordinates
        geom = mark.get("geometries", {})
        features = geom.get("features", [])
        if not features:
            return "", False

        feature = features[0]
        geometry = feature.get("geometry", {})
        properties = feature.get("properties", {})

        # Get properties
        footprint = properties.get("footprint", 0)
        nucleustype = properties.get("nucleustype", "")

        # Get annotations if any
        annotations = []
        user_update = mark.get("userUpdate", {})
        if "mark" in user_update:
            annotations = user_update["mark"].get("annotation", [])

        # Check if it's nuclear material
        is_nuclear_material = False
        if nucleustype:
            nucleus_parts = nucleustype.split(".")
            if len(nucleus_parts) >= 3:
                # Example: "tumor.ep.1" → tumor cells
                cell_type = nucleus_parts[0]  # tumor, lymphocyte, etc.
                is_nuclear_material = True

        # Only add human annotation if one exists AND is valid SNOMED
        has_valid_annotation = False
        annotation_code = None
        if annotations:
            # Get the first annotation
            first_annotation = annotations[0]
            ann_id = first_annotation.get("annotationID")
            if ann_id and ann_id.startswith("http://snomed.info/id/"):
                has_valid_annotation = True
                annotation_code = ann_id

        # Convert geometry
        wkt = polygon_to_wkt(geometry, image_width, image_height)
        if not wkt:
            return "", False

        # Build mark TTL
        mark_lines = [
            "            [",
            "                a geo:Feature ;",
            f'                camic:markId "{mark_id}" ;',
        ]

        # Add execution ID
        mark_lines.append(f'                camic:executionId "{exec_id}" ;')

        # Add cell type
        if nucleustype:
            mark_lines.append(f'                camic:nucleusType "{nucleustype}" ;')

        # Add SNOMED code for nuclear material (automatic for all nucleus marks)
        if is_nuclear_material:
            mark_lines.append(
                f"                camic:hasMaterialType snomed:68841002 ;  # Nuclear material"
            )

        # Only add human annotation if it exists and is valid
        if has_valid_annotation and annotation_code:
            mark_lines.append(
                f"                camic:hasAnnotation <{annotation_code}> ;  # Human-verified SNOMED code"
            )

        # Add numeric properties
        mark_lines.append(f"                camic:footprint {footprint} ;")

        # Add geometry (no trailing semicolon - this is the last property)
        mark_lines.extend(
            [
                "                geo:hasGeometry [",
                f'                    geo:asWKT "{wkt}"^^geo:wktLiteral',
                "                ]",
                "            ]",
            ]
        )

        return "\n".join(mark_lines), True

    except Exception as e:
        # Silently skip malformed marks
        return "", False


# =====================
# 👷 WORKER PROCESS FUNCTION
# =====================
def process_analysis_worker(args):
    """
    Worker function - processes one analysis document.
    Each worker gets its own MongoDB connection.
    """
    worker_id, analysis_doc, checkpoint_dir = args

    logger = setup_worker_logger(worker_id)

    # Get IDs for logging
    analysis = analysis_doc.get("analysis", {})
    image = analysis_doc.get("image", {})

    exec_id = analysis.get("execution_id")
    img_id = image.get("imageid")
    slide = image.get("slide")
    analysis_id = str(analysis_doc.get("_id"))

    logger.info("Starting %s:%s (analysis_id=%s)", exec_id, img_id, analysis_id)

    try:
        start_time = time.time()

        # Initialize checkpoint manager
        checkpoint = ParallelCheckpointManager(checkpoint_dir)

        # Try to mark in progress - if this fails, continue anyway
        try:
            checkpoint.mark_in_progress(analysis_id, worker_id)
        except Exception as e:
            logger.warning(f"Could not mark {analysis_id} in progress: {e}")
            # Continue anyway - the important part is processing the data

        # Create MongoDB connection (each worker gets its own)
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

            # Query for marks that belong to this analysis
            query = {
                "provenance.analysis.execution_id": exec_id,
                "provenance.image.imageid": img_id,
            }

            # Add slide filter if available (helps with index selectivity)
            if slide:
                query["provenance.image.slide"] = slide

            logger.info("Streaming marks for %s:%s", exec_id, img_id)

            # Stream marks from MongoDB
            marks_cursor = db.mark.find(query, batch_size=5000, no_cursor_timeout=False)

            try:
                batch_num = 1
                batch_marks = 0
                processed = 0
                is_first_feature = True

                # Start first batch
                ttl_content, img_width, img_height = create_ttl_header(
                    analysis_doc, batch_num
                )

                for mark in marks_cursor:
                    # Convert mark to TTL
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

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        # Close the last mark and the feature collection (no semicolon on last item)
                        ttl_content += " .\n"

                        # Write compressed TTL file
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
                            "Wrote batch %d for %s:%s (%s marks)",
                            batch_num,
                            exec_id,
                            img_id,
                            batch_marks,
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

            # Try to mark as completed
            try:
                checkpoint.mark_completed(analysis_id)
            except Exception as e:
                logger.warning(f"Could not mark {analysis_id} as completed: {e}")

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

        # Try to mark as failed
        try:
            checkpoint = ParallelCheckpointManager(checkpoint_dir)
            checkpoint.mark_failed(analysis_id, str(e))
        except:
            pass

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
                        worker_args.append((worker_id, doc, str(CHECKPOINT_DIR)))

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
