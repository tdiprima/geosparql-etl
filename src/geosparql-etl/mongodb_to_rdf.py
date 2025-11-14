"""
Parallel MongoDB → GeoSPARQL ETL for 24 cores
Processes multiple analyses simultaneously for massive speedup
Optimized for ~4 billion marks
Author: Bear 🐻
"""

import gc
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

from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

from utils import GEO, PROV, create_graph, mongo_connection

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

# =====================
# 🌐 NAMESPACES
# =====================
HAL = Namespace("https://halcyon.is/ns/")
EXIF = Namespace("http://www.w3.org/2003/12/exif/ns#")
DC = Namespace("http://purl.org/dc/terms/")
SO = Namespace("https://schema.org/")
SNO = Namespace("http://snomed.info/id/")

NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"


# =====================
# 🪵 LOGGER SETUP
# =====================
def setup_worker_logger(worker_id):
    """Setup logger for each worker process"""
    logger = logging.getLogger(f"Worker-{worker_id}")
    logger.setLevel(logging.INFO)

    # File handler for this worker
    handler = RotatingFileHandler(
        f"etl_worker_{worker_id}.log", maxBytes=LOG_MAX_BYTES, backupCount=2
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

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


def create_simple_header(g, analysis_doc, batch_num):
    """Create simplified header"""
    analysis = analysis_doc["analysis"]
    image = analysis_doc["image"]
    params = analysis["algorithm_params"]

    # Get dimensions
    image_width = int(params.get("image_width", 40000))
    image_height = int(params.get("image_height", 40000))

    # Image hash
    image_hash = get_image_hash(image["imageid"])
    case_id = params.get("case_id", image["imageid"])

    # Image object
    image_uri = URIRef(f"urn:sha256:{image_hash}")
    g.add((image_uri, RDF.type, SO.ImageObject))
    g.add((image_uri, DC.identifier, Literal(case_id)))
    g.add((image_uri, EXIF.width, Literal(image_width, datatype=XSD.integer)))
    g.add((image_uri, EXIF.height, Literal(image_height, datatype=XSD.integer)))

    # FeatureCollection with <>
    fc = URIRef("")
    g.add((fc, RDF.type, GEO.FeatureCollection))
    g.add((fc, DC.creator, Literal("http://orcid.org/0000-0003-4165-4062")))
    g.add(
        (
            fc,
            DC.date,
            Literal(datetime.now(tz=timezone.utc).isoformat(), datatype=XSD.dateTime),
        )
    )
    g.add(
        (
            fc,
            DC.description,
            Literal(f"Nuclear segmentation for {case_id} batch {batch_num}"),
        )
    )
    g.add((fc, DC.title, Literal("nuclear-segmentation-predictions")))
    g.add((fc, HAL.executionId, Literal(analysis["execution_id"])))

    # Provenance
    prov_node = URIRef(f"_:prov_{batch_num}")
    g.add((fc, PROV.wasGeneratedBy, prov_node))
    g.add((prov_node, RDF.type, PROV.Activity))
    g.add((prov_node, PROV.used, image_uri))

    return fc, image_width, image_height


def add_mark_simple(g, fc, mark, image_width, image_height, mark_counter):
    """Add mark to graph"""
    try:
        # Get geometry
        geometries = mark.get("geometries", {})
        features = geometries.get("features", [])
        if not features:
            return False

        geometry = features[0].get("geometry", {})
        wkt = polygon_to_wkt(geometry, image_width, image_height)
        if not wkt:
            return False

        # Create feature
        feature_node = URIRef(f"_:f{mark_counter}")
        g.add((fc, RDFS.member, feature_node))
        g.add((feature_node, RDF.type, GEO.Feature))

        # Geometry
        geom_node = URIRef(f"_:g{mark_counter}")
        g.add((feature_node, GEO.hasGeometry, geom_node))
        g.add((geom_node, GEO.asWKT, Literal(wkt, datatype=GEO.wktLiteral)))

        # Classification
        snomed_id = "68841002"
        g.add((feature_node, HAL.classification, SNO[snomed_id]))

        # Measurement
        meas_node = URIRef(f"_:m{mark_counter}")
        g.add((feature_node, HAL.measurement, meas_node))
        g.add((meas_node, HAL.classification, SNO[snomed_id]))
        g.add((meas_node, HAL.hasProbability, Literal(1.0, datatype=XSD.float)))

        return True
    except:
        return False


# =====================
# 👷 WORKER PROCESS FUNCTION
# =====================
def process_analysis_worker(args):
    """Worker function that processes a single analysis"""
    worker_id, analysis_doc, checkpoint_dir = args

    # Setup logger for this worker
    logger = setup_worker_logger(worker_id)

    # Get analysis info
    exec_id = analysis_doc["analysis"]["execution_id"]
    img_id = analysis_doc["image"]["imageid"]
    analysis_id = str(analysis_doc["_id"])

    logger.info(f"Starting {exec_id}:{img_id}")

    try:
        # Create new MongoDB connection for this worker
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

            # Get mark count
            mark_count = db.mark.count_documents(
                {
                    "provenance.analysis.execution_id": exec_id,
                    "provenance.image.imageid": img_id,
                }
            )

            if mark_count == 0:
                logger.warning(f"No marks found for {exec_id}:{img_id}")
                return ("completed", analysis_id, 0, 0)

            logger.info(f"Processing {mark_count:,} marks for {exec_id}:{img_id}")

            # Process marks in batches
            batch_num = 1
            processed = 0
            mark_counter = 0

            # Create first batch
            g = create_graph(
                {
                    "dc": DC,
                    "exif": EXIF,
                    "geo": GEO,
                    "hal": HAL,
                    "prov": PROV,
                    "rdfs": RDFS,
                    "sno": SNO,
                    "so": SO,
                    "xsd": XSD,
                }
            )
            fc, img_width, img_height = create_simple_header(g, analysis_doc, batch_num)
            batch_marks = 0

            # Stream marks
            marks_cursor = db.mark.find(
                {
                    "provenance.analysis.execution_id": exec_id,
                    "provenance.image.imageid": img_id,
                },
                no_cursor_timeout=True,
            ).batch_size(100)

            try:
                for mark in marks_cursor:
                    mark_counter += 1

                    if add_mark_simple(
                        g, fc, mark, img_width, img_height, mark_counter
                    ):
                        processed += 1
                        batch_marks += 1

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        output_file = (
                            OUTPUT_DIR
                            / exec_id
                            / img_id
                            / f"batch_{batch_num:06d}.ttl.gz"
                        )
                        output_file.parent.mkdir(parents=True, exist_ok=True)

                        ttl_content = g.serialize(format="turtle")
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

                        # Clear memory
                        del g
                        gc.collect()

                        # Create new graph
                        g = create_graph(
                            {
                                "dc": DC,
                                "exif": EXIF,
                                "geo": GEO,
                                "hal": HAL,
                                "prov": PROV,
                                "rdfs": RDFS,
                                "sno": SNO,
                                "so": SO,
                                "xsd": XSD,
                            }
                        )
                        fc, img_width, img_height = create_simple_header(
                            g, analysis_doc, batch_num
                        )

                # Write final batch
                if batch_marks > 0:
                    output_file = (
                        OUTPUT_DIR / exec_id / img_id / f"batch_{batch_num:06d}.ttl.gz"
                    )
                    output_file.parent.mkdir(parents=True, exist_ok=True)

                    ttl_content = g.serialize(format="turtle")
                    with gzip.open(
                        output_file,
                        "wt",
                        encoding="utf-8",
                        compresslevel=GZIP_COMPRESSION_LEVEL,
                    ) as f:
                        f.write(ttl_content)

            finally:
                marks_cursor.close()

            logger.info(
                f"✅ Completed {exec_id}:{img_id} - {processed:,} marks in {batch_num} batches"
            )
            return ("completed", analysis_id, processed, batch_num)

    except Exception as e:
        logger.error(f"Failed processing {exec_id}:{img_id}: {e}")
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
                        f"Processing chunk {chunk_start//chunk_size + 1} ({len(chunk_docs)} analyses)"
                    )

                    # Prepare worker arguments
                    worker_args = []
                    for i, doc in enumerate(chunk_docs):
                        worker_id = i % NUM_WORKERS
                        worker_args.append((worker_id, doc, CHECKPOINT_DIR))

                    # Process in parallel
                    results = pool.map(process_analysis_worker, worker_args)

                    # Process results
                    for result in results:
                        if result[0] == "completed":
                            status, analysis_id, mark_count, batch_count = result[:4]
                            checkpoint.mark_completed(analysis_id)
                            total_processed += 1
                            total_marks += mark_count
                        elif result[0] == "failed":
                            status, analysis_id, _, _, error = result
                            checkpoint.mark_failed(analysis_id, error)

                    # Progress report
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
