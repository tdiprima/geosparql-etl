"""
Massive Dataset MongoDB → GeoSPARQL ETL
Optimized for ~4 billion marks with minimal memory footprint
Author: Bear 🐻
"""

import gc  # Garbage collection
import gzip  # For compressing TTL files
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from tqdm import tqdm

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, RDFS, XSD
from utils import GEO, PROV, create_graph, mongo_connection

# =====================
# 📧 CONFIG - OPTIMIZED FOR MASSIVE SCALE
# =====================
# SMALLER BATCHES = LESS MEMORY
BATCH_SIZE = 1000  # Smaller batches to minimize memory usage
OUTPUT_DIR = Path("ttl_output")
CHECKPOINT_FILE = Path("current_checkpoint.txt")  # Simple text file
LOG_FILE = "etl_run.log"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB log files
LOG_BACKUP_COUNT = 5  # Keep only 5 logs to save space
GZIP_COMPRESSION_LEVEL = 6  # 1=fastest, 9=best compression, 6=good balance

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

# Note: Output files will be gzipped (.ttl.gz) to save ~70% disk space
# For 4B marks: ~750GB-1TB instead of 2.5-3TB

# =====================
# 🌐 NAMESPACES
# =====================
EX = Namespace("https://halcyon.is/ns/")
HAL = Namespace("https://halcyon.is/ns/")
EXIF = Namespace("http://www.w3.org/2003/12/exif/ns#")
DC = Namespace("http://purl.org/dc/terms/")
SO = Namespace("https://schema.org/")
SNO = Namespace("http://snomed.info/id/")

NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"

# =====================
# 🪵 SIMPLE ROTATING LOGGER
# =====================
logger = logging.getLogger("ETL")
logger.setLevel(logging.INFO)

# Rotating file handler
handler = RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Console handler with simpler format
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(message)s", "%H:%M:%S"))
logger.addHandler(console)


# =====================
# 🔍 ULTRA-SIMPLE CHECKPOINT
# =====================
class SimpleCheckpoint:
    """
    Dead simple checkpoint that just tracks the last processed analysis ID.
    For 4B marks, we can't keep them all in memory!
    """

    def __init__(self, checkpoint_file):
        self.checkpoint_file = Path(checkpoint_file)
        self.last_processed_id = None
        self.processed_count = 0
        self.failed_count = 0
        self.load()

    def load(self):
        """Load last checkpoint - just the last ID we processed"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    lines = f.readlines()
                    if lines:
                        # Format: last_id|processed_count|failed_count
                        parts = lines[0].strip().split("|")
                        self.last_processed_id = (
                            parts[0] if parts[0] != "None" else None
                        )
                        self.processed_count = int(parts[1]) if len(parts) > 1 else 0
                        self.failed_count = int(parts[2]) if len(parts) > 2 else 0
                        logger.info(
                            f"Resuming from checkpoint: {self.last_processed_id}, "
                            f"Already processed: {self.processed_count}, Failed: {self.failed_count}"
                        )
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")

    def save(self, analysis_id):
        """Save checkpoint with just the last processed ID"""
        self.last_processed_id = str(analysis_id)
        self.checkpoint_file.write_text(
            f"{self.last_processed_id}|{self.processed_count}|{self.failed_count}\n"
        )

    def mark_processed(self, analysis_id):
        """Mark as processed and save immediately"""
        self.processed_count += 1
        self.save(analysis_id)

        # Log progress every 10 analyses
        if self.processed_count % 10 == 0:
            logger.info(
                f"Progress: {self.processed_count} analyses processed, {self.failed_count} failed"
            )

    def mark_failed(self, analysis_id, error=None):
        """Mark as failed"""
        self.failed_count += 1
        logger.error(f"Failed processing {analysis_id}: {error}")
        # Still save checkpoint so we can continue
        self.save(analysis_id)

    def should_skip(self, analysis_id):
        """Check if we should skip this analysis (already processed)"""
        if self.last_processed_id is None:
            return False

        # This assumes MongoDB _id ordering
        # If we've seen this ID or newer, skip it
        return str(analysis_id) <= self.last_processed_id


# =====================
# 📦 HELPER FUNCTIONS (SIMPLIFIED)
# =====================


def get_image_hash(image_id):
    """Generate SHA-256 hash for image ID."""
    return hashlib.sha256(image_id.encode()).hexdigest()


def polygon_to_wkt(geometry, image_width, image_height):
    """Convert MongoDB polygon to WKT - simplified version"""
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
    """Simplified header - just essentials"""
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
    """Simplified mark addition - just essentials"""
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


def process_one_analysis(db, analysis_doc, checkpoint):
    """Process a single analysis with minimal memory usage"""
    exec_id = analysis_doc["analysis"]["execution_id"]
    img_id = analysis_doc["image"]["imageid"]
    analysis_id = analysis_doc["_id"]

    logger.info(f"Processing {exec_id}:{img_id}")

    # Get marks count first
    mark_count = db.mark.count_documents(
        {
            "provenance.analysis.execution_id": exec_id,
            "provenance.image.imageid": img_id,
        }
    )

    if mark_count == 0:
        logger.warning(f"No marks found for {exec_id}:{img_id}")
        checkpoint.mark_processed(analysis_id)
        return

    logger.info(f"Found {mark_count:,} marks to process")

    # Process in small batches
    batch_num = 1
    processed = 0
    mark_counter = 0

    try:
        # Start first batch
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

        # Stream marks with cursor
        marks_cursor = db.mark.find(
            {
                "provenance.analysis.execution_id": exec_id,
                "provenance.image.imageid": img_id,
            },
            no_cursor_timeout=True,
        ).batch_size(
            100
        )  # Small cursor batch size

        try:
            with tqdm(total=mark_count, desc=str(exec_id), unit=" marks") as pbar:
                for mark in marks_cursor:
                    mark_counter += 1

                    if add_mark_simple(
                        g, fc, mark, img_width, img_height, mark_counter
                    ):
                        processed += 1
                        batch_marks += 1
                        pbar.update(1)

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        # Write compressed file
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

                        logger.debug(
                            f"Wrote batch {batch_num} ({batch_marks} marks) - compressed"
                        )

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

                    logger.debug(
                        f"Wrote final batch {batch_num} ({batch_marks} marks) - compressed"
                    )

        finally:
            marks_cursor.close()

        logger.info(
            f"✅ Completed {exec_id}:{img_id} - {processed:,} marks in {batch_num} batches"
        )
        checkpoint.mark_processed(analysis_id)

        # Force garbage collection
        gc.collect()

    except Exception as e:
        logger.error(f"Failed processing {exec_id}:{img_id}: {e}")
        checkpoint.mark_failed(analysis_id, error=str(e))
        raise


def main():
    """Main function - optimized for 4 billion marks"""
    logger.info("=" * 60)
    logger.info("MASSIVE DATASET ETL - Optimized for ~4 billion marks")
    logger.info("Output: Compressed TTL files (.ttl.gz)")
    logger.info("Estimated disk usage: ~750GB-1TB (vs 2.5-3TB uncompressed)")
    logger.info("=" * 60)

    checkpoint = SimpleCheckpoint(CHECKPOINT_FILE)

    with mongo_connection("mongodb://localhost:27018/", "camic") as db:
        # Get total count
        total_analyses = db.analysis.count_documents({})
        logger.info(f"Found {total_analyses:,} analyses to process")

        # Estimate total marks (for user info)
        sample_size = min(10, total_analyses)
        if sample_size > 0:
            sample_marks = []
            for doc in db.analysis.find().limit(sample_size):
                count = db.mark.count_documents(
                    {
                        "provenance.analysis.execution_id": doc["analysis"][
                            "execution_id"
                        ],
                        "provenance.image.imageid": doc["image"]["imageid"],
                    }
                )
                sample_marks.append(count)

            if sample_marks:
                avg_marks = sum(sample_marks) / len(sample_marks)
                estimated_total = int(avg_marks * total_analyses)
                logger.info(f"Estimated total marks: ~{estimated_total:,}")

        # Process analyses one at a time
        processed = 0
        skipped = 0

        # Use cursor to avoid loading all analyses into memory
        analysis_cursor = db.analysis.find().sort(
            "_id", 1
        )  # Sort by ID for consistent ordering

        try:
            for analysis_doc in analysis_cursor:
                analysis_id = analysis_doc["_id"]

                # Skip if already processed
                if checkpoint.should_skip(analysis_id):
                    skipped += 1
                    if skipped % 100 == 0:
                        logger.info(f"Skipped {skipped} already-processed analyses...")
                    continue

                # Process this analysis
                try:
                    process_one_analysis(db, analysis_doc, checkpoint)
                    processed += 1

                    # Memory cleanup every 10 analyses
                    if processed % 10 == 0:
                        gc.collect()

                except KeyboardInterrupt:
                    logger.warning("⚠️ Interrupted by user - checkpoint saved")
                    break
                except Exception as e:
                    logger.error(f"Error processing analysis: {e}")
                    continue

        finally:
            analysis_cursor.close()

    logger.info("=" * 60)
    logger.info(f"ETL Complete! Processed {checkpoint.processed_count:,} analyses")
    logger.info(f"Failed: {checkpoint.failed_count:,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
