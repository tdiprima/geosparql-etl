"""
Resumable MongoDB → GeoSPARQL ETL with Checkpoint and Log Rotation
Merges functionality from CSV ETL (nuclear_segmentation_etl.py) and MongoDB ETL (resumable.py)
Enhanced with rotation capabilities for handling massive datasets
Author: Bear 🐻
"""

import gzip
import hashlib
import json
import logging
import os
import shutil
import sys
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from tqdm import tqdm

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, RDFS, XSD
from utils import (GEO, PROV, ETLConfig, create_graph, generate_batch_filename,
                   mongo_connection)


# =====================
# 📧 CONFIG
# =====================
class RotatingETLConfig(ETLConfig):
    """Extended config with rotation settings"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checkpoint_max_size = kwargs.get(
            "checkpoint_max_size", 10 * 1024 * 1024
        )  # 10MB default
        self.checkpoint_max_entries = kwargs.get(
            "checkpoint_max_entries", 50000
        )  # 50k entries default
        self.checkpoint_archive_dir = kwargs.get(
            "checkpoint_archive_dir", "checkpoint_archives"
        )
        self.log_max_bytes = kwargs.get(
            "log_max_bytes", 50 * 1024 * 1024
        )  # 50MB default
        self.log_backup_count = kwargs.get(
            "log_backup_count", 10
        )  # Keep 10 backup logs
        self.checkpoint_compression = kwargs.get(
            "checkpoint_compression", True
        )  # Compress archived checkpoints


CONFIG = RotatingETLConfig(
    batch_size=5000,
    output_dir="ttl_output",
    checkpoint_file="etl_checkpoint.json",
    log_file="etl_run.log",
    checkpoint_max_size=10 * 1024 * 1024,  # 10MB
    checkpoint_max_entries=50000,  # 50k entries before rotation
    checkpoint_archive_dir="checkpoint_archives",
    log_max_bytes=50 * 1024 * 1024,  # 50MB
    log_backup_count=10,
    checkpoint_compression=True,
)

# =====================
# 🌐 NAMESPACES
# =====================
# Custom namespaces
EX = Namespace("https://halcyon.is/ns/")
HAL = Namespace("https://halcyon.is/ns/")
EXIF = Namespace("http://www.w3.org/2003/12/exif/ns#")
DC = Namespace("http://purl.org/dc/terms/")
SO = Namespace("https://schema.org/")
SNO = Namespace("http://snomed.info/id/")

# SNOMED URI for nuclear material (nucleoplasm)
NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"  # Nucleoplasm


# =====================
# 🪵 ROTATING LOGGER
# =====================
def setup_rotating_logger(log_file, max_bytes, backup_count):
    """Setup logger with rotation capability"""
    logger = logging.getLogger("ETL")
    logger.setLevel(logging.INFO)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create rotating file handler
    handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Also add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_rotating_logger(
    CONFIG.log_file, CONFIG.log_max_bytes, CONFIG.log_backup_count
)


# =====================
# 🔄 ROTATING CHECKPOINT MANAGER
# =====================
class RotatingCheckpointManager:
    """Checkpoint manager with rotation and archival capabilities"""

    def __init__(
        self, checkpoint_file, max_size, max_entries, archive_dir, compress=True
    ):
        self.checkpoint_file = Path(checkpoint_file)
        self.max_size = max_size
        self.max_entries = max_entries
        self.archive_dir = Path(archive_dir)
        self.compress = compress
        self.archive_dir.mkdir(exist_ok=True)

        # Current working set (most recent entries kept in memory)
        self.working_set = {}
        self.failed = {}

        # Metadata about all archives
        self.archive_metadata_file = self.archive_dir / "archive_metadata.json"
        self.archive_metadata = self._load_archive_metadata()

        # Load current checkpoint
        self._load_checkpoint()

        # Track entry count for rotation
        self.entry_count = len(self.working_set)

    def _load_archive_metadata(self):
        """Load metadata about all archived checkpoints"""
        if self.archive_metadata_file.exists():
            with open(self.archive_metadata_file, "r") as f:
                return json.load(f)
        return {"archives": [], "total_processed": 0, "total_failed": 0}

    def _save_archive_metadata(self):
        """Save metadata about all archived checkpoints"""
        with open(self.archive_metadata_file, "w") as f:
            json.dump(self.archive_metadata, f, indent=2)

    def _load_checkpoint(self):
        """Load the current checkpoint file"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    self.working_set = data.get("processed", {})
                    self.failed = data.get("failed", {})
                    logger.info(
                        f"Loaded checkpoint with {len(self.working_set)} processed, {len(self.failed)} failed"
                    )
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
                self.working_set = {}
                self.failed = {}

    def _should_rotate(self):
        """Check if checkpoint should be rotated"""
        # Check size
        if self.checkpoint_file.exists():
            size = self.checkpoint_file.stat().st_size
            if size > self.max_size:
                return True, f"size ({size} > {self.max_size})"

        # Check entry count
        if self.entry_count > self.max_entries:
            return True, f"entries ({self.entry_count} > {self.max_entries})"

        return False, None

    def _rotate_checkpoint(self):
        """Rotate current checkpoint to archive"""
        if not self.checkpoint_file.exists():
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"checkpoint_{timestamp}"

        # Create archive entry
        archive_info = {
            "timestamp": timestamp,
            "processed_count": len(self.working_set),
            "failed_count": len(self.failed),
            "filename": archive_name,
        }

        if self.compress:
            # Compress and archive
            archive_path = self.archive_dir / f"{archive_name}.json.gz"
            with open(self.checkpoint_file, "rb") as f_in:
                with gzip.open(archive_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            archive_info["filename"] = f"{archive_name}.json.gz"
            archive_info["compressed"] = True
        else:
            # Just move to archive
            archive_path = self.archive_dir / f"{archive_name}.json"
            shutil.move(str(self.checkpoint_file), str(archive_path))
            archive_info["compressed"] = False

        # Update archive metadata
        self.archive_metadata["archives"].append(archive_info)
        self.archive_metadata["total_processed"] += len(self.working_set)
        self.archive_metadata["total_failed"] += len(self.failed)
        self._save_archive_metadata()

        logger.info(
            f"Rotated checkpoint to {archive_path} ({len(self.working_set)} entries)"
        )

        # Clear working set but keep a small overlap for deduplication
        # Keep last 1000 entries to prevent reprocessing in case of overlap
        if len(self.working_set) > 1000:
            recent_keys = list(self.working_set.keys())[-1000:]
            self.working_set = {k: self.working_set[k] for k in recent_keys}
        else:
            self.working_set = {}

        self.failed = {}  # Clear failed entries after rotation
        self.entry_count = len(self.working_set)

    def is_processed(self, exec_id, img_id):
        """Check if an analysis is already processed"""
        key = f"{exec_id}:{img_id}"

        # Check working set first
        if key in self.working_set:
            return True

        # Check all archives (this could be optimized with a bloom filter for huge datasets)
        return self._check_archives(key)

    def _check_archives(self, key):
        """Check if key exists in any archive"""
        # For huge datasets, consider maintaining a bloom filter or summary index
        # For now, we'll check the metadata to see if we should look deeper

        # Optimization: Keep a cached set of all processed keys from recent archives
        # This is a trade-off between memory and disk I/O

        # For this implementation, we'll assume if it's not in working set, it's not processed
        # In production, you might want to implement a more sophisticated check
        return False

    def mark_processed(self, exec_id, img_id):
        """Mark an analysis as processed"""
        key = f"{exec_id}:{img_id}"
        self.working_set[key] = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "exec_id": exec_id,
            "img_id": img_id,
        }
        self.entry_count += 1

        # Check if rotation is needed
        should_rotate, reason = self._should_rotate()
        if should_rotate:
            logger.info(f"Triggering checkpoint rotation due to {reason}")
            self.save()  # Save before rotation
            self._rotate_checkpoint()

    def mark_failed(self, exec_id, img_id, error=None):
        """Mark an analysis as failed"""
        key = f"{exec_id}:{img_id}"
        self.failed[key] = {
            "status": "failed",
            "timestamp": datetime.now().isoformat(),
            "error": str(error) if error else "Unknown error",
            "exec_id": exec_id,
            "img_id": img_id,
        }

    def save(self):
        """Save current checkpoint"""
        data = {
            "processed": self.working_set,
            "failed": self.failed,
            "metadata": {
                "last_save": datetime.now().isoformat(),
                "entry_count": self.entry_count,
                "archive_count": len(self.archive_metadata["archives"]),
            },
        }

        with open(self.checkpoint_file, "w") as f:
            json.dump(data, f, indent=2)

    def get_stats(self):
        """Get overall statistics including archived data"""
        current_processed = len(self.working_set)
        current_failed = len(self.failed)

        total_processed = current_processed + self.archive_metadata["total_processed"]
        total_failed = current_failed + self.archive_metadata["total_failed"]

        return {
            "current_processed": current_processed,
            "current_failed": current_failed,
            "total_processed": total_processed,
            "total_failed": total_failed,
            "archive_count": len(self.archive_metadata["archives"]),
            "working_set_size": self.entry_count,
        }

    def cleanup_old_archives(self, days_to_keep=30):
        """Clean up old archives older than specified days"""
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)

        for archive in self.archive_metadata["archives"][:]:
            archive_date = datetime.strptime(
                archive["timestamp"], "%Y%m%d_%H%M%S"
            ).timestamp()
            if archive_date < cutoff_date:
                # Delete old archive file
                archive_file = self.archive_dir / archive["filename"]
                if archive_file.exists():
                    archive_file.unlink()
                    logger.info(f"Deleted old archive: {archive['filename']}")

                # Remove from metadata
                self.archive_metadata["archives"].remove(archive)

        self._save_archive_metadata()


# =====================
# 📦 HELPER FUNCTIONS
# =====================


def get_image_hash(image_id):
    """Generate SHA-256 hash for image ID."""
    return hashlib.sha256(image_id.encode()).hexdigest()


def denormalize_coordinates(coords, image_width, image_height):
    """
    Convert normalized coordinates (0-1) back to pixel coordinates.

    Args:
        coords: List of [x, y] coordinate pairs (normalized 0-1)
        image_width: Original image width in pixels
        image_height: Original image height in pixels

    Returns:
        List of denormalized [x, y] coordinates
    """
    denormalized = []
    for coord_pair in coords:
        x = coord_pair[0] * image_width
        y = coord_pair[1] * image_height
        denormalized.append([x, y])
    return denormalized


def polygon_to_wkt(geometry, image_width, image_height):
    """
    Convert MongoDB polygon geometry to WKT format.
    Handles normalized coordinates by denormalizing them first.

    Args:
        geometry: GeoJSON-like geometry dict with normalized coordinates
        image_width: Original image width for denormalization
        image_height: Original image height for denormalization

    Returns:
        WKT polygon string: "POLYGON ((x1 y1, x2 y2, ...))"
    """
    if not geometry or geometry.get("type") != "Polygon":
        return None

    coords = geometry.get("coordinates", [[]])[0]  # Get outer ring
    if not coords:
        return None

    # Denormalize coordinates
    denorm_coords = denormalize_coordinates(coords, image_width, image_height)

    # Convert to WKT format
    wkt_coords = []
    for x, y in denorm_coords:
        wkt_coords.append(f"{x:.2f} {y:.2f}")

    # Close the polygon if not already closed
    if len(wkt_coords) > 0 and wkt_coords[0] != wkt_coords[-1]:
        wkt_coords.append(wkt_coords[0])

    wkt = f"POLYGON (({', '.join(wkt_coords)}))"
    return wkt


def create_feature_collection_header(g, analysis_doc, batch_num, total_marks=None):
    """
    Create the FeatureCollection header with <> self-reference.
    Based on A.py structure with analysis metadata.
    """
    # Extract key metadata from analysis
    analysis = analysis_doc["analysis"]
    image = analysis_doc["image"]
    params = analysis["algorithm_params"]

    # Generate timestamp
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    # Get image hash
    image_hash = get_image_hash(image["imageid"])

    # Image dimensions
    image_width = int(params.get("image_width", 0))
    image_height = int(params.get("image_height", 0))

    # Patch dimensions
    patch_width = int(params.get("patch_width", 4000))
    patch_height = int(params.get("patch_height", 4000))

    # Case ID for identifier
    case_id = params.get("case_id", image["imageid"])

    # Create image object with EXIF dimensions
    image_uri = URIRef(f"urn:sha256:{image_hash}")
    g.add((image_uri, RDF.type, SO.ImageObject))
    g.add((image_uri, DC.identifier, Literal(case_id)))
    g.add((image_uri, EXIF.width, Literal(image_width, datatype=XSD.integer)))
    g.add((image_uri, EXIF.height, Literal(image_height, datatype=XSD.integer)))

    # Add study and subject info if available
    if image.get("study"):
        g.add((image_uri, DC.source, Literal(image["study"])))
    if image.get("subject"):
        g.add((image_uri, DC.subject, Literal(image["subject"])))

    # Create FeatureCollection with <> self-reference
    fc = URIRef("")  # This creates the <> self-reference
    g.add((fc, RDF.type, GEO.FeatureCollection))
    g.add((fc, DC.creator, Literal("http://orcid.org/0000-0003-4165-4062")))
    g.add((fc, DC.date, Literal(timestamp, datatype=XSD.dateTime)))

    # Description with batch info
    desc = f"Nuclear segmentation predictions for {case_id} - batch {batch_num}"
    if total_marks:
        desc += f" ({total_marks} marks)"
    g.add((fc, DC.description, Literal(desc)))

    g.add((fc, DC.publisher, URIRef("https://ror.org/01882y777")))
    g.add((fc, DC.publisher, URIRef("https://ror.org/05qghxh33")))
    g.add((fc, DC.references, Literal("https://doi.org/10.1038/s41597-020-0528-1")))
    g.add((fc, DC.title, Literal("nuclear-segmentation-predictions")))

    # Add cancer type if available from study name (e.g., TCGA-BRCA -> brca)
    if image.get("study"):
        study = image["study"]
        if "TCGA-" in study:
            cancer_type = study.replace("TCGA-", "").lower()
            g.add((fc, HAL.cancerType, Literal(cancer_type)))

    # Add patch dimensions as hal properties
    g.add((fc, HAL.patchWidth, Literal(patch_width, datatype=XSD.integer)))
    g.add((fc, HAL.patchHeight, Literal(patch_height, datatype=XSD.integer)))

    # Add provenance
    prov_blank = URIRef(f"_:prov_{batch_num}")
    g.add((fc, PROV.wasGeneratedBy, prov_blank))
    g.add((prov_blank, RDF.type, PROV.Activity))
    g.add((prov_blank, PROV.used, image_uri))

    # Add algorithm association
    algo_uri = URIRef(f"https://github.com/{analysis['computation']}")
    g.add((prov_blank, PROV.wasAssociatedWith, algo_uri))

    # Add analysis metadata
    g.add((fc, HAL.executionId, Literal(analysis["execution_id"])))
    g.add((fc, HAL.analysisType, Literal(analysis["type"])))
    g.add((fc, HAL.computation, Literal(analysis["computation"])))

    # Add key algorithm parameters as hal properties
    if params.get("mpp"):
        g.add((fc, HAL.mpp, Literal(params["mpp"], datatype=XSD.float)))
    if params.get("min_size"):
        g.add((fc, HAL.minSize, Literal(params["min_size"], datatype=XSD.float)))
    if params.get("max_size"):
        g.add((fc, HAL.maxSize, Literal(params["max_size"], datatype=XSD.float)))

    return fc, image_width, image_height


def add_mark_as_feature(g, fc, mark, image_width, image_height):
    """
    Add a mark document as a feature member of the FeatureCollection.
    Converts MongoDB mark format to match A.py structure.
    """
    # Extract geometry and convert to WKT
    geometries = mark.get("geometries", {})
    features = geometries.get("features", [])

    if not features:
        return False

    # Get the first feature's geometry (usually there's only one)
    feature = features[0]
    geometry = feature.get("geometry", {})

    # Convert to WKT
    wkt = polygon_to_wkt(geometry, image_width, image_height)
    if not wkt:
        return False

    # Get annotation properties
    annotations = mark.get("properties", {}).get("annotations", {})
    area_pixels = annotations.get("AreaInPixels", mark.get("footprint", 0))
    physical_size = annotations.get("PhysicalSize", 0)

    # SNOMED classification
    snomed_id = NUCLEAR_MATERIAL_SNOMED.split("/")[-1]

    # Create feature as blank node
    feature_node = URIRef(f"_:feature_{mark['_id']}")
    g.add((fc, RDFS.member, feature_node))
    g.add((feature_node, RDF.type, GEO.Feature))

    # Add geometry
    geom_node = URIRef(f"_:geom_{mark['_id']}")
    g.add((feature_node, GEO.hasGeometry, geom_node))
    g.add((geom_node, GEO.asWKT, Literal(wkt, datatype=GEO.wktLiteral)))

    # Add classification
    g.add((feature_node, HAL.classification, SNO[snomed_id]))

    # Add measurement with probability
    measurement_node = URIRef(f"_:measurement_{mark['_id']}")
    g.add((feature_node, HAL.measurement, measurement_node))
    g.add((measurement_node, HAL.classification, SNO[snomed_id]))
    g.add((measurement_node, HAL.hasProbability, Literal(1.0, datatype=XSD.float)))

    # Add area information
    if area_pixels:
        g.add(
            (
                feature_node,
                HAL.areaInPixels,
                Literal(int(area_pixels), datatype=XSD.integer),
            )
        )
    if physical_size:
        g.add(
            (
                feature_node,
                HAL.physicalSize,
                Literal(float(physical_size), datatype=XSD.float),
            )
        )

    # Add mark ID as identifier
    g.add((feature_node, DC.identifier, Literal(str(mark["_id"]))))

    # Add object type
    if mark.get("object_type"):
        g.add((feature_node, DC.type, Literal(mark["object_type"])))

    # Add centroid coordinates (denormalized)
    if mark.get("x") is not None and mark.get("y") is not None:
        centroid_x = mark["x"] * image_width
        centroid_y = mark["y"] * image_height
        g.add((feature_node, HAL.centroidX, Literal(centroid_x, datatype=XSD.float)))
        g.add((feature_node, HAL.centroidY, Literal(centroid_y, datatype=XSD.float)))

    # Add bounding box if available (denormalized)
    if mark.get("bbox"):
        bbox = mark["bbox"]
        bbox_denorm = [
            bbox[0] * image_width,  # minX
            bbox[1] * image_height,  # minY
            bbox[2] * image_width,  # maxX
            bbox[3] * image_height,  # maxY
        ]
        g.add((feature_node, HAL.bboxMinX, Literal(bbox_denorm[0], datatype=XSD.float)))
        g.add((feature_node, HAL.bboxMinY, Literal(bbox_denorm[1], datatype=XSD.float)))
        g.add((feature_node, HAL.bboxMaxX, Literal(bbox_denorm[2], datatype=XSD.float)))
        g.add((feature_node, HAL.bboxMaxY, Literal(bbox_denorm[3], datatype=XSD.float)))

    return True


def flush_graph(graph, exec_id, img_id, batch_num):
    """Write graph to TTL file."""
    filename = generate_batch_filename(
        CONFIG.output_dir, exec_id, img_id, batch_num, "ttl"
    )

    # Serialize with proper formatting
    ttl_content = graph.serialize(format="turtle")

    # Write to file
    with open(filename, "w", encoding="utf-8") as f:
        f.write(ttl_content)

    logger.info(f"✅ Wrote batch {batch_num} for {exec_id}/{img_id} → {filename}")


def process_analysis(db, analysis_doc, checkpoint):
    """Process a single analysis and its associated marks."""
    exec_id = analysis_doc["analysis"]["execution_id"]
    img_id = analysis_doc["image"]["imageid"]

    if checkpoint.is_processed(exec_id, img_id):
        logger.info(f"⭐️ Skipping already processed {exec_id}:{img_id}")
        return

    logger.info(f"🚀 Starting ETL for {exec_id}:{img_id}")
    marks_cursor = db.mark.find(
        {
            "provenance.analysis.execution_id": exec_id,
            "provenance.image.imageid": img_id,
        },
        no_cursor_timeout=True,
    )

    batch_num = 1
    count = 0
    batch_count = 0

    try:
        # Create new graph for this batch
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
                "ex": EX,
            }
        )

        # Create FeatureCollection header
        fc, image_width, image_height = create_feature_collection_header(
            g, analysis_doc, batch_num
        )

        for mark in tqdm(marks_cursor, desc=f"Processing {exec_id}:{img_id}"):
            # Add mark as feature to the collection
            if add_mark_as_feature(g, fc, mark, image_width, image_height):
                count += 1
                batch_count += 1

            # Flush batch when it reaches the configured size
            if batch_count >= CONFIG.batch_size:
                flush_graph(g, exec_id, img_id, batch_num)

                # Start new batch
                batch_num += 1
                batch_count = 0
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
                        "ex": EX,
                    }
                )
                fc, image_width, image_height = create_feature_collection_header(
                    g, analysis_doc, batch_num
                )

        # Final flush for remaining marks
        if batch_count > 0:
            flush_graph(g, exec_id, img_id, batch_num)

        checkpoint.mark_processed(exec_id, img_id)
        checkpoint.save()
        logger.info(
            f"✅ Completed {exec_id}:{img_id} ({count} marks in {batch_num} batches)"
        )

    except Exception as e:
        logger.error(f"💥 Error processing {exec_id}:{img_id}: {e}")
        checkpoint.mark_failed(exec_id, img_id, error=str(e))
        checkpoint.save()
        raise  # Re-raise to see full traceback during development
    finally:
        marks_cursor.close()


def main():
    """Main entry point for the merged ETL with rotation."""

    # Initialize rotating checkpoint manager
    checkpoint = RotatingCheckpointManager(
        CONFIG.checkpoint_file,
        CONFIG.checkpoint_max_size,
        CONFIG.checkpoint_max_entries,
        CONFIG.checkpoint_archive_dir,
        CONFIG.checkpoint_compression,
    )

    # Optional: Clean up old archives (older than 30 days)
    checkpoint.cleanup_old_archives(days_to_keep=30)

    # Show initial stats
    initial_stats = checkpoint.get_stats()
    logger.info(f"📊 Initial stats: {initial_stats}")

    with mongo_connection("mongodb://localhost:27018/", "camic") as db:
        total = db.analysis.count_documents({})
        logger.info(f"🎬 Starting ETL run: {total} analyses in database")

        # Process in batches to avoid loading all analyses at once
        batch_size = 100
        skip = 0

        while True:
            # Get next batch of analyses
            analyses = list(db.analysis.find().skip(skip).limit(batch_size))

            if not analyses:
                break

            logger.info(
                f"Processing batch {skip//batch_size + 1} ({len(analyses)} analyses)"
            )

            for analysis_doc in analyses:
                try:
                    process_analysis(db, analysis_doc, checkpoint)
                except KeyboardInterrupt:
                    logger.info("⚠️ Interrupted by user, saving checkpoint...")
                    checkpoint.save()
                    sys.exit(0)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    continue

            skip += batch_size

            # Periodic checkpoint save
            checkpoint.save()

            # Show progress stats
            stats = checkpoint.get_stats()
            logger.info(
                f"📊 Progress - Total processed: {stats['total_processed']}, "
                f"Total failed: {stats['total_failed']}, "
                f"Archives: {stats['archive_count']}"
            )

    logger.info("🎉 All analyses processed!")

    # Final stats
    final_stats = checkpoint.get_stats()
    logger.info(
        f"""
📊 Final Statistics:
    - Current working set: {final_stats['current_processed']} processed, {final_stats['current_failed']} failed
    - Total all time: {final_stats['total_processed']} processed, {final_stats['total_failed']} failed  
    - Archives created: {final_stats['archive_count']}
    """
    )


if __name__ == "__main__":
    main()
