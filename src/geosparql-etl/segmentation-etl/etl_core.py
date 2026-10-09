"""
Shared ETL core: polygon parsing, image hashing, TTL generation, CSV-to-file dispatch,
and single-cancer-type directory traversal.

Used by nuclear_segmentation_etl.py and single_cancer_etl.py.
"""

import csv
import gzip
import hashlib
import os
import subprocess
from contextlib import suppress
from datetime import datetime, timezone
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path

import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

CREATOR_ORCID = os.getenv("CREATOR_ORCID", "http://orcid.org/0000-0000-0000-0000")
PUBLISHER_RORS = " , ".join(f"<{r.strip()}>" for r in os.getenv("PUBLISHER_RORS", "https://ror.org/00000000a").split(",") if r.strip())
DATASET_DOI = os.getenv("DATASET_DOI", "https://doi.org/10.0000/example")

# SNOMED URI for nuclear material (nucleoplasm)
# Using SNOMED code for nuclear material/nucleoplasm
NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"  # Nucleoplasm

# Whitespace-aligned prefix for an rdfs:member line inside a FeatureCollection.
# Kept as a constant so per-patch and per-slide renderers emit identical formatting.
MEMBER_PREFIX = "        rdfs:member          "

# Placeholder probability used when the source data carries no per-polygon score.
PROBABILITY_PLACEHOLDER = "1.0"


def parse_polygon_to_wkt(polygon_string):
    """
    Convert polygon string format (x1:y1:x2:y2:...) to WKT format.

    Args:
        polygon_string: String in format [x1:y1:x2:y2:x3:y3:...]

    Returns:
        WKT polygon string: "POLYGON ((x1 y1, x2 y2, ...))"
    """
    if not polygon_string:
        return None

    # Remove brackets and split by colons
    coords = polygon_string.strip("[]").split(":")

    # Parse coordinate pairs
    wkt_coords = []
    for i in range(0, len(coords), 2):
        if i + 1 < len(coords):
            x = coords[i]
            y = coords[i + 1]
            wkt_coords.append(f"{x} {y}")

    # Close the polygon by adding first point at the end if not already closed
    if len(wkt_coords) > 0 and wkt_coords[0] != wkt_coords[-1]:
        wkt_coords.append(wkt_coords[0])

    wkt = f"POLYGON (({', '.join(wkt_coords)}))"
    return wkt


def extract_image_info_from_filename(filename):
    """
    Extract image metadata from filename.
    Format: X_Y_WIDTH_HEIGHT_OTHER_INFO-features.csv
    Example: 24001_72001_4000_4000_0.2325_1-features.csv

    Returns:
        dict with x, y, width, height, and base_name
    """
    # Remove .csv extension
    name = filename.replace(".csv", "")

    # Split by underscore
    parts = name.split("_")

    if len(parts) >= 4:
        with suppress(ValueError):
            info = {
                "x": int(parts[0]),
                "y": int(parts[1]),
                "width": int(parts[2]),
                "height": int(parts[3]),
                "base_name": name,
            }
            return info

    # Fallback - use filename as base name
    return {
        "x": 0,
        "y": 0,
        "width": 40000,  # Default assumption
        "height": 40000,
        "base_name": name,
    }


def get_image_hash(image_path=None, image_id=None):
    """
    Generate SHA-256 hash for image.
    Can use actual image file or generate from ID string.

    Args:
        image_path: Path to image file (optional)
        image_id: String identifier to hash (optional)

    Returns:
        SHA-256 hash string
    """
    if image_path and Path(image_path).exists():
        # Use sha256sum command if available
        try:
            result = subprocess.run(
                ["sha256sum", str(image_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Fall back to Python implementation
            sha256_hash = hashlib.sha256()
            with open(image_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()

    # Generate from image_id string
    if image_id:
        return hashlib.sha256(image_id.encode()).hexdigest()

    # Default fallback
    return hashlib.sha256(b"unknown").hexdigest()


def read_segmentation_rows(csv_path):
    """
    Read nuclear segmentation features from a CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        list of dicts with keys: polygon_str, area_pixels, physical_size
    """
    rows = []
    with open(csv_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append({
                "polygon_str": row.get("Polygon", ""),
                "area_pixels": row.get("AreaInPixels", ""),
                "physical_size": row.get("PhysicalSize", ""),
            })
    return rows


def render_feature_member(row, snomed_id):
    """
    Render a single geo:Feature block (the value of an rdfs:member predicate).

    Pure function. Returns the bracketed Turtle blank-node string starting at
    "[" and ending at "]", or None when the row has no usable polygon.

    Args:
        row: dict from read_segmentation_rows (polygon_str, area_pixels, physical_size)
        snomed_id: SNOMED code for the feature classification

    Returns:
        Turtle blank-node string, or None to skip the row
    """
    if not row["polygon_str"]:
        return None

    wkt = parse_polygon_to_wkt(row["polygon_str"])
    if not wkt:
        return None

    # Use a placeholder probability of 1.0 (source data has no per-polygon score)
    member = f"""[ a                   geo:Feature;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}"^^geo:wktLiteral ];
                               hal:classification  sno:{snomed_id};
                               hal:measurement     [ hal:hasProbability  "{PROBABILITY_PLACEHOLDER}"^^xsd:float ]"""

    # Optionally include area information as additional properties
    if row["area_pixels"]:
        member += f""";
                               hal:areaInPixels    "{row['area_pixels']}"^^xsd:int"""

    if row["physical_size"]:
        member += f""";
                               hal:physicalSize    "{row['physical_size']}"^^xsd:float"""

    member += """
                             ]"""
    return member


def render_feature_collection_ttl(
    rows, patch_info, image_name, image_hash, cancer_type, timestamp
):
    """
    Render a GeoSPARQL Turtle document from pre-read segmentation rows.
    Pure function: no I/O, fully deterministic given the same inputs.

    Args:
        rows: list of dicts from read_segmentation_rows
        patch_info: dict with x, y, width, height from extract_image_info_from_filename
        image_name: Name of the parent SVS image
        image_hash: SHA-256 hash of image
        cancer_type: Cancer type identifier (e.g., "blca"), or None
        timestamp: ISO-8601 datetime string for dc:date

    Returns:
        Turtle/RDF content as string
    """
    snomed_id = NUCLEAR_MATERIAL_SNOMED.split("/")[-1]
    patch_desc = (
        f"patch {patch_info['x']}_{patch_info['y']} "
        f"({patch_info['width']}x{patch_info['height']})"
    )

    # TTL header with prefixes
    ttl = f"""@prefix dc:   <http://purl.org/dc/terms/> .
@prefix exif: <http://www.w3.org/2003/12/exif/ns#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix hal:  <https://halcyon.is/ns/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sno:  <http://snomed.info/id/> .
@prefix so:   <https://schema.org/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

"""

    # Add image object (SVS image - we don't have actual dimensions)
    ttl += f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_name}" .

"""

    # Feature collection header
    ttl += f"""<>      a                    geo:FeatureCollection;
        dc:creator           "{CREATOR_ORCID}";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - {patch_desc}";
        dc:publisher         {PUBLISHER_RORS};
        dc:references        "{DATASET_DOI}";
        dc:title             "nuclear-segmentation-predictions";"""

    if cancer_type:
        ttl += f"""
        hal:cancerType       "{cancer_type}";"""

    ttl += f"""
        hal:patchX           "{patch_info['x']}"^^xsd:int;
        hal:patchY           "{patch_info['y']}"^^xsd:int;
        hal:patchWidth       "{patch_info['width']}"^^xsd:int;
        hal:patchHeight      "{patch_info['height']}"^^xsd:int;
        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:{image_hash}>;
                             ];
"""

    # Build each feature string, then join with ";\n" — no fragile counter needed
    feature_strings = []
    for row in rows:
        member = render_feature_member(row, snomed_id)
        if member is None:
            continue
        feature_strings.append(MEMBER_PREFIX + member)

    ttl += ";\n".join(feature_strings)
    ttl += " .\n"
    return ttl


def create_geosparql_ttl(csv_path, image_name, image_hash=None, cancer_type=None):
    """
    Convert nuclear segmentation CSV to GeoSPARQL TTL format.

    Thin wrapper: reads the CSV then delegates to render_feature_collection_ttl.

    Args:
        csv_path: Path to CSV file (contains patch data)
        image_name: Name of the parent SVS image (from directory name)
        image_hash: SHA-256 hash of image (optional, generated from image_name if not provided)
        cancer_type: Cancer type identifier (e.g., "blca") extracted from polygon directory

    Returns:
        Turtle/RDF content as string
    """
    csv_path = Path(csv_path)
    patch_info = extract_image_info_from_filename(csv_path.name)

    if image_hash is None:
        image_hash = get_image_hash(image_id=image_name)

    timestamp = datetime.now(tz=timezone.utc).isoformat()
    rows = read_segmentation_rows(csv_path)
    return render_feature_collection_ttl(
        rows, patch_info, image_name, image_hash, cancer_type, timestamp
    )


def process_single_csv(
    csv_file, image_name, image_hash, cancer_type, prefix, output_path, compress
):
    """
    Process a single CSV file - designed to be called in parallel.

    Args:
        csv_file: Path to CSV file
        image_name: Name of the parent SVS image
        image_hash: SHA-256 hash of image
        cancer_type: Cancer type identifier
        prefix: Prefix for output filename
        output_path: Base output directory
        compress: Whether to compress output

    Returns:
        tuple: (status, csv_filename) where status is 'success', 'skipped', or 'error'
    """
    try:
        # Check if output file already exists
        image_output_dir = output_path / image_name
        output_filename = prefix + csv_file.stem + ".ttl"
        if compress:
            output_filename += ".gz"
        output_file = image_output_dir / output_filename

        if output_file.exists():
            return ("skipped", csv_file.name)

        # Convert to GeoSPARQL with cancer type
        ttl_content = create_geosparql_ttl(
            csv_file, image_name, image_hash, cancer_type
        )

        # Write output file - use image_name as subdirectory
        image_output_dir.mkdir(parents=True, exist_ok=True)

        if compress:
            with gzip.open(output_file, "wt", encoding="utf-8") as f:
                f.write(ttl_content)
        else:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(ttl_content)

        return ("success", csv_file.name)

    except Exception as e:
        return ("error", csv_file.name, str(e))


def process_cancer_type_directory(
    cancer_type_dir, output_dir, compress=False, start_from_image=None, workers=None
):
    """
    Process a single cancer type directory (e.g., coad_polygon).

    Directory structure:
        coad_polygon/
            TCGA-*.svs.tar.gz/
                coad_polygon/
                    TCGA-*.svs/
                        X_Y_WIDTH_HEIGHT_INFO-features.csv

    Args:
        cancer_type_dir: Path to a single *_polygon directory
        output_dir: Directory for output TTL files
        compress: If True, gzip compress the output files
        start_from_image: If provided, skip images until this one is reached
        workers: Number of parallel workers (default: cpu_count - 1)

    Returns:
        tuple: (success_count, error_count, skipped_count, found_start_image)
    """
    input_path = Path(cancer_type_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        print(f"Error: directory not found: {cancer_type_dir}", flush=True)
        return (0, 0, 0, start_from_image is None)

    if workers is None:
        workers = max(1, cpu_count() - 1)

    output_path.mkdir(parents=True, exist_ok=True)

    # Extract cancer type from directory name (e.g., "coad" from "coad_polygon")
    cancer_type = input_path.name.replace("_polygon", "")
    prefix = cancer_type + "_"

    print(f"Processing cancer type: {input_path.name} (type: {cancer_type})")
    print(f"Using {workers} parallel workers")

    # Find all *.svs.tar.gz subdirectories
    tar_gz_dirs = sorted(
        d for d in input_path.iterdir()
        if d.is_dir() and d.name.endswith(".svs.tar.gz")
    )

    if not tar_gz_dirs:
        print(f"No .svs.tar.gz directories found in {input_path.name}")
        return (0, 0, 0, start_from_image is None)

    print(f"Found {len(tar_gz_dirs)} slide directories")

    total_success = 0
    total_error = 0
    total_skipped = 0
    found_start_image = start_from_image is None

    for tar_gz_dir in tar_gz_dirs:
        # Find the inner *_polygon directory
        inner_polygon_dirs = [
            d for d in tar_gz_dir.iterdir()
            if d.is_dir() and d.name.endswith("_polygon")
        ]

        if not inner_polygon_dirs:
            print(f"  No inner polygon directory found in {tar_gz_dir.name}")
            continue

        for inner_polygon_dir in inner_polygon_dirs:
            # Find all *.svs subdirectories
            svs_dirs = sorted(
                d for d in inner_polygon_dir.iterdir()
                if d.is_dir() and d.name.endswith(".svs")
            )

            if not svs_dirs:
                print(f"  No .svs directories found in {inner_polygon_dir.name}")
                continue

            for svs_dir in svs_dirs:
                image_name = svs_dir.name

                if not found_start_image:
                    if image_name == start_from_image:
                        found_start_image = True
                        print(f"  Starting from image: {image_name}")
                    else:
                        print(f"  Skipping (before start point): {image_name}")
                        continue
                else:
                    print(f"  Processing image: {image_name}")

                csv_files = list(svs_dir.glob("*-features.csv"))

                if not csv_files:
                    print(f"    No CSV files found in {image_name}")
                    continue

                print(f"    Found {len(csv_files)} patch CSV files")

                image_hash = get_image_hash(image_id=image_name)

                worker_func = partial(
                    process_single_csv,
                    image_name=image_name,
                    image_hash=image_hash,
                    cancer_type=cancer_type,
                    prefix=prefix,
                    output_path=output_path,
                    compress=compress,
                )

                with Pool(processes=workers) as pool:
                    results = pool.map(worker_func, csv_files)

                success_count = 0
                error_count = 0
                skipped_count = 0

                for result in results:
                    if result[0] == "success":
                        success_count += 1
                    elif result[0] == "skipped":
                        skipped_count += 1
                    elif result[0] == "error":
                        error_count += 1
                        error_msg = result[2] if len(result) > 2 else "Unknown error"
                        print(f"      Error processing {result[1]}: {error_msg}")

                print(f"    Processed {success_count} patches successfully")
                if skipped_count > 0:
                    print(f"    Skipped {skipped_count} patches (already exist)")
                if error_count > 0:
                    print(f"    {error_count} errors")

                total_success += success_count
                total_error += error_count
                total_skipped += skipped_count

    return (total_success, total_error, total_skipped, found_start_image)


# ---------------------------------------------------------------------------
# Per-slide ("one TTL per whole-slide image") pipeline
#
# The functions above emit one TTL per patch CSV. The functions below instead
# integrate every polygon from every patch of a single .svs image into ONE
# TTL FeatureCollection, so the downstream converter produces one HDF5 per
# image. Polygon coordinates are written as-is (already in whole-slide space);
# the image width/height is the bounding box over all patches.
# ---------------------------------------------------------------------------


def compute_slide_dimensions(csv_files):
    """
    Compute whole-slide width and height from patch filenames.

    Each patch filename encodes its X_Y_WIDTH_HEIGHT offset within the slide.
    The slide extent is the maximum (x + width) and (y + height) over all patches.

    Args:
        csv_files: iterable of patch CSV Paths

    Returns:
        tuple (slide_width, slide_height) in pixels
    """
    slide_width = 0
    slide_height = 0
    for csv_file in csv_files:
        info = extract_image_info_from_filename(csv_file.name)
        slide_width = max(slide_width, info["x"] + info["width"])
        slide_height = max(slide_height, info["y"] + info["height"])
    return slide_width, slide_height


def render_slide_ttl_header(
    image_name, image_hash, cancer_type, slide_width, slide_height, patch_count, timestamp
):
    """
    Render the TTL header for a merged whole-slide FeatureCollection.

    Pure function. The returned string ends after the prov:wasGeneratedBy block
    with a trailing ';' and newline, ready for rdfs:member lines to be appended.

    Args:
        image_name: Name of the parent SVS image
        image_hash: SHA-256 hash of the image
        cancer_type: Cancer type identifier (e.g., "coad"), or None
        slide_width: Whole-slide width in pixels
        slide_height: Whole-slide height in pixels
        patch_count: Number of source patch CSV files merged
        timestamp: ISO-8601 datetime string for dc:date

    Returns:
        Turtle header string
    """
    header = f"""@prefix dc:   <http://purl.org/dc/terms/> .
@prefix exif: <http://www.w3.org/2003/12/exif/ns#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix hal:  <https://halcyon.is/ns/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sno:  <http://snomed.info/id/> .
@prefix so:   <https://schema.org/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_name}";
        exif:width   "{slide_width}"^^xsd:int;
        exif:height  "{slide_height}"^^xsd:int .

<>      a                    geo:FeatureCollection;
        dc:creator           "{CREATOR_ORCID}";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - merged from {patch_count} patches";
        dc:publisher         {PUBLISHER_RORS};
        dc:references        "{DATASET_DOI}";
        dc:title             "nuclear-segmentation-predictions";"""

    if cancer_type:
        header += f"""
        hal:cancerType       "{cancer_type}";"""

    header += f"""
        hal:imageWidth       "{slide_width}"^^xsd:int;
        hal:imageHeight      "{slide_height}"^^xsd:int;
        hal:patchCount       "{patch_count}"^^xsd:int;
        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:{image_hash}>;
                             ];
"""
    return header


def write_slide_ttl(csv_files, output_file, image_name, cancer_type, compress):
    """
    Stream all patches of one slide into a single TTL FeatureCollection.

    Writes to a temporary file first, then atomically renames it into place, so
    an interrupted run never leaves a half-written file that resume logic would
    mistake for a completed one. Streaming keeps memory flat regardless of how
    many polygons the slide contains.

    Args:
        csv_files: list of patch CSV Paths for this slide (non-empty)
        output_file: destination Path for the merged TTL
        image_name: Name of the parent SVS image
        cancer_type: Cancer type identifier, or None
        compress: If True, gzip the output

    Returns:
        Number of feature members written
    """
    image_hash = get_image_hash(image_id=image_name)
    slide_width, slide_height = compute_slide_dimensions(csv_files)
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    header = render_slide_ttl_header(
        image_name,
        image_hash,
        cancer_type,
        slide_width,
        slide_height,
        len(csv_files),
        timestamp,
    )

    snomed_id = NUCLEAR_MATERIAL_SNOMED.split("/")[-1]
    tmp_file = output_file.with_name(output_file.name + ".tmp")
    open_func = gzip.open if compress else open

    feature_count = 0
    with open_func(tmp_file, "wt", encoding="utf-8") as out:
        out.write(header)
        for csv_file in csv_files:
            for row in read_segmentation_rows(csv_file):
                member = render_feature_member(row, snomed_id)
                if member is None:
                    continue
                # Header already ends with ';'; separate subsequent members only.
                if feature_count > 0:
                    out.write(";\n")
                out.write(MEMBER_PREFIX + member)
                feature_count += 1
        out.write(" .\n")

    os.replace(tmp_file, output_file)
    return feature_count


def slide_output_filename(cancer_type, image_name, compress):
    """Build the flat per-slide output filename: <cancer>_<image>.ttl[.gz]."""
    name = f"{cancer_type}_{image_name}.ttl"
    if compress:
        name += ".gz"
    return name


def process_single_slide(slide_and_type, output_path, compress):
    """
    Process one whole-slide image directory into a single TTL file.

    Designed to be called in parallel (one task per slide).

    Args:
        slide_and_type: tuple (svs_dir Path, cancer_type str)
        output_path: Base output directory (Path)
        compress: Whether to gzip the output

    Returns:
        tuple: ("success", image_name, feature_count) |
               ("skipped", image_name) | ("empty", image_name) |
               ("error", image_name, message)
    """
    svs_dir, cancer_type = slide_and_type
    image_name = svs_dir.name
    output_file = output_path / slide_output_filename(
        cancer_type, image_name, compress
    )

    if output_file.exists():
        return ("skipped", image_name)

    csv_files = sorted(svs_dir.glob("*-features.csv"))
    if not csv_files:
        return ("empty", image_name)

    try:
        feature_count = write_slide_ttl(
            csv_files, output_file, image_name, cancer_type, compress
        )
        return ("success", image_name, feature_count)
    except Exception as e:
        # Clean up any partial temp file so the next run can retry cleanly.
        tmp_file = output_file.with_name(output_file.name + ".tmp")
        with suppress(OSError):
            tmp_file.unlink()
        return ("error", image_name, str(e))


def iter_slide_directories(cancer_type_dir):
    """
    Yield (svs_dir, cancer_type) for every whole-slide image directory under a
    single *_polygon cancer-type directory.

    Directory structure:
        coad_polygon/
            TCGA-*.svs.tar.gz/
                coad_polygon/
                    TCGA-*.svs/        <-- yielded
                        *-features.csv

    Args:
        cancer_type_dir: Path to a single *_polygon directory

    Yields:
        tuples (svs_dir Path, cancer_type str)
    """
    input_path = Path(cancer_type_dir)
    if not input_path.is_dir():
        return

    cancer_type = input_path.name.replace("_polygon", "")

    tar_gz_dirs = sorted(
        d for d in input_path.iterdir()
        if d.is_dir() and d.name.endswith(".svs.tar.gz")
    )
    for tar_gz_dir in tar_gz_dirs:
        inner_polygon_dirs = sorted(
            d for d in tar_gz_dir.iterdir()
            if d.is_dir() and d.name.endswith("_polygon")
        )
        for inner_polygon_dir in inner_polygon_dirs:
            svs_dirs = sorted(
                d for d in inner_polygon_dir.iterdir()
                if d.is_dir() and d.name.endswith(".svs")
            )
            for svs_dir in svs_dirs:
                yield svs_dir, cancer_type


def select_slides_from_start(slides, start_from_image):
    """
    Apply resume logic: drop slides before start_from_image.

    Args:
        slides: list of (svs_dir, cancer_type) tuples
        start_from_image: image name to start at, or None for all

    Returns:
        tuple (selected_slides, found_start_image)
    """
    if start_from_image is None:
        return slides, True

    selected = []
    found = False
    for svs_dir, cancer_type in slides:
        if not found:
            if svs_dir.name == start_from_image:
                found = True
            else:
                continue
        selected.append((svs_dir, cancer_type))
    return selected, found


def process_cancer_type_per_slide(
    cancer_type_dir, output_dir, compress=False, start_from_image=None, workers=None
):
    """
    Process a single *_polygon directory into one TTL per whole-slide image.

    Parallelizes across slides (each slide writes an independent output file).

    Args:
        cancer_type_dir: Path to a single *_polygon directory
        output_dir: Directory for output TTL files (flat list, one per slide)
        compress: If True, gzip compress the output files
        start_from_image: If provided, skip images until this one is reached
        workers: Number of parallel workers (default: cpu_count - 1)

    Returns:
        tuple: (success_count, error_count, skipped_count, found_start_image)
    """
    input_path = Path(cancer_type_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        print(f"Error: directory not found: {cancer_type_dir}", flush=True)
        return (0, 0, 0, start_from_image is None)

    if workers is None:
        workers = max(1, cpu_count() - 1)

    output_path.mkdir(parents=True, exist_ok=True)

    cancer_type = input_path.name.replace("_polygon", "")
    print(f"Processing cancer type: {input_path.name} (type: {cancer_type})")
    print(f"Using {workers} parallel workers")

    all_slides = list(iter_slide_directories(input_path))
    if not all_slides:
        print(f"No .svs image directories found in {input_path.name}")
        return (0, 0, 0, start_from_image is None)

    slides, found_start_image = select_slides_from_start(all_slides, start_from_image)
    if start_from_image is not None and found_start_image:
        print(f"  Starting from image: {start_from_image}")

    print(f"Found {len(all_slides)} slides ({len(slides)} to process)")

    success_count = 0
    error_count = 0
    skipped_count = 0

    worker_func = partial(
        process_single_slide, output_path=output_path, compress=compress
    )

    with Pool(processes=workers) as pool:
        for result in pool.imap_unordered(worker_func, slides):
            status = result[0]
            image_name = result[1]
            if status == "success":
                success_count += 1
                print(f"  Merged {image_name} ({result[2]:,} polygons)", flush=True)
            elif status == "skipped":
                skipped_count += 1
                print(f"  Skipped {image_name} (output exists)", flush=True)
            elif status == "empty":
                skipped_count += 1
                print(f"  Skipped {image_name} (no patch CSVs)", flush=True)
            elif status == "error":
                error_count += 1
                error_msg = result[2] if len(result) > 2 else "Unknown error"
                print(f"  Error on {image_name}: {error_msg}", flush=True)

    return (success_count, error_count, skipped_count, found_start_image)


def process_input_per_slide(
    input_dir, output_dir, compress=False, start_from_image=None, workers=None
):
    """
    Top-level per-slide driver.

    Accepts either a single *_polygon cancer-type directory or a base directory
    containing several of them, and produces one merged TTL per whole-slide image.

    Args:
        input_dir: A *_polygon directory, or a base dir containing many
        output_dir: Directory for output TTL files
        compress: If True, gzip compress the output files
        start_from_image: If provided, skip images until this one is reached
        workers: Number of parallel workers (default: cpu_count - 1)

    Returns:
        tuple: (total_success, total_error, total_skipped)
    """
    input_path = Path(input_dir)

    if not input_path.is_dir():
        print(f"Error: input directory not found: {input_dir}", flush=True)
        return (0, 0, 0)

    if input_path.name.endswith("_polygon"):
        cancer_type_dirs = [input_path]
    else:
        cancer_type_dirs = sorted(
            d for d in input_path.iterdir()
            if d.is_dir() and d.name.endswith("_polygon")
        )

    if not cancer_type_dirs:
        print(f"No *_polygon directories found in {input_dir}", flush=True)
        return (0, 0, 0)

    print(f"Found {len(cancer_type_dirs)} cancer type directory(ies) to process")

    total_success = 0
    total_error = 0
    total_skipped = 0
    found_start_image = start_from_image is None

    for cancer_type_dir in cancer_type_dirs:
        # Once the start image is found in an earlier dir, process the rest fully.
        current_start = None if found_start_image else start_from_image

        success, errors, skipped, found = process_cancer_type_per_slide(
            cancer_type_dir, output_dir, compress, current_start, workers
        )

        if found:
            found_start_image = True

        total_success += success
        total_error += errors
        total_skipped += skipped

    return (total_success, total_error, total_skipped)
