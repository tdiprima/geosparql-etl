"""
ETL script to convert nuclear segmentation CSV files to GeoSPARQL format
with SNOMED URIs for nuclear material classification.

Processes CSV files from nuclear segmentation results with polygon coordinates
and converts them to Turtle/RDF format with GeoSPARQL geometries.

Supports resumption: skips already-processed files and can start from a specific image.

# Maximum speed (use all 32 cores)
python nuclear_segmentation_etl.py --workers 32

# Resume with parallelism
python nuclear_segmentation_etl.py --start-from "TCGA-A7-A0CD-01Z-00-DX1.F045B9C8-049C-41BF-8432-EF89F236D34D.svs" --workers 32

# Check help
python nuclear_segmentation_etl.py --help
"""

import argparse
import csv
import gzip
import hashlib
import subprocess
from datetime import datetime, timezone
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

# SNOMED URI for nuclear material (nucleoplasm)
# Using SNOMED code for nuclear material/nucleoplasm
NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"  # Nucleoplasm


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
    coords_str = polygon_string.strip("[]")
    coords = coords_str.split(":")

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
        try:
            info = {
                "x": int(parts[0]),
                "y": int(parts[1]),
                "width": int(parts[2]),
                "height": int(parts[3]),
                "base_name": name,
            }
            return info
        except ValueError:
            pass

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


def create_geosparql_ttl(csv_path, image_name, image_hash=None, cancer_type=None):
    """
    Convert nuclear segmentation CSV to GeoSPARQL TTL format.

    Args:
        csv_path: Path to CSV file (contains patch data)
        image_name: Name of the parent SVS image (from directory name)
        image_hash: SHA-256 hash of image (optional, generated from image_name if not provided)
        cancer_type: Cancer type identifier (e.g., "blca") extracted from polygon directory

    Returns:
        Turtle/RDF content as string
    """
    csv_path = Path(csv_path)
    filename = csv_path.name

    # Extract patch info from CSV filename (x, y, width, height)
    patch_info = extract_image_info_from_filename(filename)

    # Generate image hash from image name if not provided
    if image_hash is None:
        image_hash = get_image_hash(image_id=image_name)

    timestamp = datetime.now(tz=timezone.utc).isoformat()

    # TTL header with prefixes
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

    # Add image object (SVS image - we don't have actual dimensions)
    ttl_content += f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_name}" .

"""

    # Start feature collection with <> as the subject (self-reference)
    # Include patch dimensions in description
    patch_desc = f"patch {patch_info['x']}_{patch_info['y']} ({patch_info['width']}x{patch_info['height']})"

    # Build the feature collection with optional cancer type
    ttl_content += f"""<>      a                    geo:FeatureCollection;
        dc:creator           "http://orcid.org/0000-0003-4165-4062";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - {patch_desc}";
        dc:publisher         <https://ror.org/01882y777> , <https://ror.org/05qghxh33>;
        dc:references        "https://doi.org/10.1038/s41597-020-0528-1";
        dc:title             "nuclear-segmentation-predictions";"""

    # Add cancer type if provided
    if cancer_type:
        ttl_content += f"""
        hal:cancerType       "{cancer_type}";"""

    ttl_content += f"""
        hal:patchX           "{patch_info['x']}"^^xsd:int;
        hal:patchY           "{patch_info['y']}"^^xsd:int;
        hal:patchWidth       "{patch_info['width']}"^^xsd:int;
        hal:patchHeight      "{patch_info['height']}"^^xsd:int;
        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:{image_hash}>;
                               prov:wasAssociatedWith  <https://github.com/nuclear-segmentation-model>
                             ];
"""

    # Read CSV and process features
    feature_count = 0
    snomed_id = NUCLEAR_MATERIAL_SNOMED.split("/")[-1]

    with open(csv_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            area_pixels = row.get("AreaInPixels", "")
            physical_size = row.get("PhysicalSize", "")
            polygon_str = row.get("Polygon", "")

            if not polygon_str:
                continue

            # Convert polygon to WKT
            wkt = parse_polygon_to_wkt(polygon_str)

            if not wkt:
                continue

            # Add separator for multiple features
            if feature_count > 0:
                ttl_content += ";\n"

            # Add feature with proper indentation for <> subject
            # Use probability of 1.0 as placeholder (as per requirements)
            ttl_content += f"""        rdfs:member          [ a                   geo:Feature;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}" ];
                               hal:classification  sno:{snomed_id};
                               hal:measurement     [ hal:classification  sno:{snomed_id};
                                                     hal:hasProbability  "1.0"^^xsd:float
                                                   ]"""

            # Optionally include area information as additional properties
            if area_pixels:
                ttl_content += f""";
                               hal:areaInPixels    "{area_pixels}"^^xsd:int"""

            if physical_size:
                ttl_content += f""";
                               hal:physicalSize    "{physical_size}"^^xsd:float"""

            ttl_content += "\n                             ]"
            feature_count += 1

    # Close the feature collection with proper terminator
    ttl_content += " .\n"

    return ttl_content


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


def process_image_directories(
    input_base_dir, output_dir, compress=False, start_from_image=None, workers=None
):
    """
    Process directories of SVS images, where each directory contains CSV patch files.

    Directory structure:
        input_base_dir/
            blca_polygon/                          # Cancer type folder
                TCGA-*.svs.tar.gz/                 # Slide tar.gz folder
                    blca_polygon/                  # Inner polygon folder
                        TCGA-*.svs/                # SVS folder
                            X_Y_WIDTH_HEIGHT_INFO-features.csv
                            ...

    Args:
        input_base_dir: Base directory containing cancer type folders (*_polygon)
        output_dir: Directory for output TTL files
        compress: If True, gzip compress the output files
        start_from_image: If provided, skip all images until this one is reached
        workers: Number of parallel workers (default: cpu_count - 1)
    """
    input_path = Path(input_base_dir)
    output_path = Path(output_dir)

    # Set number of workers
    if workers is None:
        workers = max(1, cpu_count() - 1)  # Leave one core free

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Find all top-level cancer type folders (*_polygon)
    cancer_type_dirs = [
        d for d in input_path.iterdir() if d.is_dir() and d.name.endswith("_polygon")
    ]

    if not cancer_type_dirs:
        print(f"No *_polygon directories found in {input_base_dir}")
        return

    print(f"Found {len(cancer_type_dirs)} cancer type directories to process")
    print(f"Using {workers} parallel workers")

    total_success = 0
    total_error = 0
    total_skipped = 0
    found_start_image = (
        start_from_image is None
    )  # If no start image specified, start immediately

    for cancer_type_dir in cancer_type_dirs:
        # Extract cancer type from directory name (e.g., "blca" from "blca_polygon")
        cancer_type_name = cancer_type_dir.name
        cancer_type = cancer_type_name.replace("_polygon", "")
        prefix = cancer_type + "_"

        print(f"\nProcessing cancer type: {cancer_type_name} (type: {cancer_type})")

        # Find all *.svs.tar.gz subdirectories within the cancer type folder
        tar_gz_dirs = [
            d
            for d in cancer_type_dir.iterdir()
            if d.is_dir() and d.name.endswith(".svs.tar.gz")
        ]

        if not tar_gz_dirs:
            print(f"  ⚠ No .svs.tar.gz directories found in {cancer_type_name}")
            continue

        print(f"  Found {len(tar_gz_dirs)} slide directories")

        for tar_gz_dir in tar_gz_dirs:
            # Find the inner *_polygon directory
            inner_polygon_dirs = [
                d
                for d in tar_gz_dir.iterdir()
                if d.is_dir() and d.name.endswith("_polygon")
            ]

            if not inner_polygon_dirs:
                print(f"    ⚠ No inner polygon directory found in {tar_gz_dir.name}")
                continue

            # Process each inner polygon directory (should typically be just one)
            for inner_polygon_dir in inner_polygon_dirs:
                # Find all *.svs subdirectories
                svs_dirs = [
                    d
                    for d in inner_polygon_dir.iterdir()
                    if d.is_dir() and d.name.endswith(".svs")
                ]

                if not svs_dirs:
                    print(
                        f"    ⚠ No .svs directories found in {inner_polygon_dir.name}"
                    )
                    continue

                for svs_dir in svs_dirs:
                    image_name = svs_dir.name

                    # Check if we should skip this image (before start_from_image)
                    if not found_start_image:
                        if image_name == start_from_image:
                            found_start_image = True
                            print(f"  ▶ Starting from image: {image_name}")
                        else:
                            print(
                                f"  ⏭ Skipping image (before start point): {image_name}"
                            )
                            continue
                    else:
                        print(f"  Processing image: {image_name}")

                    # Get all CSV files in this SVS directory
                    csv_files = list(svs_dir.glob("*-features.csv"))

                    if not csv_files:
                        print(f"    ⚠ No CSV files found in {image_name}")
                        continue

                    print(f"    Found {len(csv_files)} patch CSV files")

                    # Generate image hash once for all patches
                    image_hash = get_image_hash(image_id=image_name)

                    success_count = 0
                    error_count = 0
                    skipped_count = 0

                    # Create worker function with fixed parameters
                    worker_func = partial(
                        process_single_csv,
                        image_name=image_name,
                        image_hash=image_hash,
                        cancer_type=cancer_type,
                        prefix=prefix,
                        output_path=output_path,
                        compress=compress,
                    )

                    # Process CSV files in parallel
                    with Pool(processes=workers) as pool:
                        results = pool.map(worker_func, csv_files)

                    # Count results
                    for result in results:
                        if result[0] == "success":
                            success_count += 1
                        elif result[0] == "skipped":
                            skipped_count += 1
                        elif result[0] == "error":
                            error_count += 1
                            error_msg = (
                                result[2] if len(result) > 2 else "Unknown error"
                            )
                            print(f"      ✗ Error processing {result[1]}: {error_msg}")

                    print(f"    ✓ Processed {success_count} patches successfully")
                    if skipped_count > 0:
                        print(f"    ⏭ Skipped {skipped_count} patches (already exist)")
                    if error_count > 0:
                        print(f"    ✗ {error_count} errors")

                    total_success += success_count
                    total_error += error_count
                    total_skipped += skipped_count

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Total success: {total_success} files")
    print(f"  Total skipped: {total_skipped} files (already processed)")
    print(f"  Total errors: {total_error} files")


def main():
    """Main entry point for the ETL script."""

    # Set up argument parser
    formatter_class = (
        rich_argparse.RichHelpFormatter if rich_argparse else argparse.HelpFormatter
    )
    parser = argparse.ArgumentParser(
        description="Nuclear Segmentation to GeoSPARQL ETL Converter - "
        "Converts nuclear segmentation CSV files to GeoSPARQL RDF format",
        formatter_class=formatter_class,
    )

    parser.add_argument(
        "-i",
        "--input",
        default="/data3/tammy/nuclear_segmentation_data/cvpr-data",
        help="Input base directory containing cancer type folders (*_polygon)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="./nuclear_geosparql_output",
        help="Output directory for TTL files",
    )

    parser.add_argument(
        "-c",
        "--compress",
        action="store_true",
        default=True,
        help="Gzip compress output files (default: True)",
    )

    parser.add_argument(
        "--no-compress",
        action="store_false",
        dest="compress",
        help="Disable gzip compression",
    )

    parser.add_argument(
        "-s",
        "--start-from",
        metavar="IMAGE_NAME",
        help="Start processing from a specific image (e.g., 'TCGA-A7-A0CD-01Z-00-DX1.F045B9C8-049C-41BF-8432-EF89F236D34D.svs'). "
        "All images before this one will be skipped.",
    )

    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        metavar="N",
        help=f"Number of parallel workers for processing CSV files (default: CPU count - 1 = {max(1, cpu_count() - 1)}). "
        f"With {cpu_count()} cores available, you can use up to {cpu_count()} workers.",
    )

    args = parser.parse_args()

    print("Nuclear Segmentation to GeoSPARQL ETL Converter")
    print("=" * 60)
    print(f"Input base directory:  {args.input}")
    print(f"Output directory:      {args.output}")
    print(f"Compression:           {'Enabled (gzip)' if args.compress else 'Disabled'}")
    print(
        f"Parallel workers:      {args.workers if args.workers else f'{max(1, cpu_count() - 1)} (auto)'}"
    )
    if args.start_from:
        print(f"Start from image:      {args.start_from}")
    print()

    # Process all image directories
    process_image_directories(
        args.input,
        args.output,
        compress=args.compress,
        start_from_image=args.start_from,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
