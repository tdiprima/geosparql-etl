"""
ETL script to convert nuclear segmentation CSV files to GeoSPARQL format
with SNOMED URIs for nuclear material classification.

VERSION 2 CHANGES:
- Removed redundant hal:classification from measurement nodes
- For single-class scenarios (nuclear segmentation), only the feature gets classification
- Measurements only contain hal:hasProbability (no classification needed)

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
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

from etl_core import get_image_hash, process_single_csv


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
        default="${CVPR_DATA_DIR}",
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
        f"Parallel workers:      {args.workers or f'{max(1, cpu_count() - 1)} (auto)'}"
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
