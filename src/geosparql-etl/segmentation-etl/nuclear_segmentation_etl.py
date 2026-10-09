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
import os
from multiprocessing import cpu_count
from pathlib import Path

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from etl_core import process_cancer_type_directory


def process_image_directories(
    input_base_dir, output_dir, compress=False, start_from_image=None, workers=None
):
    """
    Process all cancer type directories under input_base_dir.

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

    # Find all top-level cancer type folders (*_polygon)
    cancer_type_dirs = sorted(
        d for d in input_path.iterdir() if d.is_dir() and d.name.endswith("_polygon")
    )

    if not cancer_type_dirs:
        print(f"No *_polygon directories found in {input_base_dir}")
        return

    print(f"Found {len(cancer_type_dirs)} cancer type directories to process")

    total_success = 0
    total_error = 0
    total_skipped = 0
    found_start_image = start_from_image is None

    for cancer_type_dir in cancer_type_dirs:
        # Once the start image has been found in an earlier cancer-type dir,
        # pass None so subsequent dirs start immediately.
        current_start = None if found_start_image else start_from_image

        success, errors, skipped, found = process_cancer_type_directory(
            cancer_type_dir, output_dir, compress, current_start, workers
        )

        if found:
            found_start_image = True

        total_success += success
        total_error += errors
        total_skipped += skipped

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Total success: {total_success} files")
    print(f"  Total skipped: {total_skipped} files (already processed)")
    print(f"  Total errors:  {total_error} files")


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
        default=os.getenv("CVPR_DATA_DIR", "/path/to/cvpr-data"),
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
