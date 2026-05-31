"""
ETL script to convert nuclear segmentation CSV files to GeoSPARQL format
for a single cancer type directory (e.g., coad_polygon).

Processes one *_polygon directory instead of scanning the entire base directory.

# Process coad_polygon with defaults
python single_cancer_etl.py -i ${CVPR_DATA_DIR}/coad_polygon

# With all options
python single_cancer_etl.py \
    -i ${CVPR_DATA_DIR}/coad_polygon \
    -o ./coad_output \
    --workers 16 \
    --start-from "TCGA-A6-2671-01Z-00-DX1.8DC29822-6738-4C25-94E0-B97412E3A22D.svs"
"""

import argparse
from multiprocessing import cpu_count

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

from etl_core import process_cancer_type_directory


def main():
    """Main entry point."""
    formatter_class = (
        rich_argparse.RichHelpFormatter if rich_argparse else argparse.HelpFormatter
    )
    parser = argparse.ArgumentParser(
        description="Single Cancer Type Nuclear Segmentation to GeoSPARQL ETL - "
        "Converts one *_polygon directory to GeoSPARQL RDF format",
        formatter_class=formatter_class,
    )

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to a single cancer type directory (e.g., ${CVPR_DATA_DIR}/coad_polygon)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="./nuclear_geosparql_output",
        help="Output directory for TTL files (default: ./nuclear_geosparql_output)",
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
        help="Start processing from a specific image (skip all images before this one)",
    )

    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        metavar="N",
        help=f"Number of parallel workers (default: CPU count - 1 = {max(1, cpu_count() - 1)})",
    )

    args = parser.parse_args()

    print("Single Cancer Type - Nuclear Segmentation to GeoSPARQL ETL")
    print("=" * 60)
    print(f"Input directory:   {args.input}")
    print(f"Output directory:  {args.output}")
    print(f"Compression:       {'Enabled (gzip)' if args.compress else 'Disabled'}")
    print(
        f"Parallel workers:  {args.workers or f'{max(1, cpu_count() - 1)} (auto)'}"
    )
    if args.start_from:
        print(f"Start from image:  {args.start_from}")
    print()

    success, errors, skipped, _ = process_cancer_type_directory(
        args.input,
        args.output,
        compress=args.compress,
        start_from_image=args.start_from,
        workers=args.workers,
    )

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Total success: {success} files")
    print(f"  Total skipped: {skipped} files (already processed)")
    print(f"  Total errors:  {errors} files")


if __name__ == "__main__":
    main()
