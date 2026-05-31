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
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

from etl_core import get_image_hash, process_single_csv


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
    """
    input_path = Path(cancer_type_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        print(f"Error: directory not found: {cancer_type_dir}")
        return

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
        return

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

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Total success: {total_success} files")
    print(f"  Total skipped: {total_skipped} files (already processed)")
    print(f"  Total errors: {total_error} files")


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

    process_cancer_type_directory(
        args.input,
        args.output,
        compress=args.compress,
        start_from_image=args.start_from,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
