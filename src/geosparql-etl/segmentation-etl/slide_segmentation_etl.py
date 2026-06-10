"""
ETL script to convert nuclear segmentation CSV files to GeoSPARQL format,
producing ONE integrated Turtle file per whole-slide image.

Unlike nuclear_segmentation_etl.py / single_cancer_etl.py (which emit one TTL
per patch CSV), this script merges every polygon from every patch of a single
.svs image into a single GeoSPARQL FeatureCollection. The downstream converter
then produces one spatially-indexed HDF5 file per image.

Polygon coordinates are written as-is (already in whole-slide pixel space); the
slide's width/height is the bounding box over all of its patches.

The input directory layout is unchanged from the per-patch scripts:

    input/
        coad_polygon/                      # Cancer type folder
            TCGA-*.svs.tar.gz/             # Slide archive folder
                coad_polygon/              # Inner polygon folder
                    TCGA-*.svs/            # SVS folder (one whole-slide image)
                        *-features.csv     # Patch CSVs

Output is a flat list, one file per image:

    output/
        coad_TCGA-A6-2671-...svs.ttl.gz
        coad_TCGA-A6-2674-...svs.ttl.gz
        ...


# Process one cancer type directory
python slide_segmentation_etl.py -i ${CVPR_DATA_DIR}/coad_polygon

# Process every cancer type under a base directory
python slide_segmentation_etl.py -i ${CVPR_DATA_DIR}

# Crank off just a few to eyeball, uncompressed, then stop
python slide_segmentation_etl.py -i .../coad_polygon -o ./coad_slides --no-compress --workers 8


# One cancer type
python slide_segmentation_etl.py -i ${DATA_DIR}/.../coad_polygon -o ./coad_slides

# Crank off a few to eyeball, then Ctrl-C (to spot-check first)
python slide_segmentation_etl.py -i ${DATA_DIR}/.../coad_polygon -o ./coad_slides --no-compress
"""

import argparse
from multiprocessing import cpu_count

try:
    import rich_argparse

    rich_argparse.RichHelpFormatter.styles["argparse.groups"] = "bold yellow"
except ImportError:
    rich_argparse = None

from etl_core import process_input_per_slide


def parse_args():
    """Parse command-line arguments."""
    formatter_class = (
        rich_argparse.RichHelpFormatter if rich_argparse else argparse.HelpFormatter
    )
    parser = argparse.ArgumentParser(
        description="Per-Slide Nuclear Segmentation to GeoSPARQL ETL - "
        "Merges all patches of each whole-slide image into one TTL file",
        formatter_class=formatter_class,
    )

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="A single *_polygon cancer type directory, OR a base directory "
        "containing several of them (e.g., .../cvpr-data or .../cvpr-data/coad_polygon)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="./nuclear_geosparql_slides",
        help="Output directory for merged per-slide TTL files "
        "(default: ./nuclear_geosparql_slides)",
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
        help=f"Number of parallel workers, one slide per worker "
        f"(default: CPU count - 1 = {max(1, cpu_count() - 1)})",
    )

    return parser.parse_args()


def main():
    """Main entry point: parse args, run the per-slide ETL, report totals."""
    args = parse_args()

    print("Per-Slide Nuclear Segmentation to GeoSPARQL ETL")
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

    success, errors, skipped = process_input_per_slide(
        args.input,
        args.output,
        compress=args.compress,
        start_from_image=args.start_from,
        workers=args.workers,
    )

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Slides merged:  {success} files")
    print(f"  Slides skipped: {skipped} files (already done or empty)")
    print(f"  Errors:         {errors} slides")


if __name__ == "__main__":
    main()
