"""
Merge per-tile nuclear segmentation Turtle (.ttl.gz) files into a single
Turtle file per whole-slide image.

Each image folder (named like TCGA-*.svs) contains many tile-level .ttl.gz
files. This script:
  1. Reads every tile file in the folder
  2. Extracts all rdfs:member feature blocks (the polygon data)
  3. Computes the overall image bounding box from tile coordinates
  4. Writes a single merged .ttl.gz with one FeatureCollection containing
     every polygon from every tile

Usage:
    # Merge one image folder
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output

    # Merge all image folders under a parent directory
    python merge_tiles.py -i /path/to/parent_with_many_image_folders -o /path/to/output --all

    # Merge all with parallel image processing (4 images at once)
    python merge_tiles.py -i /path/to/parent -o /path/to/output --all -w 4

    # Dry run (just show what would be merged, don't write)
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output --dry-run

    # No compression
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output --no-compress

    # Fast compression (level 1 — ~4x faster writes, slightly larger files)
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output --fast
"""

import argparse
import gzip
import re
import sys
from contextlib import suppress
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from pathlib import Path

# SNOMED URI for nuclear material (nucleoplasm) - same as ETL script
NUCLEAR_MATERIAL_SNOMED_ID = "68841002"


def extract_tile_info_from_filename(filename):
    """
    Extract tile X, Y, Width, Height from a tile filename.
    Format: blca_X_Y_WIDTH_HEIGHT_OTHER-features.ttl.gz
    Example: blca_100001_12001_4000_4000_0.2485_1-features.ttl.gz

    The prefix (cancer type) varies, so we strip it first.
    """
    # Remove extensions
    name = filename
    for ext in (".gz", ".ttl"):
        name = name.removesuffix(ext)

    # Pattern: PREFIX_X_Y_WIDTH_HEIGHT_REST-features
    # The prefix is the cancer type (e.g., "blca"), then underscore-separated numbers
    # We need to find the numeric parts after the cancer type prefix
    parts = name.split("_")

    # Find the first part that looks like a number — that's the start of X_Y_W_H
    num_start = None
    for i, part in enumerate(parts):
        try:
            int(part)
            num_start = i
            break
        except ValueError:
            continue

    if num_start is not None and len(parts) >= num_start + 4:
        with suppress(ValueError):
            return {
                "x": int(parts[num_start]),
                "y": int(parts[num_start + 1]),
                "width": int(parts[num_start + 2]),
                "height": int(parts[num_start + 3]),
            }

    return None


def extract_raw_feature_block(content):
    """
    Extract the entire rdfs:member block from a TTL file as a raw string.

    Instead of parsing individual features (which requires character-by-character
    bracket matching), this slices out the complete feature block in one operation.
    ~430x faster than per-feature extraction.

    Returns the raw string from the first 'rdfs:member' (including leading
    whitespace) to just before the closing ' .', or None if no features found.
    """
    first_member = content.find("rdfs:member")
    if first_member == -1:
        return None

    # Back up to include the leading whitespace on the same line
    line_start = content.rfind("\n", 0, first_member) + 1

    # Find the closing ' .\n' that terminates the FeatureCollection
    last_dot = content.rfind(" .\n")
    if last_dot == -1:
        last_dot = content.rfind(" .")
    if last_dot == -1 or last_dot < first_member:
        return None

    return content[line_start:last_dot]


def count_features_in_block(block):
    """Count the number of rdfs:member entries in a raw feature block."""
    return block.count("rdfs:member")


def extract_cancer_type(filename):
    """Extract cancer type prefix from filename like 'blca_100001_...'"""
    parts = filename.split("_")
    # The cancer type is everything before the first numeric part
    cancer_parts = []
    for part in parts:
        try:
            int(part)
            break
        except ValueError:
            cancer_parts.append(part)
    return "_".join(cancer_parts) if cancer_parts else "unknown"


def extract_image_name_from_ttl(content):
    """
    Extract the image name (dc:identifier) from a TTL file.
    Looks for: dc:identifier "TCGA-....." .
    """
    match = re.search(r'dc:identifier\s+"([^"]+)"', content)
    if match:
        return match.group(1)
    return None


def extract_image_hash_from_ttl(content):
    """
    Extract the image hash (urn:sha256:...) from a TTL file.
    """
    match = re.search(r"<urn:sha256:([a-f0-9]+)>", content)
    if match:
        return match.group(1)
    return None


def compute_bounding_box(tile_infos):
    """
    Compute the overall bounding box from all tile coordinates.
    Returns (max_width, max_height) representing the full image dimensions.
    """
    max_x_extent = 0
    max_y_extent = 0

    for info in tile_infos:
        x_extent = info["x"] + info["width"]
        y_extent = info["y"] + info["height"]
        max_x_extent = max(max_x_extent, x_extent)
        max_y_extent = max(max_y_extent, y_extent)

    return max_x_extent, max_y_extent


def read_ttl_file(filepath):
    """Read a .ttl or .ttl.gz file and return its text content."""
    filepath = Path(filepath)
    if filepath.name.endswith(".gz"):
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            return f.read()
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()


def merge_image_tiles(image_dir, output_dir, compress=True, dry_run=False,
                      compresslevel=6):
    """
    Merge all tile .ttl.gz files in an image directory into a single TTL file.

    Uses raw string slicing to extract feature blocks (~430x faster than
    per-feature bracket parsing) and streams directly to output to minimize
    memory usage.

    Args:
        image_dir: Path to directory containing tile .ttl.gz files
        output_dir: Path to output directory
        compress: Whether to gzip the output
        dry_run: If True, just report what would happen
        compresslevel: Gzip compression level (1=fastest, 9=smallest, default 6)

    Returns:
        (status, message) tuple
    """
    image_dir = Path(image_dir)
    output_dir = Path(output_dir)

    # Find all tile TTL files
    tile_files = sorted(
        list(image_dir.glob("*.ttl.gz")) + list(image_dir.glob("*.ttl"))
    )

    if not tile_files:
        return ("skip", f"No .ttl or .ttl.gz files found in {image_dir.name}")

    if dry_run:
        return (
            "dry_run",
            f"Would merge {len(tile_files)} tiles from {image_dir.name}",
        )

    # Use the folder name as the image name, but also try to get it from TTL content
    folder_name = image_dir.name  # e.g., TCGA-YF-AA3M-...svs

    # Read the first tile to extract metadata
    first_content = read_ttl_file(tile_files[0])
    image_name = extract_image_name_from_ttl(first_content) or folder_name
    image_hash = extract_image_hash_from_ttl(first_content)
    cancer_type = extract_cancer_type(tile_files[0].name)

    # Phase 1: Read all tiles — extract raw feature blocks and tile info
    # We stream-write to avoid holding all features in memory at once,
    # but we do need tile_infos from all files first for the header.
    tile_infos = []
    total_files = len(tile_files)

    # First pass: just get tile coordinates from filenames (instant, no I/O)
    for tile_file in tile_files:
        tile_info = extract_tile_info_from_filename(tile_file.name)
        if tile_info:
            tile_infos.append(tile_info)

    # Compute overall image dimensions from tile bounding box
    if tile_infos:
        img_width, img_height = compute_bounding_box(tile_infos)
    else:
        img_width, img_height = 0, 0

    # Build the merged TTL header
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    # Build image object and provenance differently depending on hash availability
    if image_hash:
        image_object_ttl = f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_name}";
        exif:width   "{img_width}"^^xsd:int;
        exif:height  "{img_height}"^^xsd:int ."""
        prov_ttl = f"""        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:{image_hash}>;
                             ];"""
    else:
        image_object_ttl = f"""<urn:image:{image_name}>
        a            so:ImageObject;
        dc:identifier "{image_name}";
        exif:width   "{img_width}"^^xsd:int;
        exif:height  "{img_height}"^^xsd:int;
        hal:hashMissing       "true"^^xsd:boolean ."""
        prov_ttl = f"""        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:image:{image_name}>;
                             ];"""

    ttl_header = f"""@prefix dc:   <http://purl.org/dc/terms/> .
@prefix exif: <http://www.w3.org/2003/12/exif/ns#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix hal:  <https://halcyon.is/ns/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sno:  <http://snomed.info/id/> .
@prefix so:   <https://schema.org/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

{image_object_ttl}

<>      a                    geo:FeatureCollection;
        dc:creator           "${CREATOR_ORCID}";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - merged from {total_files} tiles";
        dc:publisher         <${PUBLISHER_ROR_1}> , <${PUBLISHER_ROR_2}>;
        dc:references        "${DATASET_DOI}";
        dc:title             "nuclear-segmentation-predictions";
        hal:cancerType       "{cancer_type}";
        hal:imageWidth       "{img_width}"^^xsd:int;
        hal:imageHeight      "{img_height}"^^xsd:int;
        hal:tileCount        "{total_files}"^^xsd:int;
{prov_ttl}
"""

    # Phase 2: Stream-write — read each tile, extract raw block, write immediately.
    # This avoids holding all features in memory.
    output_dir.mkdir(parents=True, exist_ok=True)
    output_filename = f"{cancer_type}_{image_name}.ttl"
    if compress:
        output_filename += ".gz"
    output_file = output_dir / output_filename

    if compress:
        out_f = gzip.open(output_file, "wt", encoding="utf-8",
                          compresslevel=compresslevel)
    else:
        out_f = open(output_file, "w", encoding="utf-8")

    feature_count = 0
    tiles_written = 0

    try:
        out_f.write(ttl_header)

        for idx, tile_file in enumerate(tile_files, 1):
            if idx % 50 == 0 or idx == total_files:
                print(f"    Processing tile {idx}/{total_files}...", flush=True)

            try:
                content = read_ttl_file(tile_file)
            except Exception as e:
                print(f"    WARNING: Could not read {tile_file.name}: {e}")
                continue

            # Extract the raw feature block (single string slice — very fast)
            raw_block = extract_raw_feature_block(content)
            if not raw_block:
                continue

            # Join with previous tile's features using semicolon separator
            if tiles_written > 0:
                out_f.write(";\n")

            out_f.write(raw_block)
            feature_count += count_features_in_block(raw_block)
            tiles_written += 1

        # Close the FeatureCollection
        if tiles_written > 0:
            out_f.write(" .\n")

    finally:
        out_f.close()

    if tiles_written == 0:
        # Clean up empty file
        output_file.unlink(missing_ok=True)
        return ("skip", f"No features found in any tiles for {image_dir.name}")

    return (
        "success",
        f"Merged {total_files} tiles → {output_filename} "
        f"({feature_count:,} features, {img_width}x{img_height}px)",
    )


def _merge_worker(args):
    """Worker function for parallel image processing."""
    image_dir, output_path, compress, compresslevel = args
    try:
        status, message = merge_image_tiles(
            image_dir, output_path, compress=compress, compresslevel=compresslevel
        )
        return (image_dir.name, status, message)
    except Exception as e:
        return (image_dir.name, "error", str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Merge per-tile nuclear segmentation TTL files into one file per image"
    )

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input directory: either a single image folder (containing .ttl.gz files) "
        "or a parent directory containing multiple image folders (use with --all)",
    )

    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output directory for merged TTL files",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all subdirectories in the input directory as separate images",
    )

    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Don't gzip compress output (default: compress)",
    )

    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use fastest gzip compression (level 1 instead of 6). "
        "~4x faster writes, ~40%% larger output files",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just show what would be merged without writing files",
    )

    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of images to process in parallel (default: 1). "
        f"With {cpu_count()} cores available, try -w {max(1, cpu_count() // 2)}",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    compress = not args.no_compress
    compresslevel = 1 if args.fast else 6

    if not input_path.exists():
        print(f"ERROR: Input path does not exist: {input_path}")
        sys.exit(1)

    print("Nuclear Segmentation Tile Merger")
    print("=" * 60)
    print(f"Input:       {input_path}")
    print(f"Output:      {output_path}")
    print(f"Compression: {'gzip' if compress else 'none'}"
          + (f" (level {compresslevel})" if compress else ""))
    print(f"Mode:        {'dry run' if args.dry_run else 'merge'}")
    if args.all:
        print(f"Workers:     {args.workers}")
    print()

    if args.all:
        # Process all subdirectories as separate images
        image_dirs = sorted(
            [d for d in input_path.iterdir() if d.is_dir()]
        )

        if not image_dirs:
            print(f"No subdirectories found in {input_path}")
            sys.exit(1)

        print(f"Found {len(image_dirs)} image directories")
        print()

        success_count = 0
        skip_count = 0
        error_count = 0

        if args.dry_run or args.workers <= 1:
            # Sequential processing
            for image_dir in image_dirs:
                print(f"  Processing: {image_dir.name}")
                try:
                    status, message = merge_image_tiles(
                        image_dir, output_path, compress=compress,
                        dry_run=args.dry_run, compresslevel=compresslevel
                    )
                    print(f"    {status}: {message}")

                    if status == "success":
                        success_count += 1
                    elif status in ("skip", "dry_run"):
                        skip_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    print(f"    ERROR: {e}")
                    error_count += 1
        else:
            # Parallel processing across images
            work_items = [
                (d, output_path, compress, compresslevel)
                for d in image_dirs
            ]

            with Pool(processes=args.workers) as pool:
                for dir_name, status, message in pool.imap_unordered(
                    _merge_worker, work_items
                ):
                    print(f"  {dir_name}")
                    print(f"    {status}: {message}")

                    if status == "success":
                        success_count += 1
                    elif status in ("skip", "dry_run"):
                        skip_count += 1
                    else:
                        error_count += 1

        print()
        print("=" * 60)
        print(f"Done! Success: {success_count}, Skipped: {skip_count}, Errors: {error_count}")

    else:
        # Process single image directory
        print(f"Processing: {input_path.name}")
        try:
            status, message = merge_image_tiles(
                input_path, output_path, compress=compress,
                dry_run=args.dry_run, compresslevel=compresslevel
            )
            print(f"  {status}: {message}")
        except Exception as e:
            print(f"  ERROR: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
