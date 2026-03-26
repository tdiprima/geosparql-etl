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

    # Dry run (just show what would be merged, don't write)
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output --dry-run

    # No compression
    python merge_tiles.py -i /path/to/image_folder -o /path/to/output --no-compress
"""

import argparse
import gzip
import re
import sys
from contextlib import suppress
from datetime import datetime, timezone
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


def extract_features_from_ttl(content):
    """
    Extract all rdfs:member blocks from a TTL file's content.

    Returns a list of feature strings (each is a complete rdfs:member block
    WITHOUT the leading 'rdfs:member' or trailing semicolon/dot).
    """
    features = []

    # Split on 'rdfs:member' to get each feature block
    # The first split will be the header, subsequent ones are features
    parts = content.split("rdfs:member")

    for i, part in enumerate(parts):
        if i == 0:
            # This is the header before the first feature — skip
            continue

        # Each feature block looks like:
        #   <whitespace>[ a  geo:Feature;
        #       ...
        #   ] ;   (or  ] .  for the last one)
        #
        # We need to extract the bracket content

        # Find the opening bracket
        bracket_start = part.find("[")
        if bracket_start == -1:
            continue

        # Find the matching closing bracket — we need to count nesting
        depth = 0
        bracket_end = None
        for j in range(bracket_start, len(part)):
            if part[j] == "[":
                depth += 1
            elif part[j] == "]":
                depth -= 1
                if depth == 0:
                    bracket_end = j
                    break

        if bracket_end is not None:
            # Extract the full bracket content including brackets
            feature_block = part[bracket_start : bracket_end + 1]
            features.append(feature_block)

    return features


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


def merge_image_tiles(image_dir, output_dir, compress=True, dry_run=False):
    """
    Merge all tile .ttl.gz files in an image directory into a single TTL file.

    Args:
        image_dir: Path to directory containing tile .ttl.gz files
        output_dir: Path to output directory
        compress: Whether to gzip the output
        dry_run: If True, just report what would happen

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

    # Process all tile files: extract features and tile info
    all_features = []
    tile_infos = []
    total_files = len(tile_files)

    for idx, tile_file in enumerate(tile_files, 1):
        if idx % 50 == 0 or idx == total_files:
            print(f"    Reading tile {idx}/{total_files}...", flush=True)

        try:
            content = read_ttl_file(tile_file)
        except Exception as e:
            print(f"    WARNING: Could not read {tile_file.name}: {e}")
            continue

        # Extract tile coordinates from filename
        tile_info = extract_tile_info_from_filename(tile_file.name)
        if tile_info:
            tile_infos.append(tile_info)

        # Extract feature blocks
        features = extract_features_from_ttl(content)
        all_features.extend(features)

    if not all_features:
        return ("skip", f"No features found in any tiles for {image_dir.name}")

    # Compute overall image dimensions from tile bounding box
    if tile_infos:
        img_width, img_height = compute_bounding_box(tile_infos)
    else:
        img_width, img_height = 0, 0

    # Build the merged TTL
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

    # Write output using streaming to handle large files
    output_dir.mkdir(parents=True, exist_ok=True)
    output_filename = f"{cancer_type}_{image_name}.ttl"
    if compress:
        output_filename += ".gz"
    output_file = output_dir / output_filename

    open_func = gzip.open if compress else open
    mode = "wt" if compress else "w"

    with open_func(output_file, mode, encoding="utf-8") as f:
        f.write(ttl_header)

        for i, feature in enumerate(all_features):
            if i > 0:
                f.write(";\n")
            f.write(f"        rdfs:member          {feature}")

        # Close the FeatureCollection
        f.write(" .\n")

    feature_count = len(all_features)
    return (
        "success",
        f"Merged {total_files} tiles → {output_filename} "
        f"({feature_count:,} features, {img_width}x{img_height}px)",
    )


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
        "--dry-run",
        action="store_true",
        help="Just show what would be merged without writing files",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    compress = not args.no_compress

    if not input_path.exists():
        print(f"ERROR: Input path does not exist: {input_path}")
        sys.exit(1)

    print("Nuclear Segmentation Tile Merger")
    print("=" * 60)
    print(f"Input:       {input_path}")
    print(f"Output:      {output_path}")
    print(f"Compression: {'gzip' if compress else 'none'}")
    print(f"Mode:        {'dry run' if args.dry_run else 'merge'}")
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

        for image_dir in image_dirs:
            print(f"  Processing: {image_dir.name}")
            try:
                status, message = merge_image_tiles(
                    image_dir, output_path, compress=compress, dry_run=args.dry_run
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

        print()
        print("=" * 60)
        print(f"Done! Success: {success_count}, Skipped: {skip_count}, Errors: {error_count}")

    else:
        # Process single image directory
        print(f"Processing: {input_path.name}")
        try:
            status, message = merge_image_tiles(
                input_path, output_path, compress=compress, dry_run=args.dry_run
            )
            print(f"  {status}: {message}")
        except Exception as e:
            print(f"  ERROR: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
