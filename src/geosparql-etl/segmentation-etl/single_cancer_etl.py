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
NUCLEAR_MATERIAL_SNOMED = "http://snomed.info/id/68841002"


def parse_polygon_to_wkt(polygon_string):
    """Convert polygon string format (x1:y1:x2:y2:...) to WKT format."""
    if not polygon_string:
        return None

    coords = polygon_string.strip("[]").split(":")

    wkt_coords = []
    for i in range(0, len(coords), 2):
        if i + 1 < len(coords):
            wkt_coords.append(f"{coords[i]} {coords[i + 1]}")

    if len(wkt_coords) > 0 and wkt_coords[0] != wkt_coords[-1]:
        wkt_coords.append(wkt_coords[0])

    return f"POLYGON (({', '.join(wkt_coords)}))"


def extract_image_info_from_filename(filename):
    """
    Extract image metadata from filename.
    Format: X_Y_WIDTH_HEIGHT_OTHER_INFO-features.csv
    """
    name = filename.replace(".csv", "")
    parts = name.split("_")

    if len(parts) >= 4:
        from contextlib import suppress

        with suppress(ValueError):
            return {
                "x": int(parts[0]),
                "y": int(parts[1]),
                "width": int(parts[2]),
                "height": int(parts[3]),
                "base_name": name,
            }

    return {
        "x": 0,
        "y": 0,
        "width": 40000,
        "height": 40000,
        "base_name": name,
    }


def get_image_hash(image_path=None, image_id=None):
    """Generate SHA-256 hash for image identification."""
    if image_path and Path(image_path).exists():
        try:
            result = subprocess.run(
                ["sha256sum", str(image_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.split()[0]
        except (subprocess.CalledProcessError, FileNotFoundError):
            sha256_hash = hashlib.sha256()
            with open(image_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()

    if image_id:
        return hashlib.sha256(image_id.encode()).hexdigest()

    return hashlib.sha256(b"unknown").hexdigest()


def create_geosparql_ttl(csv_path, image_name, image_hash=None, cancer_type=None):
    """Convert nuclear segmentation CSV to GeoSPARQL TTL format."""
    csv_path = Path(csv_path)
    patch_info = extract_image_info_from_filename(csv_path.name)

    if image_hash is None:
        image_hash = get_image_hash(image_id=image_name)

    timestamp = datetime.now(tz=timezone.utc).isoformat()

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

    ttl_content += f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_name}" .

"""

    patch_desc = f"patch {patch_info['x']}_{patch_info['y']} ({patch_info['width']}x{patch_info['height']})"

    ttl_content += f"""<>      a                    geo:FeatureCollection;
        dc:creator           "${CREATOR_ORCID}";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - {patch_desc}";
        dc:publisher         <${PUBLISHER_ROR_1}> , <${PUBLISHER_ROR_2}>;
        dc:references        "${DATASET_DOI}";
        dc:title             "nuclear-segmentation-predictions";"""

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
                             ];
"""

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

            wkt = parse_polygon_to_wkt(polygon_str)
            if not wkt:
                continue

            if feature_count > 0:
                ttl_content += ";\n"

            ttl_content += f"""        rdfs:member          [ a                   geo:Feature;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}"^^geo:wktLiteral ];
                               hal:classification  sno:{snomed_id};
                               hal:measurement     [ hal:hasProbability  "1.0"^^xsd:float ]"""

            if area_pixels:
                ttl_content += f""";
                               hal:areaInPixels    "{area_pixels}"^^xsd:int"""

            if physical_size:
                ttl_content += f""";
                               hal:physicalSize    "{physical_size}"^^xsd:float"""

            ttl_content += """
                             ]"""
            feature_count += 1

    ttl_content += " .\n"
    return ttl_content


def process_single_csv(
    csv_file, image_name, image_hash, cancer_type, prefix, output_path, compress
):
    """Process a single CSV file - designed to be called in parallel."""
    try:
        image_output_dir = output_path / image_name
        output_filename = prefix + csv_file.stem + ".ttl"
        if compress:
            output_filename += ".gz"
        output_file = image_output_dir / output_filename

        if output_file.exists():
            return ("skipped", csv_file.name)

        ttl_content = create_geosparql_ttl(
            csv_file, image_name, image_hash, cancer_type
        )

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
