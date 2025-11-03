"""
ETL script to convert nuclear segmentation CSV files to GeoSPARQL format
with SNOMED URIs for nuclear material classification.

Processes CSV files from nuclear segmentation results with polygon coordinates
and converts them to Turtle/RDF format with GeoSPARQL geometries.
"""

import csv
import gzip
import hashlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path

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


def create_geosparql_ttl(csv_path, image_name, image_hash=None):
    """
    Convert nuclear segmentation CSV to GeoSPARQL TTL format.

    Args:
        csv_path: Path to CSV file (contains patch data)
        image_name: Name of the parent SVS image (from directory name)
        image_hash: SHA-256 hash of image (optional, generated from image_name if not provided)

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

    ttl_content += f"""<>      a                    geo:FeatureCollection;
        dc:creator           "http://orcid.org/0000-0003-4165-4062";
        dc:date              "{timestamp}"^^xsd:dateTime;
        dc:description       "Nuclear segmentation predictions for {image_name} - {patch_desc}";
        dc:publisher         <https://ror.org/01882y777> , <https://ror.org/05qghxh33>;
        dc:references        "https://doi.org/10.1038/s41597-020-0528-1";
        dc:title             "nuclear-segmentation-predictions";
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


def process_image_directories(input_base_dir, output_dir, compress=False):
    """
    Process directories of SVS images, where each directory contains CSV patch files.

    Directory structure:
        input_base_dir/
            TCGA-XX-XXXX-XXX-XX-XXX.UUID.svs/
                X_Y_WIDTH_HEIGHT_INFO-features.csv
                X_Y_WIDTH_HEIGHT_INFO-features.csv
                ...

    Args:
        input_base_dir: Base directory containing SVS image subdirectories
        output_dir: Directory for output TTL files
        compress: If True, gzip compress the output files
    """
    input_path = Path(input_base_dir)
    output_path = Path(output_dir)

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Find all subdirectories (SVS image directories)
    image_dirs = [d for d in input_path.iterdir() if d.is_dir()]

    if not image_dirs:
        print(f"No image directories found in {input_base_dir}")
        return

    print(f"Found {len(image_dirs)} image directories to process")

    total_success = 0
    total_error = 0

    for image_dir in image_dirs:
        image_name = image_dir.name
        print(f"\nProcessing image: {image_name}")

        # Get all CSV files in this image directory
        csv_files = list(image_dir.glob("*-features.csv"))

        if not csv_files:
            print(f"  ⚠ No CSV files found in {image_name}")
            continue

        print(f"  Found {len(csv_files)} patch CSV files")

        # Generate image hash once for all patches
        image_hash = get_image_hash(image_id=image_name)

        success_count = 0
        error_count = 0

        for csv_file in csv_files:
            try:
                # Convert to GeoSPARQL
                ttl_content = create_geosparql_ttl(csv_file, image_name, image_hash)

                # Write output file - use image_name as subdirectory
                image_output_dir = output_path / image_name
                image_output_dir.mkdir(parents=True, exist_ok=True)

                output_filename = csv_file.stem + ".ttl"
                if compress:
                    output_filename += ".gz"

                output_file = image_output_dir / output_filename

                if compress:
                    with gzip.open(output_file, "wt", encoding="utf-8") as f:
                        f.write(ttl_content)
                else:
                    with open(output_file, "w", encoding="utf-8") as f:
                        f.write(ttl_content)

                success_count += 1

            except Exception as e:
                print(f"    ✗ Error processing {csv_file.name}: {e}")
                error_count += 1

        print(f"  ✓ Processed {success_count} patches successfully")
        if error_count > 0:
            print(f"  ✗ {error_count} errors")

        total_success += success_count
        total_error += error_count

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"  Total success: {total_success} files")
    print(f"  Total errors: {total_error} files")


def main():
    """Main entry point for the ETL script."""

    # Configuration
    INPUT_BASE_DIR = "./nuclear_segmentation_data"  # Base dir with SVS subdirectories
    OUTPUT_DIR = "./nuclear_geosparql_output"  # Directory for output TTL files
    COMPRESS_OUTPUT = True  # Set to True to gzip compress output files

    print("Nuclear Segmentation to GeoSPARQL ETL Converter")
    print("=" * 60)
    print(f"Input base directory:  {INPUT_BASE_DIR}")
    print(f"Output directory:      {OUTPUT_DIR}")
    print(
        f"Compression:           {'Enabled (gzip)' if COMPRESS_OUTPUT else 'Disabled'}"
    )
    print()

    # Test mode for single file
    test_mode = True

    if test_mode:
        # Test with the example file
        test_file = "24001_72001_4000_4000_0.2325_1-features.csv"
        test_image_name = (
            "TCGA-05-4245-01Z-00-DX1.36ff5403-d4bb-4415-b2c5-7c750d655cde.svs"
        )
        test_path = Path(test_file)

        if test_path.exists():
            print(f"Testing with: {test_file}")
            print(f"Image name: {test_image_name}")

            ttl_content = create_geosparql_ttl(test_path, test_image_name)

            # Save test output
            output_path = Path(OUTPUT_DIR)
            output_path.mkdir(parents=True, exist_ok=True)

            output_file = output_path / (test_path.stem + "_test.ttl")

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(ttl_content)

            print(f"Test output saved to: {output_file}")
            print(f"\nGenerated {ttl_content.count('rdfs:member')} features")
            print("\nFirst 2000 characters of output:")
            print(ttl_content[:2000])
        else:
            print(f"Test file not found: {test_file}")
            print("Set test_mode = False to process directories")
    else:
        # Process all image directories
        process_image_directories(INPUT_BASE_DIR, OUTPUT_DIR, compress=COMPRESS_OUTPUT)


if __name__ == "__main__":
    main()
