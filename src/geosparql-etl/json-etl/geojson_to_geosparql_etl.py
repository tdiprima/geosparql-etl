"""
ETL script to convert GeoJSON files to GeoSPARQL format with SNOMED URIs
for pathology tissue classifications.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

# SNOMED URI mappings for tissue classes
SNOMED_MAPPINGS = {
    "400p-Acinar tissue": "http://snomed.info/id/73681006",
    "400p-Dysplastic epithelium": "http://snomed.info/id/61313004",
    "400p-Fibrosis": "http://snomed.info/id/112674009",
    "400p-Lymph Aggregates": "http://snomed.info/id/267190001",
    "400p-Necrosis": "http://snomed.info/id/6574001",
    "400p-Nerves": "http://snomed.info/id/88545005",
    "400p-Normal ductal epithelium": "http://snomed.info/id/27834005",
    "400p-Reactive": "http://snomed.info/id/11214006",
    "400p-Stroma": "http://snomed.info/id/128752000",
    "400p-Tumor": "http://snomed.info/id/108369006",  # Neoplasm
}


def polygon_to_wkt(coordinates):
    """Convert GeoJSON polygon coordinates to WKT format."""
    if not coordinates or not coordinates[0]:
        return None

    # GeoJSON coordinates are in [x, y] format
    # Need to convert to WKT: POLYGON ((x1 y1, x2 y2, ...))
    ring = coordinates[0]  # Get the outer ring
    wkt_coords = []

    for coord in ring:
        x, y = coord[0], coord[1]
        wkt_coords.append(f"{x} {y}")

    wkt = f"POLYGON (({', '.join(wkt_coords)}))"
    return wkt


def get_dominant_class(measurements):
    """Determine the dominant tissue class based on probability measurements."""
    if not measurements:
        return None, 0.0

    max_prob = 0.0
    dominant_class = None

    for key, value in measurements.items():
        if key.startswith("prob_"):
            class_name = key.replace("prob_", "")
            if value > max_prob:
                max_prob = value
                dominant_class = class_name

    return dominant_class, max_prob


def extract_image_id(filename):
    """Extract the image ID from the filename."""
    # Pattern: TCGA-XX-XXXX-XXX-XX-XXX.UUID.geojson
    parts = filename.replace(".geojson", "").split(".")
    if len(parts) >= 2:
        return parts[0]  # Return the TCGA ID part
    return filename.replace(".geojson", "")


def generate_image_hash(image_id):
    """Generate SHA-256 hash for image URN."""
    return hashlib.sha256(image_id.encode()).hexdigest()


def create_geosparql_ttl(geojson_data, filename, output_dir):
    """Convert GeoJSON to GeoSPARQL TTL format."""

    image_id = extract_image_id(filename)
    image_hash = generate_image_hash(image_id)
    # timestamp = datetime.utcnow().isoformat() + "Z"
    timestamp = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

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

    # Add image object
    ttl_content += f"""<urn:sha256:{image_hash}>
        a            so:ImageObject;
        dc:identifier "{image_id}" ;
        exif:height  "40000"^^xsd:int;
        exif:width   "40000"^^xsd:int .

"""

    # Start feature collection with <> as the subject (self-reference)
    ttl_content += (
        """<>      a                    geo:FeatureCollection;
        dc:creator           "http://orcid.org/0000-0003-4165-4062";
        dc:date              \""""
        + timestamp
        + """\"^^xsd:dateTime;
        dc:description       "Raj's 10-class classification results produced via wsinfer and Tammy's PyTorch model for """
        + image_id
        + """";
        dc:title             "tissue-classification-predictions";
        prov:wasGeneratedBy  [ a                       prov:Activity;
                               prov:used               <urn:sha256:"""
        + image_hash
        + """>;
                             ];
"""
    )

    # Process features
    features = geojson_data.get("features", [])
    feature_count = 0

    for feature in features:
        geometry = feature.get("geometry", {})
        properties = feature.get("properties", {})
        measurements = properties.get("measurements", {})

        # Get dominant tissue class
        dominant_class, probability = get_dominant_class(measurements)

        if dominant_class and dominant_class in SNOMED_MAPPINGS:
            # Convert coordinates to WKT
            coordinates = geometry.get("coordinates", [])
            wkt = polygon_to_wkt(coordinates)

            if wkt:
                # Get SNOMED URI for the class
                snomed_uri = SNOMED_MAPPINGS[dominant_class]
                snomed_id = snomed_uri.split("/")[-1]

                # Add separator for multiple features
                if feature_count > 0:
                    ttl_content += ";\n"

                # Add feature with proper indentation for <> subject
                ttl_content += f"""        rdfs:member          [ a                   geo:Feature;
                               geo:hasGeometry     [ geo:asWKT  "{wkt}"^^geo:wktLiteral ];
                               hal:classification  sno:{snomed_id};
                               hal:measurement     """

                # Add measurements for all classes
                measurement_count = 0
                for key, value in measurements.items():
                    if key.startswith("prob_"):
                        class_name = key.replace("prob_", "")
                        if class_name in SNOMED_MAPPINGS:
                            class_snomed = SNOMED_MAPPINGS[class_name].split("/")[-1]

                            if measurement_count > 0:
                                ttl_content += ","

                            ttl_content += f"""
                                             [ hal:classification  sno:{class_snomed};
                                               hal:hasProbability  "{value:.6f}"^^xsd:float
                                             ]"""

                            measurement_count += 1

                ttl_content += "\n                             ]"
                feature_count += 1

    # Close the feature collection with proper terminator
    ttl_content += " .\n"

    return ttl_content


def process_directory(input_dir, output_dir):
    """Process all GeoJSON files in the input directory."""

    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Get all GeoJSON files
    geojson_files = list(Path(input_dir).glob("*.geojson"))

    if not geojson_files:
        print(f"No GeoJSON files found in {input_dir}")
        return

    print(f"Found {len(geojson_files)} GeoJSON files to process")

    success_count = 0
    error_count = 0

    for geojson_path in geojson_files:
        try:
            print(f"Processing: {geojson_path.name}")

            # Read GeoJSON file
            with open(geojson_path, "r") as f:
                geojson_data = json.load(f)

            # Convert to GeoSPARQL
            ttl_content = create_geosparql_ttl(
                geojson_data, geojson_path.name, output_dir
            )

            # Write output file
            output_filename = geojson_path.stem + ".ttl"
            output_path = Path(output_dir) / output_filename

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(ttl_content)

            print(f"  ✓ Created: {output_filename}")
            success_count += 1

        except Exception as e:
            print(f"  ✗ Error processing {geojson_path.name}: {e}")
            error_count += 1

    print("\nProcessing complete!")
    print(f"  Success: {success_count} files")
    print(f"  Errors: {error_count} files")


def main():
    """Main entry point for the ETL script."""

    # Configuration
    INPUT_DIR = "./geojson_files"  # Directory containing your 471 GeoJSON files
    OUTPUT_DIR = "./geosparql_output"  # Directory for output TTL files

    print("GeoJSON to GeoSPARQL ETL Converter")
    print("===================================")
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    process_directory(INPUT_DIR, OUTPUT_DIR)


if __name__ == "__main__":
    main()
