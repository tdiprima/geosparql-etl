# GeoJSON to GeoSPARQL ETL

A simple Python script to convert GeoJSON files into GeoSPARQL RDF (in Turtle format).

This script is designed for pathology applications, specifically for converting tissue classification data into a linked data format using SNOMED URIs.

## How It Works

The script processes each `.geojson` file from an input directory and performs the following steps:

1.  **Reads GeoJSON:** Loads polygon features and their associated properties.
2.  **Extracts Data:** Pulls out the geometry and tissue classification measurements.
3.  **Converts Geometry:** Transforms the GeoJSON polygon coordinates into WKT (`POLYGON (...)`) format.
4.  **Maps to SNOMED:** Assigns a SNOMED URI to the dominant tissue class based on probability scores.
5.  **Generates TTL:** Creates a `.ttl` file containing the data in GeoSPARQL format, complete with appropriate prefixes (`geo:`, `sno:`, `prov:`, etc.) and metadata.

## Usage

1.  Place your `.geojson` files into a directory named `geojson_files`.
2.  Run the script from the same directory:
    ```bash
    python geojson_to_geosparql_etl.py
    ```
3.  The converted `.ttl` files will be saved in the `geosparql_output` directory.

You can change the default input and output directories by editing the `INPUT_DIR` and `OUTPUT_DIR` variables in the script's `main()` function.

<br>
