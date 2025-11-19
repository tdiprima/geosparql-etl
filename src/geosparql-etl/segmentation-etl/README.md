# Nuclear Segmentation ETL: CSV to GeoSPARQL Converter

This script is a simple ETL (Extract, Transform, Load) tool to process nuclear segmentation data from CSV files. It converts polygon coordinates from the CSVs into the GeoSPARQL format, enriches the data with SNOMED CT classifications for nuclear material, and saves the output as compressed Turtle RDF files.

## Key Features

*   **Converts CSV to GeoSPARQL**: Transforms segmentation data into linked data using the GeoSPARQL ontology.
*   **Parallel Processing**: Can run on multiple CPU cores to process large datasets quickly.
*   **Resumption Support**: If a job is interrupted, it can be resumed from where it left off, skipping already-completed files.
*   **Standardized Classification**: Uses the SNOMED CT URI for "Nucleoplasm" (`68841002`) for classification.
*   **Compressed Output**: Automatically compresses the output Turtle files using Gzip (`.ttl.gz`).

## Input Directory Structure

The script expects a specific directory layout for the input data. It navigates through folders organized by cancer type and slide name to find the CSV patch files.

```
/path/to/input/
└── blca_polygon/                  # Cancer type (e.g., "blca")
    └── TCGA-XYZ.svs.tar.gz/       # Slide archive directory
        └── blca_polygon/
            └── TCGA-XYZ.svs/      # Slide-specific directory
                ├── patch1-features.csv
                ├── patch2-features.csv
                └── ...
```

## Usage

You can run the script from the command line with several options.

### Basic Execution

This command processes all data in the default input directory and saves it to the default output directory.

```bash
python nuclear_segmentation_etl.py
```

### Specify Directories

Provide custom input and output paths.

```bash
python nuclear_segmentation_etl.py \
  --input /path/to/your/data \
  --output /path/to/your/output
```

### Use Parallel Workers

To speed up processing, specify the number of CPU cores to use. This is highly recommended for large datasets.

```bash
# Use all available cores
python nuclear_segmentation_etl.py --workers $(nproc)

# Or specify a number
python nuclear_segmentation_etl.py --workers 32
```

### Resume a Job

If the script stops, you can resume it from a specific slide image. The script will skip all images alphabetically before the one you specify.

```bash
python nuclear_segmentation_etl.py --start-from "TCGA-A7-A0CD-01Z-00-DX1.svs"
```

## Output

The script generates a corresponding output directory for each slide, containing the converted Turtle files, which are compressed by default.

```
/path/to/output/
└── TCGA-XYZ.svs/
    ├── blca_patch1-features.ttl.gz
    ├── blca_patch2-features.ttl.gz
    └── ...
```

<br>
