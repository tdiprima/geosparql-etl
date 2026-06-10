## Nuclear Segmentation ETL: CSV to GeoSPARQL Converter

This script is a simple ETL (Extract, Transform, Load) tool to process nuclear segmentation data from CSV files. It converts polygon coordinates from the CSVs into the GeoSPARQL format, enriches the data with SNOMED CT classifications for nuclear material, and saves the output as compressed Turtle RDF files.

## Two Output Modes

The same input data can be converted two ways, depending on what the downstream HDF5 converter expects:

| Script | Output granularity | Use when |
| --- | --- | --- |
| `nuclear_segmentation_etl.py` / `single_cancer_etl.py` | **One TTL per patch** (a folder of small TTLs per slide) | You want each patch indexed separately. |
| `slide_segmentation_etl.py` | **One TTL per whole-slide image** (all patches merged) | You want one spatially-indexed HDF5 file per image. |

The per-patch and per-slide scripts share their core logic in `etl_core.py`. The sections below describe the per-patch scripts; see [Per-Slide Output](#per-slide-output-one-ttl-per-image) for the merged variant.

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

## Per-Slide Output (One TTL per Image)

Use `slide_segmentation_etl.py` when you need **all polygons for a whole-slide image integrated into a single Turtle file**, so the downstream converter produces one spatially-indexed HDF5 file per image. It reads the same input layout but merges every patch of an `.svs` image into one `geo:FeatureCollection`.

Polygon coordinates are written as-is (they are already in whole-slide pixel space). The slide's `width`/`height` is computed as the bounding box over all of its patches — `max(x + width)` and `max(y + height)` — and recorded as `exif:width`/`exif:height` and `hal:imageWidth`/`hal:imageHeight`, along with `hal:patchCount`.

### Usage

The `-i` input can be **either** a single `*_polygon` cancer type directory **or** a base directory containing several of them; the script auto-detects which.

```bash
# Process one cancer type
python slide_segmentation_etl.py -i /path/to/data/coad_polygon -o ./coad_slides

# Process every cancer type under a base directory
python slide_segmentation_etl.py -i /path/to/data -o ./slides

# Crank off a few uncompressed to eyeball, with a worker cap
python slide_segmentation_etl.py -i /path/to/data/coad_polygon -o ./coad_slides --no-compress --workers 8
```

It supports the same `--compress` / `--no-compress`, `--start-from`, and `--workers` flags as the per-patch scripts. Work is parallelized one slide per worker, output is skipped if it already exists (safe to resume), and each slide is written atomically (temp file + rename) so an interrupted run never leaves a half-written file.

### Output

A flat list, one file per image, named `<cancer_type>_<image_name>.ttl.gz`:

```
/path/to/output/
├── coad_TCGA-A6-2671-...svs.ttl.gz
├── coad_TCGA-A6-2674-...svs.ttl.gz
└── ...
```

<br>
