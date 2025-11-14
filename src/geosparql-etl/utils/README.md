# MongoDB Harvester Utilities

This package provides utility modules to simplify MongoDB ETL operations.

## Modules

### 1. `mongo_client.py` - MongoDB Connection Management

Provides context managers for clean connection handling.

**Basic Usage:**
```python
from utils import mongo_connection

# Simple context manager
with mongo_connection("mongodb://localhost:27018/", "camic") as db:
    results = db.analysis.find()
    for doc in results:
        print(doc)
# Connection automatically closed
```

**Class-based Usage:**
```python
from utils import MongoConnection

with MongoConnection("mongodb://localhost:27018/", "camic") as db:
    collection = db["mark"]
    for doc in collection.find():
        process(doc)
```

### 2. `config.py` - Configuration Management

Centralized configuration with environment variable support.

**Usage:**
```python
from utils import AppConfig, MongoConfig, ETLConfig

# Load from environment variables
config = AppConfig.from_env()
print(config.mongo.uri)
print(config.etl.batch_size)

# Or use directly
mongo_config = MongoConfig(
    uri="mongodb://localhost:27018/",
    db_name="camic"
)

etl_config = ETLConfig(
    batch_size=5000,
    output_dir="ttl_output"
)
```

**Environment Variables:**
```bash
export MONGO_URI="mongodb://localhost:27018/"
export MONGO_DB_NAME="camic"
export ETL_BATCH_SIZE=5000
export ETL_OUTPUT_DIR="output"
```

### 3. `checkpoint.py` - State Management

Track progress for resumable operations.

**Usage:**
```python
from utils import CheckpointManager

checkpoint = CheckpointManager("etl_checkpoint.json")

# Mark items as processed
checkpoint.mark_processed("exec123", "img456")

# Check if processed
if checkpoint.is_processed("exec123", "img456"):
    print("Already processed, skipping...")
else:
    process_item()
    checkpoint.mark_processed("exec123", "img456")

# Save checkpoint
checkpoint.save()

# Get statistics
stats = checkpoint.get_stats()
print(f"Processed: {stats['processed_count']}")
```

**Simple Checkpoint:**
```python
from utils import SimpleCheckpoint

checkpoint = SimpleCheckpoint("checkpoint.json")

for item in items:
    if item['id'] in checkpoint:
        continue

    process(item)
    checkpoint.add(item['id'])
    checkpoint.save()
```

### 4. `logger.py` - Logging Utilities

Consistent logging setup across scripts.

**Usage:**
```python
from utils import setup_logger, setup_etl_logger

# Basic logger with file and console output
logger = setup_logger("MyApp", log_file="app.log")
logger.info("Application started")
logger.error("An error occurred")

# ETL-specific logger
logger = setup_etl_logger("etl.log")
logger.info("Processing batch 1")
```

### 5. `serialization.py` - MongoDB JSON Encoding

Handle MongoDB types in JSON serialization.

**Usage:**
```python
from utils import (
    MongoJSONEncoder,
    clean_mongo_document,
    serialize_mongo_document,
    save_mongo_document_to_file
)
import json

# Use custom encoder
doc = db.collection.find_one()
json_str = json.dumps(doc, cls=MongoJSONEncoder)

# Clean document (converts ObjectId, datetime to strings)
clean_doc = clean_mongo_document(doc)
json.dumps(clean_doc)  # Now works with standard json

# Serialize to string
json_str = serialize_mongo_document(doc)

# Save to file
save_mongo_document_to_file(doc, "output.json")
```

### 6. `geometry.py` - Geometry Processing

Extract and process geometries from MongoDB documents.

**Usage:**
```python
from utils import (
    geometry_to_wkt,
    extract_geometry_from_mark,
    calculate_geometry_area
)

# Extract WKT from mark document
mark = db.mark.find_one()
wkt = geometry_to_wkt(mark)
print(f"WKT: {wkt}")

# Get Shapely geometry object
geom = extract_geometry_from_mark(mark)
print(f"Area: {geom.area}")

# Calculate area directly
area = calculate_geometry_area(mark)
print(f"Area: {area}")
```

### 7. `file_utils.py` - File Operations

Utilities for file and directory management.

**Usage:**
```python
from utils import (
    ensure_directory,
    generate_batch_filename,
    list_files_with_extension
)

# Create directory if it doesn't exist
output_dir = ensure_directory("ttl_output")

# Generate standardized filename
filename = generate_batch_filename(
    "ttl_output",
    "exec123",
    "img456",
    batch_num=1
)
# Returns: ttl_output/exec123_img456_batch1.ttl

# List all TTL files
ttl_files = list_files_with_extension("ttl_output", "ttl")
for file in ttl_files:
    print(file)
```

### 8. `rdf_utils.py` - RDF/Graph Utilities

Simplify RDF graph creation and manipulation.

**Usage:**
```python
from utils import (
    create_graph,
    create_mark_uri,
    add_wkt_geometry,
    add_provenance,
    serialize_graph,
    GEO, PROV, EX
)

# Create graph with namespaces
g = create_graph()

# Create URIs
mark_uri = create_mark_uri(mark["_id"])
analysis_uri = create_analysis_uri(analysis["_id"])

# Add geometry
wkt = geometry_to_wkt(mark)
add_wkt_geometry(g, mark_uri, wkt)

# Add provenance
add_provenance(g, mark_uri, activity_uri=analysis_uri)

# Serialize
serialize_graph(g, format="turtle", destination="output.ttl")
```

## Complete Example

Here's a complete example using multiple utilities:

```python
"""Complete ETL example using utilities."""
from utils import (
    mongo_connection,
    CheckpointManager,
    setup_etl_logger,
    geometry_to_wkt,
    create_graph,
    create_mark_uri,
    add_wkt_geometry,
    serialize_graph,
    ensure_directory
)

# Set up logging
logger = setup_etl_logger("etl_complete.log")

# Set up checkpoint
checkpoint = CheckpointManager("etl_checkpoint.json")

# Ensure output directory exists
ensure_directory("ttl_output")

# Connect to MongoDB
with mongo_connection("mongodb://localhost:27018/", "camic") as db:
    logger.info("Starting ETL process")

    for analysis_doc in db.analysis.find():
        exec_id = analysis_doc["analysis"]["execution_id"]
        img_id = analysis_doc["image"]["imageid"]

        # Check if already processed
        if checkpoint.is_processed(exec_id, img_id):
            logger.info(f"Skipping {exec_id}_{img_id} (already processed)")
            continue

        # Create RDF graph
        g = create_graph()

        # Query related marks
        marks = db.mark.find({
            "provenance.image.imageid": img_id,
            "provenance.analysis.execution_id": exec_id,
        })

        mark_count = 0
        for mark in marks:
            mark_uri = create_mark_uri(mark["_id"])

            # Extract and add geometry
            wkt = geometry_to_wkt(mark)
            if wkt:
                add_wkt_geometry(g, mark_uri, wkt)
                mark_count += 1

        # Save RDF graph
        output_file = f"ttl_output/{exec_id}_{img_id}.ttl"
        serialize_graph(g, format="turtle", destination=output_file)

        # Mark as processed
        checkpoint.mark_processed(exec_id, img_id)
        checkpoint.save()

        logger.info(f"Processed {exec_id}_{img_id}: {mark_count} marks")

    logger.info("ETL process completed")
```

## Benefits

1. **Automatic Resource Management**: Context managers ensure connections are properly closed
2. **Error Handling**: Built-in error handling and logging
3. **Reusability**: DRY principle - write once, use everywhere
4. **Type Safety**: Consistent handling of MongoDB types
5. **Resumability**: Easy checkpoint management for long-running processes
6. **Configuration**: Environment variable support for easy deployment
7. **Logging**: Consistent logging format across all scripts

## Migration Guide

### Before (old code):
```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018/")
db = client["camic"]

for mark in db.mark.find():
    geom = shape(mark["geometries"]["features"][0]["geometry"])
    wkt = geom.wkt
    print(wkt)

client.close()
```

### After (with utilities):
```python
from utils import mongo_connection, geometry_to_wkt

with mongo_connection("mongodb://localhost:27018/", "camic") as db:
    for mark in db.mark.find():
        wkt = geometry_to_wkt(mark)
        if wkt:
            print(wkt)
```

Benefits:
- Automatic connection cleanup
- Better error handling
- More concise code
- Handles missing geometry gracefully
