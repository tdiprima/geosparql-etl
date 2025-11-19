## Turtle File Summary

### Structure per File
Each `.ttl.gz` file contains:

- **1 Feature Collection** (the container)
- **Up to 1,000 marks** (as `rdfs:member` entries)
- **1 Image object** (shared reference)
- **1 Analysis activity** (provenance)

### Key Field Mappings

**Image Entity** (`<urn:sha256:{hash}>`):

- `dcterms:identifier` → `case_id` (e.g., "TCGA-3C-AALI-01Z-00-DX1")
- `hal:subjectId` → `subject` (patient ID)
- `dcterms:isPartOf` → `study` (e.g., "TCGA-BRCA")
- `exif:width/height` → Image dimensions

**Feature Collection** (the main container):

- `dcterms:identifier` → MongoDB `_id`
- `dcterms:created` → `submit_date`
- `hal:caseId` → `case_id`
- `hal:executionId` → `execution_id`
- `hal:analysisRandval` → Random value from analysis

**Each Mark** (`rdfs:member`):

- `geo:asWKT` → Polygon geometry (denormalized from 0-1 to pixel coordinates)
- `hal:areaInPixels` → Nuclear area
- `hal:randval` → Mark's random value
- `hal:batchId` → Processing batch (e.g., "b0")
- `hal:tagId` → Processing tag (e.g., "t0")
- `prov:wasGeneratedBy` → Links to analysis activity

**Analysis Activity** (`<urn:analysis:{execution_id}>`):

- All `algorithm_params` as `hal:` properties (e.g., `hal:mpp`, `hal:minsize`)
- `prov:startedAtTime` → Submission timestamp

### Why 1,000 Marks per File?

**Practical balance between:**

- **File size**: ~5-20 MB compressed (manageable)
- **Memory usage**: Can load/process without OOM
- **Query performance**: Reasonable chunk for SPARQL queries
- **Parallelization**: Good work unit size
- **File system**: Avoids millions of tiny files (4B marks = 4M files vs 4B files)

With ~4 billion marks total, this creates ~4 million TTL files organized hierarchically by `execution_id/image_id/batch_number`.

<br>
