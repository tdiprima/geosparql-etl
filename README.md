# GeoSPARQL ETL 🧠

Note to self: this repo's just a pair of quick ETL scripts I hacked together to convert geospatial + pathology data into [GeoSPARQL](https://www.ogc.org/standards/geosparql) RDF triples.  
Basically: turn messy data → semantic linked data.

## 🚀 Overview / Reminder

### `geojson_to_geosparql_etl.py`
Takes **GeoJSON** feature collections and spits out **GeoSPARQL TTL** files.  
Uses SNOMED CT URIs for the tissue class mappings.

**Does this:**

- Loops through `.geojson` files in `./geojson_files`
- Pulls geometry + probability data
- Turns polygons → WKT
- Builds RDF triples w/ GeoSPARQL + PROV metadata
- Writes `.ttl` files into `./geosparql_output`

`wsinfer` output to Halcyon input.

### `nuclear_segmentation_etl.py`
Handles **nuclear segmentation CSVs** (from digital pathology workflows).  
Same idea — ends up with GeoSPARQL TTL, just different input.

**Does this:**

- Goes through subdirs of SVS images
- Geads patch-level CSVs (`*-features.csv`)
- Extracts polygons + patch metadata
- Maps everything to SNOMED "nuclear material"
- Can gzip compress the output TTLs

`quip_cnn_segmentation` "polygons" output to Halcyon input.

## ⚙️ Running these later

### geojson → ttl

```bash
python geojson_to_geosparql_etl.py
```

Defaults:

* Input: `./geojson_files`
* Output: `./geosparql_output`

Change paths in the `main()` section if needed.

### nuclear csv → ttl

```bash
python nuclear_segmentation_etl.py
```

Defaults:

* Input base: `./nuclear_segmentation_data`
* Output: `./nuclear_geosparql_output`
* Compression: on (`.ttl.gz`)

## 🧾 Notes

* No external deps. runs fine with stock Python
* If I ever need RDF validation, install `rdflib`
* Everything uses deterministic SHA-256 hashes
* SNOMED + GeoSPARQL prefixes are all in the script headers

## 👩‍💻 License

MIT — check [LICENSE](LICENSE) for the boring part.

<br>
