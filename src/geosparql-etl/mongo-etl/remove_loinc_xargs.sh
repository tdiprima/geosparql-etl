#!/bin/bash
# Script to remove loinc prefix line from .ttl.gz files in parallel (using xargs)

DATA_DIR="/data/tammy/mongo-ttl"
PREFIX_TO_REMOVE="@prefix loinc: <http://loinc.org/rdf/> ."
NUM_CORES=20

echo "Processing .ttl.gz files in ${DATA_DIR}"
echo "Using ${NUM_CORES} parallel processes"
echo "Removing line: ${PREFIX_TO_REMOVE}"
echo ""

# Function to process a single file
process_file() {
    local file="$1"
    local temp_file="${file}.tmp"

    echo "Processing: $(basename ${file})"

    # Decompress, remove the line, recompress
    zcat "${file}" | grep -Fv "${PREFIX_TO_REMOVE}" | gzip > "${temp_file}"

    # Check if the operation was successful
    if [ $? -eq 0 ]; then
        # Replace original file with processed file
        mv "${temp_file}" "${file}"
        echo "Completed: $(basename ${file})"
    else
        echo "Error processing: $(basename ${file})"
        rm -f "${temp_file}"
        return 1
    fi
}

# Export the function and variables
export -f process_file
export PREFIX_TO_REMOVE

# Find all .ttl.gz files and process them in parallel using xargs
find "${DATA_DIR}" -name "*.ttl.gz" -type f -print0 | \
    xargs -0 -P ${NUM_CORES} -I {} bash -c 'process_file "$@"' _ {}

echo ""
echo "Processing complete!"

# find /data/tammy/mongo-ttl -name "*.ttl.gz.tmp" -type f 2>/dev/null | head -20
# find /data/tammy/mongo-ttl -name "*.ttl.gz.tmp" -type f -delete
