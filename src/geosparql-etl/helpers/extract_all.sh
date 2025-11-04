#!/bin/bash
# Extract all .tar.gz files recursively and delete the archives

find . -type f -name "*.tar.gz" | while read -r archive; do
    echo "Extracting: $archive"
    dir="$(dirname "$archive")"
    tar -xzf "$archive" -C "$dir" && rm -f "$archive"
done

echo "✅ All archives extracted and cleaned up."
