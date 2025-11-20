#!/usr/bin/env python3
"""
Update .ttl.gz files with SHA256 hashes from JSON file.

This script (Part 2):
1. Reads slide_hashes.json with {slide: "name.svs", hash: "sha256-value"}
2. For each .svs subfolder in /data3/tammy/nuclear_geosparql_output
3. Updates all .ttl.gz files in that folder with the correct SHA256 hash
4. Updates urn:sha256: values by decompressing, modifying, and recompressing
"""
import gzip
import json
import re
from pathlib import Path
from typing import Dict, Optional


def load_hash_mapping(json_path: Path) -> Dict[str, str]:
    """
    Load the slide -> hash mapping from JSON file.

    Args:
        json_path: Path to slide_hashes.json

    Returns:
        Dictionary mapping slide names to SHA256 hashes
    """
    with open(json_path, 'r') as f:
        data = json.load(f)

    # Convert list of dicts to a simple slide->hash mapping
    mapping = {item['slide']: item['hash'] for item in data}
    return mapping


def update_ttl_gz_file(ttl_gz_path: Path, sha256_hash: str) -> bool:
    """
    Update a .ttl.gz file with the correct SHA256 hash.

    Args:
        ttl_gz_path: Path to the .ttl.gz file
        sha256_hash: The SHA256 hash to use

    Returns:
        True if updated successfully, False otherwise
    """
    try:
        # Decompress and read
        with gzip.open(ttl_gz_path, 'rt', encoding='utf-8') as f:
            content = f.read()

        # Replace urn:sha256: value
        # Pattern matches: <urn:sha256:HASH> where HASH is any hex string
        pattern = r'<urn:sha256:([0-9a-fA-F]+)>'
        replacement = f'<urn:sha256:{sha256_hash}>'

        # Check if pattern exists
        if not re.search(pattern, content):
            # Maybe it's still urn:md5:? Try replacing that too
            pattern_md5 = r'<urn:md5:([0-9a-fA-F]+)>'
            if re.search(pattern_md5, content):
                # Replace md5 with sha256
                updated_content = re.sub(pattern_md5, replacement, content)
            else:
                print(f"    Warning: No urn:sha256: or urn:md5: pattern found in {ttl_gz_path.name}")
                return False
        else:
            updated_content = re.sub(pattern, replacement, content)

        # Recompress and write back
        with gzip.open(ttl_gz_path, 'wt', encoding='utf-8') as f:
            f.write(updated_content)

        return True

    except Exception as e:
        print(f"    Error processing {ttl_gz_path.name}: {e}")
        return False


def main():
    """Main processing function."""
    # Paths
    json_path = Path("slide_hashes.json")
    base_dir = Path("/data3/tammy/nuclear_geosparql_output")

    # Check if JSON file exists
    if not json_path.exists():
        print(f"Error: JSON file not found: {json_path}")
        print("Please make sure slide_hashes.json is in the current directory")
        return

    # Check if base directory exists
    if not base_dir.exists():
        print(f"Error: Directory not found: {base_dir}")
        return

    # Load hash mapping
    print("Loading slide hashes from JSON...")
    hash_mapping = load_hash_mapping(json_path)
    print(f"Loaded {len(hash_mapping)} slide hashes")

    # Get all .svs folders
    svs_folders = [d for d in base_dir.iterdir() if d.is_dir() and d.name.endswith('.svs')]

    if not svs_folders:
        print(f"No .svs folders found in {base_dir}")
        return

    print(f"Found {len(svs_folders)} .svs folders to process")
    print("=" * 80)

    # Process each folder
    total_files_updated = 0
    total_files_processed = 0

    for svs_folder in svs_folders:
        folder_name = svs_folder.name

        # Get the hash for this slide
        if folder_name not in hash_mapping:
            print(f"\nWarning: No hash found for {folder_name}, skipping")
            continue

        sha256_hash = hash_mapping[folder_name]
        print(f"\nProcessing {folder_name}")
        print(f"  Hash: {sha256_hash[:16]}...")

        # Get all .ttl.gz files in this folder
        ttl_gz_files = list(svs_folder.glob("*.ttl.gz"))

        if not ttl_gz_files:
            print(f"  No .ttl.gz files found")
            continue

        print(f"  Found {len(ttl_gz_files)} .ttl.gz files")

        # Update each file
        updated_count = 0
        for ttl_gz_file in ttl_gz_files:
            if update_ttl_gz_file(ttl_gz_file, sha256_hash):
                updated_count += 1

        print(f"  Updated {updated_count}/{len(ttl_gz_files)} files")
        total_files_updated += updated_count
        total_files_processed += len(ttl_gz_files)

    print("=" * 80)
    print(f"Processing complete!")
    print(f"Total files processed: {total_files_processed}")
    print(f"Total files updated: {total_files_updated}")


if __name__ == "__main__":
    main()
