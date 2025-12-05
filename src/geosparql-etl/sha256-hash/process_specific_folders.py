"""
Process specific .svs folders by name.

Usage:
    python process_specific_folders.py TCGA-D5-6530-01Z-00-DX1.5c4bbcd1-51ba-467d-93f9-f2a9e7c5e010.svs
    python process_specific_folders.py folder1.svs folder2.svs folder3.svs
"""

import gzip
import json
import re
import sys
from pathlib import Path
from typing import Dict, Tuple
from multiprocessing import Pool, cpu_count


def load_hash_mapping(json_path: Path) -> Dict[str, str]:
    """Load the slide -> hash mapping from JSON file."""
    with open(json_path, "r") as f:
        data = json.load(f)
    mapping = {item["slide"]: item["hash"] for item in data}
    return mapping


def update_ttl_gz_file(ttl_gz_path: Path, sha256_hash: str) -> Tuple[bool, str]:
    """Update a .ttl.gz file with the correct SHA256 hash."""
    try:
        # Decompress and read
        with gzip.open(ttl_gz_path, "rt", encoding="utf-8") as f:
            content = f.read()

        # Replace urn:sha256: value
        pattern = r"<urn:sha256:([0-9a-fA-F]+)>"
        replacement = f"<urn:sha256:{sha256_hash}>"

        # Check if pattern exists
        if not re.search(pattern, content):
            # Maybe it's still urn:md5:? Try replacing that too
            pattern_md5 = r"<urn:md5:([0-9a-fA-F]+)>"
            if re.search(pattern_md5, content):
                updated_content = re.sub(pattern_md5, replacement, content)
            else:
                return False, f"No urn:sha256: or urn:md5: pattern found"
        else:
            updated_content = re.sub(pattern, replacement, content)

        # Recompress and write back
        with gzip.open(ttl_gz_path, "wt", encoding="utf-8") as f:
            f.write(updated_content)

        return True, ""

    except Exception as e:
        return False, str(e)


def process_single_file(args: Tuple[Path, str]) -> Tuple[bool, str, str]:
    """Wrapper function for multiprocessing."""
    ttl_gz_path, sha256_hash = args
    success, message = update_ttl_gz_file(ttl_gz_path, sha256_hash)
    return success, message, ttl_gz_path.name


def process_folder(folder_name: str, base_dir: Path, hash_mapping: Dict[str, str]):
    """Process a single folder."""
    svs_folder = base_dir / folder_name

    if not svs_folder.exists():
        print(f"ERROR: Folder not found: {svs_folder}")
        return

    if folder_name not in hash_mapping:
        print(f"ERROR: No hash found in JSON for: {folder_name}")
        return

    sha256_hash = hash_mapping[folder_name]
    print(f"\nProcessing: {folder_name}")
    print(f"  SHA256: {sha256_hash}")

    # Get all .ttl.gz files
    ttl_gz_files = list(svs_folder.glob("*.ttl.gz"))

    if not ttl_gz_files:
        print("  No .ttl.gz files found")
        return

    print(f"  Found {len(ttl_gz_files)} .ttl.gz files")

    # Process files with multiprocessing
    args_list = [(ttl_file, sha256_hash) for ttl_file in ttl_gz_files]
    num_workers = min(cpu_count(), len(ttl_gz_files))

    updated_count = 0
    failed_files = []

    with Pool(processes=num_workers) as pool:
        results = pool.map(process_single_file, args_list)

        for success, message, filename in results:
            if success:
                updated_count += 1
            else:
                failed_files.append((filename, message))

    print(f"  Successfully updated: {updated_count}/{len(ttl_gz_files)} files")

    if failed_files:
        print(f"  Failed files ({len(failed_files)}):")
        for filename, error in failed_files:
            print(f"    - {filename}: {error}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python process_specific_folders.py <folder1.svs> [folder2.svs ...]")
        print("\nExample:")
        print("  python process_specific_folders.py TCGA-D5-6530-01Z-00-DX1.5c4bbcd1-51ba-467d-93f9-f2a9e7c5e010.svs")
        sys.exit(1)

    # Paths
    json_path = Path("slide_hashes.json")
    base_dir = Path("/data3/tammy/nuclear_geosparql_output")

    # Check if JSON file exists
    if not json_path.exists():
        print(f"ERROR: JSON file not found: {json_path}")
        sys.exit(1)

    # Check if base directory exists
    if not base_dir.exists():
        print(f"ERROR: Directory not found: {base_dir}")
        sys.exit(1)

    # Load hash mapping
    print("Loading slide hashes from JSON...")
    hash_mapping = load_hash_mapping(json_path)
    print(f"Loaded {len(hash_mapping)} slide hashes")
    print("=" * 80)

    # Process each folder specified on command line
    folder_names = sys.argv[1:]

    for folder_name in folder_names:
        process_folder(folder_name, base_dir, hash_mapping)

    print("=" * 80)
    print("Done!")


if __name__ == "__main__":
    main()
