"""
Update .ttl.gz files with SHA256 hashes from JSON file.

This script (Part 2):
1. Reads slide_hashes.json with {slide: "name.svs", hash: "sha256-value"}
2. For each .svs subfolder in /data3/tammy/nuclear_geosparql_output
3. Updates all .ttl.gz files in that folder with the correct SHA256 hash
4. Updates urn:sha256: values by decompressing, modifying, and recompressing

Usage:
    python update_ttl_gz_from_json.py                    # Process all folders
    python update_ttl_gz_from_json.py --start-from <name> # Resume from specific folder
"""

import gzip
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple
from multiprocessing import Pool, cpu_count
import argparse
from functools import partial


def load_hash_mapping(json_path: Path) -> Dict[str, str]:
    """
    Load the slide -> hash mapping from JSON file.

    Args:
        json_path: Path to slide_hashes.json

    Returns:
        Dictionary mapping slide names to SHA256 hashes
    """
    with open(json_path, "r") as f:
        data = json.load(f)

    # Convert list of dicts to a simple slide->hash mapping
    mapping = {item["slide"]: item["hash"] for item in data}
    return mapping


def update_ttl_gz_file(ttl_gz_path: Path, sha256_hash: str) -> Tuple[bool, str]:
    """
    Update a .ttl.gz file with the correct SHA256 hash.

    Args:
        ttl_gz_path: Path to the .ttl.gz file
        sha256_hash: The SHA256 hash to use

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Decompress and read
        with gzip.open(ttl_gz_path, "rt", encoding="utf-8") as f:
            content = f.read()

        # Replace urn:sha256: value
        # Pattern matches: <urn:sha256:HASH> where HASH is any hex string
        pattern = r"<urn:sha256:([0-9a-fA-F]+)>"
        replacement = f"<urn:sha256:{sha256_hash}>"

        # Check if pattern exists
        if not re.search(pattern, content):
            # Maybe it's still urn:md5:? Try replacing that too
            pattern_md5 = r"<urn:md5:([0-9a-fA-F]+)>"
            if re.search(pattern_md5, content):
                # Replace md5 with sha256
                updated_content = re.sub(pattern_md5, replacement, content)
            else:
                return False, f"No urn:sha256: or urn:md5: pattern found in {ttl_gz_path.name}"
        else:
            updated_content = re.sub(pattern, replacement, content)

        # Recompress and write back
        with gzip.open(ttl_gz_path, "wt", encoding="utf-8") as f:
            f.write(updated_content)

        return True, ""

    except Exception as e:
        return False, f"Error processing {ttl_gz_path.name}: {e}"


def process_single_file(args: Tuple[Path, str]) -> Tuple[bool, str]:
    """
    Wrapper function for multiprocessing.

    Args:
        args: Tuple of (ttl_gz_path, sha256_hash)

    Returns:
        Tuple of (success: bool, message: str)
    """
    ttl_gz_path, sha256_hash = args
    return update_ttl_gz_file(ttl_gz_path, sha256_hash)


def process_folder(svs_folder: Path, hash_mapping: Dict[str, str], num_workers: int = None) -> Tuple[int, int, List[str]]:
    """
    Process all .ttl.gz files in a folder using multiprocessing.

    Args:
        svs_folder: Path to the .svs folder
        hash_mapping: Dictionary mapping slide names to SHA256 hashes
        num_workers: Number of worker processes (default: CPU count)

    Returns:
        Tuple of (updated_count, total_count, error_messages)
    """
    folder_name = svs_folder.name

    # Get the hash for this slide
    if folder_name not in hash_mapping:
        return 0, 0, [f"No hash found for {folder_name}"]

    sha256_hash = hash_mapping[folder_name]

    # Get all .ttl.gz files in this folder
    ttl_gz_files = list(svs_folder.glob("*.ttl.gz"))

    if not ttl_gz_files:
        return 0, 0, ["No .ttl.gz files found"]

    # Prepare arguments for multiprocessing
    args_list = [(ttl_file, sha256_hash) for ttl_file in ttl_gz_files]

    # Use multiprocessing pool
    if num_workers is None:
        num_workers = min(cpu_count(), len(ttl_gz_files))

    error_messages = []
    updated_count = 0

    with Pool(processes=num_workers) as pool:
        results = pool.map(process_single_file, args_list)

        for success, message in results:
            if success:
                updated_count += 1
            elif message:
                error_messages.append(message)

    return updated_count, len(ttl_gz_files), error_messages


def main():
    """Main processing function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Update .ttl.gz files with SHA256 hashes from JSON file"
    )
    parser.add_argument(
        "--start-from",
        type=str,
        help="Resume processing from this folder name (e.g., TCGA-D5-6530-01Z-00-DX1...svs)",
        default=None,
    )
    parser.add_argument(
        "--workers",
        type=int,
        help=f"Number of worker processes (default: {cpu_count()})",
        default=cpu_count(),
    )
    args = parser.parse_args()

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

    # Get all .svs folders (sorted for consistent ordering)
    svs_folders = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and d.name.endswith(".svs")]
    )

    if not svs_folders:
        print(f"No .svs folders found in {base_dir}")
        return

    # Handle resume from specific folder
    start_index = 0
    if args.start_from:
        try:
            start_index = next(
                i for i, folder in enumerate(svs_folders)
                if folder.name == args.start_from
            )
            print(f"Resuming from: {args.start_from}")
            print(f"Skipping first {start_index} folders")
        except StopIteration:
            print(f"Warning: Start folder '{args.start_from}' not found")
            print("Processing all folders...")
            start_index = 0

    svs_folders_to_process = svs_folders[start_index:]

    print(f"Found {len(svs_folders)} total .svs folders")
    print(f"Processing {len(svs_folders_to_process)} folders")
    print(f"Using {args.workers} worker processes")
    print("=" * 80)

    # Process each folder
    total_files_updated = 0
    total_files_processed = 0

    for idx, svs_folder in enumerate(svs_folders_to_process, start=1):
        folder_name = svs_folder.name
        print(f"\n[{idx}/{len(svs_folders_to_process)}] Processing {folder_name}")

        updated_count, total_count, error_messages = process_folder(
            svs_folder, hash_mapping, num_workers=args.workers
        )

        if error_messages:
            for msg in error_messages:
                print(f"  Warning: {msg}")

        if total_count > 0:
            print(f"  Updated {updated_count}/{total_count} files")
            total_files_updated += updated_count
            total_files_processed += total_count

    print("=" * 80)
    print("Processing complete!")
    print(f"Total files processed: {total_files_processed}")
    print(f"Total files updated: {total_files_updated}")


if __name__ == "__main__":
    main()
