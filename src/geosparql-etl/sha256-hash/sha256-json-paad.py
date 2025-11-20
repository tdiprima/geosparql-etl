#!/usr/bin/env python3
"""
Update TTL files with SHA256 hashes from corresponding SVS files.

This script:
1. Reads TTL filenames from ~/tammy/test_wsinfer/results/geosparql_output
2. For each TTL file, computes SHA256 of the corresponding SVS file
3. Updates the TTL file:
   - Replaces urn:md5: with urn:sha256:
   - Replaces the hash value with the computed SHA256
4. If SVS file is missing or hash computation fails, adds hal:missing true triple
"""
import hashlib
import re
from pathlib import Path
from typing import Optional


def compute_sha256(file_path: Path) -> Optional[str]:
    """
    Compute SHA256 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        Hex string of SHA256 hash, or None if file doesn't exist or can't be read
    """
    if not file_path.exists():
        return None

    try:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        print(f"Error computing SHA256 for {file_path}: {e}")
        return None


def update_ttl_file(ttl_path: Path, sha256_hash: Optional[str]) -> None:
    """
    Update a TTL file with SHA256 hash or add hal:missing triple.

    Args:
        ttl_path: Path to the TTL file
        sha256_hash: SHA256 hash string, or None if file is missing
    """
    # Read the entire TTL file
    with open(ttl_path, "r", encoding="utf-8") as f:
        content = f.read()

    if sha256_hash:
        # Replace urn:md5: with urn:sha256: and update the hash value
        # Pattern matches: <urn:md5:HASH> where HASH is any hex string
        pattern = r"<urn:md5:([0-9a-fA-F]+)>"
        replacement = f"<urn:sha256:{sha256_hash}>"
        updated_content = re.sub(pattern, replacement, content)

        if updated_content == content:
            print(f"Warning: No urn:md5: pattern found in {ttl_path.name}")
        else:
            print(f"Updated {ttl_path.name} with SHA256 hash")
    else:
        # Add hal:missing true triple to the ImageObject
        # Find the ImageObject definition and add hal:missing true
        pattern = r"(<urn:md5:[0-9a-fA-F]+>)\s*\n(\s+a\s+so:ImageObject;)"
        replacement = r"\1\n\2\n        hal:missing      true;"
        updated_content = re.sub(pattern, replacement, content)

        # Also update urn:md5: to urn:sha256: even for missing files
        # (keeping the old hash as a placeholder)
        updated_content = re.sub(r"urn:md5:", "urn:sha256:", updated_content)

        if updated_content == content:
            print(f"Warning: Could not add hal:missing to {ttl_path.name}")
        else:
            print(f"Added hal:missing true to {ttl_path.name} (SVS file not found)")

    # Write the updated content back
    with open(ttl_path, "w", encoding="utf-8") as f:
        f.write(updated_content)


def main():
    """Main processing function."""
    # Define paths
    ttl_dir = Path.home() / "tammy" / "test_wsinfer" / "results" / "geosparql_output"
    svs_base_dir = Path("/data/quip_distro/images/tcga_data/paad")

    if not ttl_dir.exists():
        print(f"Error: TTL directory not found: {ttl_dir}")
        return

    if not svs_base_dir.exists():
        print(f"Error: SVS base directory not found: {svs_base_dir}")
        return

    # Get all TTL files
    ttl_files = list(ttl_dir.glob("*.ttl"))

    if not ttl_files:
        print(f"No TTL files found in {ttl_dir}")
        return

    print(f"Found {len(ttl_files)} TTL files to process")
    print(f"SVS base directory: {svs_base_dir}")
    print("-" * 80)

    # Process each TTL file
    for ttl_path in ttl_files:
        # Extract base filename (without .ttl extension)
        base_name = ttl_path.stem

        # Construct SVS file path
        svs_path = svs_base_dir / f"{base_name}.svs"

        # Compute SHA256 hash
        sha256_hash = compute_sha256(svs_path)

        if sha256_hash:
            print(f"Processing {base_name}.ttl - SHA256: {sha256_hash[:16]}...")
        else:
            print(f"Processing {base_name}.ttl - SVS file not found at {svs_path}")

        # Update the TTL file
        update_ttl_file(ttl_path, sha256_hash)

    print("-" * 80)
    print("Processing complete!")


if __name__ == "__main__":
    main()
