#!/usr/bin/env python3
"""
Generate a shell script to compute SHA256 hashes for SVS files.

This script (Part 1):
1. Scans /data3/tammy/nuclear_geosparql_output for subfolders ending in .svs
2. Extracts the prefix from .ttl.gz filenames in each subfolder
3. Generates a shell script that will compute SHA256 for each SVS file
4. The generated script outputs to a JSON file: {"slide": "name.svs", "hash": "sha256"}
"""
import json
from pathlib import Path


def extract_prefix_from_ttl(ttl_gz_file: Path) -> str:
    """
    Extract the prefix from a .ttl.gz filename.

    Example: cesc_36001_4001_4000_4000_0.248_1-features.ttl.gz -> cesc

    Args:
        ttl_gz_file: Path to the .ttl.gz file

    Returns:
        Prefix string (first part before underscore)
    """
    filename = ttl_gz_file.name
    # Split by underscore and get first part
    prefix = filename.split("_")[0]
    return prefix


def generate_script():
    """Generate the SHA256 computation shell script."""
    base_dir = Path("/data3/tammy/nuclear_geosparql_output")

    if not base_dir.exists():
        print(f"Error: Directory not found: {base_dir}")
        return

    # Get all subdirectories ending in .svs
    svs_folders = [
        d for d in base_dir.iterdir() if d.is_dir() and d.name.endswith(".svs")
    ]

    if not svs_folders:
        print(f"No .svs folders found in {base_dir}")
        return

    print(f"Found {len(svs_folders)} .svs folders")

    # Store folder -> prefix mapping
    folder_info = []

    for svs_folder in svs_folders:
        # Find any .ttl.gz file in this folder to extract the prefix
        ttl_files = list(svs_folder.glob("*.ttl.gz"))

        if not ttl_files:
            print(f"Warning: No .ttl.gz files found in {svs_folder.name}, skipping")
            continue

        # Extract prefix from the first .ttl.gz file
        prefix = extract_prefix_from_ttl(ttl_files[0])
        folder_name = svs_folder.name

        folder_info.append(
            {"folder": folder_name, "prefix": prefix, "ttl_count": len(ttl_files)}
        )

        print(f"  {folder_name} -> prefix: {prefix} ({len(ttl_files)} ttl.gz files)")

    # Generate the shell script
    script_path = Path("compute_sha256_hashes.sh")
    json_output = "slide_hashes.json"

    with open(script_path, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("#\n")
        f.write("# Auto-generated script to compute SHA256 hashes for SVS files\n")
        f.write("# Output: slide_hashes.json\n")
        f.write("#\n\n")
        f.write("set -e  # Exit on error\n\n")
        f.write("# Output JSON file\n")
        f.write(f'OUTPUT_FILE="{json_output}"\n\n')
        f.write("# Start JSON array\n")
        f.write('echo "[" > "$OUTPUT_FILE"\n\n')
        f.write("FIRST=true\n\n")

        for info in folder_info:
            folder = info["folder"]
            prefix = info["prefix"]
            svs_path = f"/data/quip_distro/images/tcga_data/{prefix}/{folder}"

            f.write(f"# Process {folder}\n")
            f.write(f'if [ -f "{svs_path}" ]; then\n')
            f.write(f'    echo "Computing SHA256 for {folder}..."\n')
            f.write(f"    HASH=$(sha256sum \"{svs_path}\" | cut -d' ' -f1)\n")
            f.write(f"    \n")
            f.write(f"    # Add comma if not first entry\n")
            f.write(f'    if [ "$FIRST" = false ]; then\n')
            f.write(f'        echo "," >> "$OUTPUT_FILE"\n')
            f.write(f"    fi\n")
            f.write(f"    FIRST=false\n")
            f.write(f"    \n")
            f.write(f"    # Write JSON entry\n")
            f.write(f'    echo "  {{" >> "$OUTPUT_FILE"\n')
            f.write(f'    echo "    \\"slide\\": \\"{folder}\\"," >> "$OUTPUT_FILE"\n')
            f.write(f'    echo "    \\"hash\\": \\"$HASH\\"" >> "$OUTPUT_FILE"\n')
            f.write(f'    echo "  }}" >> "$OUTPUT_FILE"\n')
            f.write(f"else\n")
            f.write(f'    echo "Warning: SVS file not found: {svs_path}"\n')
            f.write(f"fi\n\n")

        f.write("# Close JSON array\n")
        f.write('echo "]" >> "$OUTPUT_FILE"\n\n')
        f.write('echo "Done! Results written to $OUTPUT_FILE"\n')

    # Make script executable
    script_path.chmod(0o755)

    print(f"\n{'='*80}")
    print(f"Generated script: {script_path.absolute()}")
    print(f"Output JSON file will be: {json_output}")
    print(f"Total slides to process: {len(folder_info)}")
    print(f"\nTo run the generated script:")
    print(f"  ./{script_path.name}")
    print(f"{'='*80}")


if __name__ == "__main__":
    generate_script()
