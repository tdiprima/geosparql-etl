#!/usr/bin/env python3
"""
Generate a shell script to compute SHA256 hashes for SVS files.

This script (Part 1):
1. Scans /data3/tammy/nuclear_segmentation_data/cvpr-data for subfolders ending in _polygon
2. Extracts the prefix from .svs.tar.gz filenames in each subfolder
3. Generates a shell script that will compute SHA256 for each SVS file
4. The generated script outputs to a JSON file: {"slide": "name.svs", "hash": "sha256"}
"""
from pathlib import Path


def extract_svs_filename(tar_gz_file: Path) -> str:
    """
    Extract the SVS filename from a .svs.tar.gz filename.

    Example: TCGA-A2-A0T2-01Z-00-DX1.svs.tar.gz -> TCGA-A2-A0T2-01Z-00-DX1.svs

    Args:
        tar_gz_file: Path to the .svs.tar.gz file

    Returns:
        SVS filename (without .tar.gz)
    """
    filename = tar_gz_file.name
    # Remove .tar.gz suffix to get the .svs filename
    if filename.endswith('.svs.tar.gz'):
        return filename[:-7]  # Remove last 7 characters (.tar.gz)
    return filename


def extract_prefix_from_folder(folder_name: str) -> str:
    """
    Extract the prefix from a folder name ending in _polygon.

    Example: brca_polygon -> brca

    Args:
        folder_name: Name of the folder

    Returns:
        Prefix string (first part before underscore)
    """
    # Split by underscore and get first part
    prefix = folder_name.split("_")[0]
    return prefix


def generate_script():
    """Generate the SHA256 computation shell script."""
    base_dir = Path("/data3/tammy/nuclear_segmentation_data/cvpr-data")

    if not base_dir.exists():
        print(f"Error: Directory not found: {base_dir}")
        return

    # Get all subdirectories ending in _polygon
    polygon_folders = [
        d for d in base_dir.iterdir() if d.is_dir() and d.name.endswith("_polygon")
    ]

    if not polygon_folders:
        print(f"No _polygon folders found in {base_dir}")
        return

    print(f"Found {len(polygon_folders)} _polygon folders")

    # Store SVS files to process
    svs_file_info = []

    for polygon_folder in polygon_folders:
        # Extract prefix from folder name (e.g., brca_polygon -> brca)
        prefix = extract_prefix_from_folder(polygon_folder.name)

        # Find all .svs.tar.gz files in this folder
        tar_gz_files = list(polygon_folder.glob("*.svs.tar.gz"))

        if not tar_gz_files:
            print(f"Warning: No .svs.tar.gz files found in {polygon_folder.name}, skipping")
            continue

        print(f"  {polygon_folder.name} -> prefix: {prefix} ({len(tar_gz_files)} svs.tar.gz files)")

        # Process each .svs.tar.gz file
        for tar_gz_file in tar_gz_files:
            svs_filename = extract_svs_filename(tar_gz_file)
            svs_file_info.append({
                "svs_filename": svs_filename,
                "prefix": prefix,
                "polygon_folder": polygon_folder.name
            })

    print(f"\nTotal SVS files to process: {len(svs_file_info)}")

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

        for info in svs_file_info:
            svs_filename = info["svs_filename"]
            prefix = info["prefix"]
            svs_path = f"/data/quip_distro/images/tcga_data/{prefix}/{svs_filename}"

            f.write(f"# Process {svs_filename} from {info['polygon_folder']}\n")
            f.write(f'if [ -f "{svs_path}" ]; then\n')
            f.write(f'    echo "Computing SHA256 for {svs_filename}..."\n')
            f.write(f"    HASH=$(sha256sum \"{svs_path}\" | cut -d' ' -f1)\n")
            f.write("    \n")
            f.write("    # Add comma if not first entry\n")
            f.write('    if [ "$FIRST" = false ]; then\n')
            f.write('        echo "," >> "$OUTPUT_FILE"\n')
            f.write("    fi\n")
            f.write("    FIRST=false\n")
            f.write("    \n")
            f.write("    # Write JSON entry\n")
            f.write('    echo "  {" >> "$OUTPUT_FILE"\n')
            f.write(f'    echo "    \\"slide\\": \\"{svs_filename}\\"," >> "$OUTPUT_FILE"\n')
            f.write('    echo "    \\"hash\\": \\"$HASH\\"" >> "$OUTPUT_FILE"\n')
            f.write('    echo "  }" >> "$OUTPUT_FILE"\n')
            f.write("else\n")
            f.write(f'    echo "Warning: SVS file not found: {svs_path}"\n')
            f.write("fi\n\n")

        f.write("# Close JSON array\n")
        f.write('echo "]" >> "$OUTPUT_FILE"\n\n')
        f.write('echo "Done! Results written to $OUTPUT_FILE"\n')

    # Make script executable
    script_path.chmod(0o755)

    print(f"\n{'='*80}")
    print(f"Generated script: {script_path.absolute()}")
    print(f"Output JSON file will be: {json_output}")
    print(f"Total slides to process: {len(svs_file_info)}")
    print("\nTo run the generated script:")
    print(f"  ./{script_path.name}")
    print(f"{'='*80}")


if __name__ == "__main__":
    generate_script()
