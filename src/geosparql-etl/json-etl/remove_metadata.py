import re
from multiprocessing import Pool
from pathlib import Path


def process_file(file_path):
    """Remove dc:publisher and dc:references lines from the file."""

    # Read the current content
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    original_content = content

    # Remove dc:publisher line (matches the entire line including semicolon)
    content = re.sub(
        r"\s*dc:publisher\s+<https://ror\.org/01882y777>\s*,\s*<https://ror\.org/05qghxh33>\s*;\s*\n",
        "",
        content,
    )

    # Remove dc:references line (matches the entire line including semicolon)
    content = re.sub(
        r'\s*dc:references\s+"https://doi\.org/10\.1038/s41597-020-0528-1"\s*;\s*\n',
        "",
        content,
    )

    # Only write if content changed
    if content != original_content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Processed: {file_path}")
    else:
        print(f"Skipped (no changes needed): {file_path}")


if __name__ == "__main__":
    # Get all .ttl files in the directory
    input_dir = Path("/data/tammy/raj-class1")
    pathlist = list(input_dir.glob("**/*.ttl"))

    # Use parallel processing with Pool
    with Pool(12) as p:
        p.map(process_file, pathlist)

    print("Metadata removed from all files.")
