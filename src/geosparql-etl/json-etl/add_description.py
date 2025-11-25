from multiprocessing import Pool
from pathlib import Path


def process_file(file_path):
    """Add a description comment to the top of the file."""
    comment = "# Raj's 10-class classification results produced via wsinfer and Tammy's PyTorch model\n"

    # Read the current content
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Only add comment if it's not already there
    if not content.startswith(comment.strip()):
        # Write comment at the top followed by original content
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(comment)
            f.write(content)
        print(f"Processed: {file_path}")
    else:
        print(f"Skipped (already has comment): {file_path}")


if __name__ == "__main__":
    # Get all .ttl files in the directory
    input_dir = Path("/data/tammy/raj-class1")
    pathlist = list(input_dir.glob("**/*.ttl"))

    # Use parallel processing with Pool
    with Pool(12) as p:
        p.map(process_file, pathlist)

    print("Descriptions added to all files.")
