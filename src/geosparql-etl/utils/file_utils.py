"""File and directory operations utilities."""

import shutil
from pathlib import Path
from typing import List, Optional


def ensure_directory(directory: str) -> Path:
    """Create directory if it doesn't exist.

    Args:
        directory: Directory path to create

    Returns:
        Path: Path object for the directory

    Example:
        from utils.file_utils import ensure_directory

        output_dir = ensure_directory("output/ttl_files")
        print(f"Directory ready: {output_dir}")
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def ensure_parent_directory(file_path: str) -> Path:
    """Create parent directory for a file if it doesn't exist.

    Args:
        file_path: File path whose parent directory should be created

    Returns:
        Path: Path object for the parent directory

    Example:
        ensure_parent_directory("output/data/file.json")
        # Creates "output/data/" if it doesn't exist
    """
    file_path_obj = Path(file_path)
    if file_path_obj.parent != Path("."):
        file_path_obj.parent.mkdir(parents=True, exist_ok=True)
    return file_path_obj.parent


def generate_output_filename(
    output_dir: str,
    base_name: str,
    extension: str = "ttl",
    suffix: Optional[str] = None,
) -> Path:
    """Generate output file path with proper formatting.

    Args:
        output_dir: Output directory path
        base_name: Base filename (without extension)
        extension: File extension (default: "ttl")
        suffix: Optional suffix to append to filename

    Returns:
        Path: Complete file path

    Example:
        path = generate_output_filename(
            "output",
            "exec123_img456",
            "ttl",
            "batch1"
        )
        # Returns: output/exec123_img456_batch1.ttl
    """
    ensure_directory(output_dir)

    if suffix:
        filename = f"{base_name}_{suffix}.{extension}"
    else:
        filename = f"{base_name}.{extension}"

    return Path(output_dir) / filename


def generate_batch_filename(
    output_dir: str,
    exec_id: str,
    img_id: str,
    batch_num: int,
    extension: str = "ttl",
) -> Path:
    """Generate filename for batch processing output.

    Args:
        output_dir: Output directory
        exec_id: Execution ID
        img_id: Image ID
        batch_num: Batch number
        extension: File extension (default: "ttl")

    Returns:
        Path: Complete file path

    Example:
        path = generate_batch_filename(
            "ttl_output",
            "exec123",
            "img456",
            1
        )
        # Returns: ttl_output/exec123_img456_batch1.ttl
    """
    ensure_directory(output_dir)
    filename = f"{exec_id}_{img_id}_batch{batch_num}.{extension}"
    return Path(output_dir) / filename


def list_files_with_extension(
    directory: str, extension: str, recursive: bool = False
) -> List[Path]:
    """List all files with specific extension in directory.

    Args:
        directory: Directory to search
        extension: File extension (without dot)
        recursive: Whether to search recursively (default: False)

    Returns:
        List of Path objects for matching files

    Example:
        ttl_files = list_files_with_extension("output", "ttl")
        for file in ttl_files:
            print(file)
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return []

    if recursive:
        pattern = f"**/*.{extension}"
        return list(dir_path.glob(pattern))
    else:
        pattern = f"*.{extension}"
        return list(dir_path.glob(pattern))


def file_exists(file_path: str) -> bool:
    """Check if file exists.

    Args:
        file_path: Path to file

    Returns:
        bool: True if file exists

    Example:
        if file_exists("data.json"):
            print("File found!")
    """
    return Path(file_path).exists()


def directory_exists(directory: str) -> bool:
    """Check if directory exists.

    Args:
        directory: Path to directory

    Returns:
        bool: True if directory exists

    Example:
        if directory_exists("output"):
            print("Directory found!")
    """
    return Path(directory).is_dir()


def get_file_size(file_path: str) -> int:
    """Get file size in bytes.

    Args:
        file_path: Path to file

    Returns:
        int: File size in bytes

    Raises:
        FileNotFoundError: If file doesn't exist

    Example:
        size = get_file_size("data.json")
        print(f"File size: {size} bytes")
    """
    return Path(file_path).stat().st_size


def delete_file(file_path: str) -> bool:
    """Delete a file if it exists.

    Args:
        file_path: Path to file

    Returns:
        bool: True if file was deleted, False if it didn't exist

    Example:
        if delete_file("temp.txt"):
            print("File deleted")
    """
    path = Path(file_path)
    if path.exists():
        path.unlink()
        return True
    return False


def delete_directory(directory: str, recursive: bool = False) -> bool:
    """Delete a directory.

    Args:
        directory: Path to directory
        recursive: Whether to delete recursively (default: False)

    Returns:
        bool: True if directory was deleted

    Example:
        delete_directory("temp_output", recursive=True)
    """
    path = Path(directory)
    if not path.exists():
        return False

    if recursive:
        shutil.rmtree(path)
    else:
        path.rmdir()  # Only works if empty

    return True


def copy_file(source: str, destination: str) -> Path:
    """Copy a file from source to destination.

    Args:
        source: Source file path
        destination: Destination file path

    Returns:
        Path: Path to copied file

    Example:
        copy_file("data.json", "backup/data.json")
    """
    ensure_parent_directory(destination)
    shutil.copy2(source, destination)
    return Path(destination)


def move_file(source: str, destination: str) -> Path:
    """Move a file from source to destination.

    Args:
        source: Source file path
        destination: Destination file path

    Returns:
        Path: Path to moved file

    Example:
        move_file("temp.json", "archive/temp.json")
    """
    ensure_parent_directory(destination)
    shutil.move(source, destination)
    return Path(destination)


def get_absolute_path(file_path: str) -> Path:
    """Get absolute path for a file.

    Args:
        file_path: Relative or absolute file path

    Returns:
        Path: Absolute path

    Example:
        abs_path = get_absolute_path("../data/file.json")
        print(abs_path)
    """
    return Path(file_path).resolve()


def count_files_in_directory(directory: str, pattern: str = "*") -> int:
    """Count files in directory matching pattern.

    Args:
        directory: Directory to search
        pattern: Glob pattern (default: "*" for all files)

    Returns:
        int: Number of matching files

    Example:
        ttl_count = count_files_in_directory("output", "*.ttl")
        print(f"Found {ttl_count} TTL files")
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return 0
    return len(list(dir_path.glob(pattern)))
