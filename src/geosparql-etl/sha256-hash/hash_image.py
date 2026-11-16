#!/usr/bin/env python3
"""
Print the SHA256 hash of an image file's contents to stdout.

Usage:
    python hash_image.py /path/to/image.svs
"""
import hashlib
import sys


def main():
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {sys.argv[0]} <image_path>")
    try:
        with open(sys.argv[1], "rb") as f:
            print(hashlib.file_digest(f, "sha256").hexdigest())
    except OSError as e:
        sys.exit(f"Error: {e}")


if __name__ == "__main__":
    main()
