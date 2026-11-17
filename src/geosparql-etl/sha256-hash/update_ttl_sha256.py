#!/usr/bin/env python
"""
Replace <urn:sha256:...> in TTL files with the real SHA256 of the image file.

Walks TTL_DIR for .ttl and .ttl.gz files, reads the image name from
dc:identifier "<name>", finds that file anywhere under IMAGE_DIR, hashes its
contents, and rewrites the hash in the TTL file.

Runs on Python 2.7 and Python 3 (standard library only).

Usage:
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR --dry-run
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR --backup   # keep <file>.bak
"""
from __future__ import print_function

import argparse
import gzip
import hashlib
import os
import re
import shutil
import sys

IDENTIFIER_RE = re.compile(br'dc:identifier\s+"([^"]+)"')
HASH_RE = re.compile(br"<urn:sha256:[0-9a-fA-F]+>")


def index_images(image_dir):
    """Map file name -> full path for every file under image_dir."""
    index = {}
    for root, _, names in os.walk(image_dir):
        for name in names:
            index.setdefault(name, os.path.join(root, name))
    return index


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def new_content(content, sha256_hash):
    """Return content with every <urn:sha256:...> set to sha256_hash."""
    return HASH_RE.sub(b"<urn:sha256:" + sha256_hash.encode("ascii") + b">", content)


def process(ttl_path, images, hashes, dry_run, backup=False):
    """Return 'updated', 'unchanged', or a 'skipped: ...' reason."""
    opener = gzip.open if ttl_path.endswith(".gz") else open
    # shortcut: whole file in memory, switch to streaming if TTL files exceed RAM
    with opener(ttl_path, "rb") as f:
        content = f.read()

    match = IDENTIFIER_RE.search(content)
    if not match:
        return "skipped: no dc:identifier"
    name = match.group(1).decode("utf-8")
    if not HASH_RE.search(content):
        return "skipped: no urn:sha256"
    if name not in images:
        return "skipped: image not found: " + name

    if name not in hashes:
        hashes[name] = sha256_of(images[name])
    updated = new_content(content, hashes[name])
    if updated == content:
        return "unchanged"

    if not dry_run:
        # Write a temp file then rename, so a crash never leaves a half-written TTL.
        tmp_path = ttl_path + ".tmp"
        with opener(tmp_path, "wb") as f:
            f.write(updated)
        # Keep the first backup: a re-run must not replace the original with an updated copy.
        if backup and not os.path.exists(ttl_path + ".bak"):
            shutil.copy2(ttl_path, ttl_path + ".bak")
        os.rename(tmp_path, ttl_path)
    return "updated"


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("ttl_dir")
    parser.add_argument("image_dir")
    parser.add_argument(
        "--dry-run", action="store_true", help="report only, change no files"
    )
    parser.add_argument(
        "--backup", action="store_true", help="copy each file to <file>.bak before overwriting"
    )
    args = parser.parse_args()

    for d in (args.ttl_dir, args.image_dir):
        if not os.path.isdir(d):
            sys.exit("Error: not a directory: " + d)

    images = index_images(args.image_dir)
    hashes = {}
    counts = {}
    for root, _, names in os.walk(args.ttl_dir):
        for name in sorted(names):
            if not name.endswith((".ttl", ".ttl.gz")):
                continue
            path = os.path.join(root, name)
            try:
                result = process(path, images, hashes, args.dry_run, args.backup)
            except (IOError, OSError) as e:
                result = "skipped: " + str(e)
            key = result.split(":")[0]
            counts[key] = counts.get(key, 0) + 1
            if key == "skipped":
                print(path + ": " + result, file=sys.stderr)

    for key in sorted(counts):
        print("%s: %d" % (key, counts[key]))
    if args.dry_run:
        print("dry run: no files changed")


def self_test():
    old = b'dc:identifier "a.svs";\n<urn:sha256:' + b"0" * 64 + b"> x"
    assert IDENTIFIER_RE.search(old).group(1) == b"a.svs"
    assert new_content(old, "ab" * 32).endswith(b"<urn:sha256:" + b"ab" * 32 + b"> x")
    print("self test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        main()
