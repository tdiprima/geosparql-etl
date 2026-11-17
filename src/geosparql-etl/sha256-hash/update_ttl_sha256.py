#!/usr/bin/env python
"""
Replace <urn:sha256:...> in TTL files with the real SHA256 of the image file.

Walks TTL_DIR for .ttl and .ttl.gz files, reads the image name from
dc:identifier "<name>", finds that file anywhere under IMAGE_DIR, hashes its
contents, and rewrites the hash in the TTL file. If the image is not found,
tags the ImageObject with "hal:missing true" instead.

Runs on Python 2.7 and Python 3 (standard library only).

Usage:
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR --dry-run
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR --backup   # keep <file>.bak
    python update_ttl_sha256.py TTL_DIR IMAGE_DIR --workers 12

Progress goes to stderr, the final counts to stdout.
"""
from __future__ import print_function

import argparse
import gzip
import hashlib
import os
import re
import shutil
import sys
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

IDENTIFIER_RE = re.compile(br'dc:identifier\s+"([^"]+)"')
HASH_RE = re.compile(br"<urn:sha256:[0-9a-fA-F]+>")
IMAGE_OBJECT_RE = re.compile(br"(<urn:sha256:[0-9a-fA-F]+>\s+a\s+so:ImageObject\s*;)")
MISSING_RE = re.compile(br"\n[ \t]*hal:missing\s+true\s*;")
HAL_PREFIX = b"@prefix hal:  <https://halcyon.is/ns/> .\n"


def log(message):
    print(message, file=sys.stderr)
    sys.stderr.flush()


def walk_files(top):
    return [(name, os.path.join(root, name)) for root, _, names in os.walk(top) for name in names]


def index_images(image_dir, pool):
    """Map file name -> full path for every file under image_dir, one top-level folder per thread."""
    index = {}
    subdirs = []
    for name in sorted(os.listdir(image_dir)):
        path = os.path.join(image_dir, name)
        if not os.path.isdir(path):
            index[name] = path
        elif not os.path.islink(path):  # os.walk does not follow linked folders either
            subdirs.append(path)
    for found in pool.map(walk_files, subdirs):
        for name, path in found:
            index.setdefault(name, path)
    return index


def image_name(ttl_path):
    """Image name from the first 1 MB of a TTL file, or None. Cheap: no full read."""
    opener = gzip.open if ttl_path.endswith(".gz") else open
    try:
        with opener(ttl_path, "rb") as f:
            match = IDENTIFIER_RE.search(f.read(1 << 20))
    except (IOError, OSError):
        return None
    return match.group(1).decode("utf-8") if match else None


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def new_content(content, sha256_hash):
    """Return content with every <urn:sha256:...> set to sha256_hash."""
    # Image exists now, so drop any hal:missing tag left by an earlier run.
    content = MISSING_RE.sub(b"", content)
    return HASH_RE.sub(b"<urn:sha256:" + sha256_hash.encode("ascii") + b">", content)


def mark_missing(content):
    """Return content with 'hal:missing true' on the ImageObject (same wording as sha256-json-paad.py)."""
    if MISSING_RE.search(content):
        return content
    content = IMAGE_OBJECT_RE.sub(br"\1\n        hal:missing      true;", content, count=1)
    if MISSING_RE.search(content) and b"@prefix hal:" not in content:
        content = HAL_PREFIX + content
    return content


def process(ttl_path, images, hashes, dry_run, backup=False):
    """Return 'updated', 'unchanged', 'missing: ...', or a 'skipped: ...' reason."""
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
        updated = mark_missing(content)
        if not MISSING_RE.search(updated):
            return "skipped: image not found, no ImageObject to tag: " + name
        result = "missing: image not found, tagged hal:missing: " + name
    else:
        if name not in hashes:
            hashes[name] = sha256_of(images[name])
        updated = new_content(content, hashes[name])
        result = "updated"
    if updated == content:
        return result if result != "updated" else "unchanged"

    if not dry_run:
        # Write a temp file then rename, so a crash never leaves a half-written TTL.
        tmp_path = ttl_path + ".tmp"
        with opener(tmp_path, "wb") as f:
            f.write(updated)
        # Keep the first backup: a re-run must not replace the original with an updated copy.
        if backup and not os.path.exists(ttl_path + ".bak"):
            shutil.copy2(ttl_path, ttl_path + ".bak")
        os.rename(tmp_path, ttl_path)
    return result


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
    parser.add_argument(
        "--workers", type=int, default=cpu_count(), help="threads to use (default: all cores)"
    )
    args = parser.parse_args()

    for d in (args.ttl_dir, args.image_dir):
        if not os.path.isdir(d):
            sys.exit("Error: not a directory: " + d)

    # Threads, not processes: file reads, sha256 and gzip all release the GIL.
    pool = ThreadPool(max(1, args.workers))
    images = index_images(args.image_dir, pool)
    log("indexed %d image files" % len(images))

    ttl_paths = []
    for root, _, names in os.walk(args.ttl_dir):
        for name in sorted(names):
            if name.endswith((".ttl", ".ttl.gz")):
                ttl_paths.append(os.path.join(root, name))
    log("found %d TTL files" % len(ttl_paths))

    # Hash each needed image once, up front, so TTL files sharing an image never hash it twice.
    todo = sorted(n for n in set(pool.imap_unordered(image_name, ttl_paths)) if n in images)
    log("hashing %d images" % len(todo))

    def hash_one(name):
        try:
            return name, sha256_of(images[name])
        except (IOError, OSError):
            return name, None  # process() retries and reports the error

    hashes = {}
    for done, (name, digest) in enumerate(pool.imap_unordered(hash_one, todo), 1):
        if digest:
            hashes[name] = digest
        log("hashed %d/%d: %s" % (done, len(todo), name))

    def run_one(path):
        try:
            return path, process(path, images, hashes, args.dry_run, args.backup)
        except (IOError, OSError) as e:
            return path, "skipped: " + str(e)

    counts = {}
    for done, (path, result) in enumerate(pool.imap(run_one, ttl_paths), 1):
        key = result.split(":")[0]
        counts[key] = counts.get(key, 0) + 1
        if key in ("skipped", "missing"):
            log(path + ": " + result)
        if done % 100 == 0:
            log("processed %d/%d TTL files" % (done, len(ttl_paths)))

    for key in sorted(counts):
        print("%s: %d" % (key, counts[key]))
    if args.dry_run:
        print("dry run: no files changed")


def self_test():
    old = b'dc:identifier "a.svs";\n<urn:sha256:' + b"0" * 64 + b"> x"
    assert IDENTIFIER_RE.search(old).group(1) == b"a.svs"
    assert new_content(old, "ab" * 32).endswith(b"<urn:sha256:" + b"ab" * 32 + b"> x")
    tagged = mark_missing(b"<urn:sha256:" + b"0" * 64 + b">\n        a            so:ImageObject;\n x")
    assert tagged.startswith(HAL_PREFIX) and b"so:ImageObject;\n        hal:missing      true;\n x" in tagged
    assert mark_missing(tagged) == tagged
    assert b"hal:missing" not in new_content(tagged, "ab" * 32)
    print("self test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        main()
