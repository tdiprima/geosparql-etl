"""
Update image hashes in TTL files using actual file SHA256 values
Uses sha256_pipeline to get correct hash from Drupal node/slide ID
Author: Bear 🐻
"""

import gzip
import logging
import re
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from sha256_pipeline import get_auth, get_real_hash_from_node

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'update_ttl_hashes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Configuration
TTL_OUTPUT_DIR = Path("ttl_output")

# Cache for node_id -> hash mappings to avoid redundant lookups
hash_cache = {}
failed_nodes = set()


@contextmanager
def timer(name):
    """Context manager to time operations"""
    start = time.time()
    logger.info(f"Starting {name}...")
    yield
    elapsed = time.time() - start
    logger.info(f"Completed {name} in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")


def extract_slide_id_and_hash(content):
    """
    Extract the slideId (node_id) and current hash from TTL content.
    Returns (slide_id, old_hash) or (None, None) if not found.
    """
    # Look for hal:slideId "123" ;
    slide_match = re.search(r'hal:slideId\s+"(\d+)"', content)
    if not slide_match:
        return None, None

    slide_id = int(slide_match.group(1))

    # Look for <urn:sha256:{hash}>
    hash_match = re.search(r"<urn:sha256:([0-9a-f]{64})>", content)
    if not hash_match:
        return slide_id, None

    old_hash = hash_match.group(1)

    return slide_id, old_hash


def get_correct_hash(slide_id, auth):
    """
    Get the correct SHA256 hash for a slide/node.
    Uses cache to avoid redundant lookups.
    Returns hash string or None if failed.
    """
    # Check cache first
    if slide_id in hash_cache:
        return hash_cache[slide_id]

    # Check if we've already failed on this node
    if slide_id in failed_nodes:
        return None

    try:
        correct_hash = get_real_hash_from_node(slide_id, auth=auth)
        hash_cache[slide_id] = correct_hash
        logger.debug(f"Got hash for slide {slide_id}: {correct_hash}")
        return correct_hash
    except Exception as e:
        logger.warning(f"Failed to get hash for slide {slide_id}: {e}")
        failed_nodes.add(slide_id)
        return None


def update_hash_in_content(content, old_hash, new_hash):
    """
    Replace old hash with new hash in TTL content.
    Returns updated content.
    """
    old_urn = f"<urn:sha256:{old_hash}>"
    new_urn = f"<urn:sha256:{new_hash}>"

    updated_content = content.replace(old_urn, new_urn)

    return updated_content


def process_ttl_file(file_path, auth):
    """
    Process a single TTL.gz file.
    Returns (success, updated_bool, slide_id)
    """
    try:
        # Read compressed file
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            content = f.read()

        # Extract slide_id and current hash
        slide_id, old_hash = extract_slide_id_and_hash(content)

        if slide_id is None:
            logger.warning(
                f"No slideId found in {file_path.relative_to(TTL_OUTPUT_DIR)}"
            )
            return True, False, None

        if old_hash is None:
            logger.warning(f"No hash found in {file_path.relative_to(TTL_OUTPUT_DIR)}")
            return True, False, slide_id

        # Get correct hash from sha256_pipeline
        correct_hash = get_correct_hash(slide_id, auth)

        if correct_hash is None:
            # Failed to get hash, skip this file
            return True, False, slide_id

        # Check if hash needs updating
        if old_hash == correct_hash:
            # Hash is already correct
            return True, False, slide_id

        # Update the hash in content
        updated_content = update_hash_in_content(content, old_hash, correct_hash)

        # Write back to file (compressed)
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            f.write(updated_content)

        logger.info(
            f"Updated hash in {file_path.relative_to(TTL_OUTPUT_DIR)} (slide {slide_id})"
        )
        return True, True, slide_id

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return False, False, None


def main():
    """Main function to process all TTL files"""
    logger.info("=" * 80)
    logger.info("TTL Hash Update Script")
    logger.info(f"Directory: {TTL_OUTPUT_DIR.absolute()}")
    logger.info("=" * 80)

    if not TTL_OUTPUT_DIR.exists():
        logger.error(f"Directory not found: {TTL_OUTPUT_DIR}")
        return

    # Get authentication
    try:
        auth = get_auth()
    except ValueError as e:
        logger.error(f"Authentication error: {e}")
        return

    # Find all batch_*.ttl.gz files
    pattern = "batch_*.ttl.gz"
    ttl_files = list(TTL_OUTPUT_DIR.rglob(pattern))

    if not ttl_files:
        logger.warning(f"No files matching '{pattern}' found in {TTL_OUTPUT_DIR}")
        return

    logger.info(f"Found {len(ttl_files)} TTL files to process")

    # Process each file
    total_files = len(ttl_files)
    processed = 0
    updated = 0
    errors = 0
    slides_processed = defaultdict(int)

    for i, file_path in enumerate(ttl_files, 1):
        if i % 100 == 0:
            logger.info(
                f"Progress: {i}/{total_files} files processed... "
                f"(updated: {updated}, cached hashes: {len(hash_cache)})"
            )

        success, was_updated, slide_id = process_ttl_file(file_path, auth)

        if success:
            processed += 1
            if was_updated:
                updated += 1
            if slide_id:
                slides_processed[slide_id] += 1
        else:
            errors += 1

    # Summary
    logger.info("=" * 80)
    logger.info("Summary:")
    logger.info(f"  Total files found: {total_files}")
    logger.info(f"  Successfully processed: {processed}")
    logger.info(f"  Files updated: {updated}")
    logger.info(f"  Files unchanged: {processed - updated}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Unique slides processed: {len(slides_processed)}")
    logger.info(f"  Hashes cached: {len(hash_cache)}")
    logger.info(f"  Failed node lookups: {len(failed_nodes)}")
    logger.info("=" * 80)


if __name__ == "__main__":
    with timer("TTL Hash Update"):
        main()
