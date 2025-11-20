"""
Update image hashes in TTL files using actual file SHA256 values
Uses sha256_pipeline to get correct hash from Drupal node/slide ID
PARALLEL VERSION - uses multiprocessing for speed
Author: Bear 🐻
"""

import gzip
import re
import time
from pathlib import Path
import logging
from datetime import datetime
from contextlib import contextmanager
from multiprocessing import Pool, Manager, cpu_count

from sha256_pipeline import get_real_hash_from_node, get_auth

# Configuration
TTL_OUTPUT_DIR = Path("ttl_output")
NUM_WORKERS = 20  # Use 20 cores for processing, leave 4 for system/MongoDB

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'update_ttl_hashes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
    hash_match = re.search(r'<urn:sha256:([0-9a-f]{64})>', content)
    if not hash_match:
        return slide_id, None

    old_hash = hash_match.group(1)

    return slide_id, old_hash

def get_correct_hash(slide_id, auth, hash_cache, failed_nodes):
    """
    Get the correct SHA256 hash for a slide/node.
    Uses shared cache to avoid redundant lookups.
    Returns hash string or None if failed.
    """
    # Check cache first (shared dict)
    if slide_id in hash_cache:
        return hash_cache[slide_id]

    # Check if we've already failed on this node
    if slide_id in failed_nodes:
        return None

    try:
        correct_hash = get_real_hash_from_node(slide_id, auth=auth)
        hash_cache[slide_id] = correct_hash
        return correct_hash
    except Exception as e:
        failed_nodes[slide_id] = True  # Mark as failed in shared dict
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

def process_ttl_file_worker(args):
    """
    Worker function to process a single TTL.gz file.
    Returns (success, updated_bool, slide_id, file_path_str)
    """
    file_path, auth, hash_cache, failed_nodes = args

    try:
        # Read compressed file
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            content = f.read()

        # Extract slide_id and current hash
        slide_id, old_hash = extract_slide_id_and_hash(content)

        if slide_id is None:
            return True, False, None, str(file_path)

        if old_hash is None:
            return True, False, slide_id, str(file_path)

        # Get correct hash from sha256_pipeline (with shared cache)
        correct_hash = get_correct_hash(slide_id, auth, hash_cache, failed_nodes)

        if correct_hash is None:
            # Failed to get hash, skip this file
            return True, False, slide_id, str(file_path)

        # Check if hash needs updating
        if old_hash == correct_hash:
            # Hash is already correct
            return True, False, slide_id, str(file_path)

        # Update the hash in content
        updated_content = update_hash_in_content(content, old_hash, correct_hash)

        # Write back to file (compressed)
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            f.write(updated_content)

        return True, True, slide_id, str(file_path)

    except Exception as e:
        return False, False, None, f"ERROR: {file_path}: {e}"

def main():
    """Main function to process all TTL files in parallel"""
    logger.info("=" * 80)
    logger.info("TTL Hash Update Script (PARALLEL)")
    logger.info(f"Directory: {TTL_OUTPUT_DIR.absolute()}")
    logger.info(f"Workers: {NUM_WORKERS}")
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

    logger.info(f"Found {len(ttl_files):,} TTL files to process")

    # Create shared cache and failed nodes tracker using Manager
    with Manager() as manager:
        hash_cache = manager.dict()
        failed_nodes = manager.dict()

        # Prepare worker arguments
        worker_args = [(file_path, auth, hash_cache, failed_nodes) for file_path in ttl_files]

        # Process files in parallel
        total_files = len(ttl_files)
        processed = 0
        updated = 0
        errors = 0
        slides_seen = set()

        with Pool(processes=NUM_WORKERS) as pool:
            chunk_size = 1000
            for i, result in enumerate(pool.imap_unordered(process_ttl_file_worker, worker_args, chunksize=chunk_size), 1):
                success, was_updated, slide_id, file_info = result

                if success:
                    processed += 1
                    if was_updated:
                        updated += 1
                    if slide_id:
                        slides_seen.add(slide_id)
                else:
                    errors += 1
                    logger.error(file_info)

                # Progress every 1000 files
                if i % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta_seconds = (total_files - i) / rate if rate > 0 else 0
                    logger.info(f"Progress: {i:,}/{total_files:,} files "
                               f"({100*i/total_files:.1f}%) | "
                               f"Updated: {updated:,} | "
                               f"Cached hashes: {len(hash_cache)} | "
                               f"Rate: {rate:.0f} files/sec | "
                               f"ETA: {eta_seconds/60:.1f} min")

        # Summary
        logger.info("=" * 80)
        logger.info("Summary:")
        logger.info(f"  Total files found: {total_files:,}")
        logger.info(f"  Successfully processed: {processed:,}")
        logger.info(f"  Files updated: {updated:,}")
        logger.info(f"  Files unchanged: {processed - updated:,}")
        logger.info(f"  Errors: {errors}")
        logger.info(f"  Unique slides processed: {len(slides_seen)}")
        logger.info(f"  Hashes cached: {len(hash_cache)}")
        logger.info(f"  Failed node lookups: {len(failed_nodes)}")
        logger.info("=" * 80)

if __name__ == "__main__":
    start_time = time.time()
    with timer("TTL Hash Update"):
        main()
