"""
Fix TTL files to use 'hal' namespace instead of 'camic'
Traverses ttl_output directory and updates all batch_*.ttl.gz files
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
from multiprocessing import Pool, cpu_count

# Configuration
TTL_OUTPUT_DIR = Path("ttl_output")
NUM_WORKERS = 20  # Use 20 cores for processing, leave 4 for system

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'fix_ttl_identifiers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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

def fix_ttl_content(content):
    """
    Apply all necessary fixes to TTL content.
    Returns (fixed_content, changes_made)
    """
    changes = []

    # 1. Fix prefix declaration
    if '@prefix camic:' in content:
        content = content.replace(
            '@prefix camic: <http://example.org/camic#> .',
            '@prefix hal: <https://halcyon.is/ns/> .'
        )
        changes.append("Fixed prefix declaration")

    # 2. Fix image identifier pattern: camic:image_{hash} -> <urn:sha256:{hash}>
    pattern = r'camic:image_([0-9a-f]{64})'
    if re.search(pattern, content):
        content = re.sub(pattern, r'<urn:sha256:\1>', content)
        changes.append("Fixed image identifiers")

    # 3. Replace all remaining camic: property references with hal:
    if 'camic:' in content:
        content = re.sub(r'\bcamic:', 'hal:', content)
        changes.append("Replaced camic: with hal:")

    return content, changes

def process_ttl_file(file_path):
    """
    Worker function to process a single TTL.gz file.
    Returns (success, was_updated, file_path_str)
    """
    try:
        # Read compressed file
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            original_content = f.read()

        # Apply fixes
        fixed_content, changes = fix_ttl_content(original_content)

        # Check if any changes were made
        if not changes:
            return True, False, str(file_path)

        # Write back to file (compressed)
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            f.write(fixed_content)

        return True, True, str(file_path)

    except Exception as e:
        return False, False, f"ERROR: {file_path}: {e}"

def main():
    """Main function to process all TTL files in parallel"""
    logger.info("=" * 80)
    logger.info("TTL Identifier Fix Script (PARALLEL)")
    logger.info(f"Directory: {TTL_OUTPUT_DIR.absolute()}")
    logger.info(f"Workers: {NUM_WORKERS}")
    logger.info("=" * 80)

    if not TTL_OUTPUT_DIR.exists():
        logger.error(f"Directory not found: {TTL_OUTPUT_DIR}")
        return

    # Find all batch_*.ttl.gz files
    pattern = "batch_*.ttl.gz"
    ttl_files = list(TTL_OUTPUT_DIR.rglob(pattern))

    if not ttl_files:
        logger.warning(f"No files matching '{pattern}' found in {TTL_OUTPUT_DIR}")
        return

    logger.info(f"Found {len(ttl_files):,} TTL files to process")

    # Process files in parallel
    total_files = len(ttl_files)
    processed = 0
    updated = 0
    errors = 0

    with Pool(processes=NUM_WORKERS) as pool:
        # Process in chunks and show progress
        chunk_size = 1000
        for i, result in enumerate(pool.imap_unordered(process_ttl_file, ttl_files, chunksize=chunk_size), 1):
            success, was_updated, file_info = result

            if success:
                processed += 1
                if was_updated:
                    updated += 1
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
    logger.info("=" * 80)

if __name__ == "__main__":
    start_time = time.time()
    with timer("TTL Identifier Fix"):
        main()
