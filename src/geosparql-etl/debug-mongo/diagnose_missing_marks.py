"""
Diagnostic script to identify missing marks in RDF conversion
Finds marks that exist in MongoDB but weren't processed
Author: Assistant
"""

import logging
import sys
from collections import defaultdict
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

# from utils import mongo_connection
from utils.mongo_connection import mongo_connection

# MongoDB connection settings - adjust as needed
MONGO_HOST = "172.18.0.2"  # Or "localhost"
MONGO_PORT = 27017  # Or 27018
MONGO_DB = "camic"
CHECKPOINT_DIR = Path("checkpoints")


def load_completed_analyses():
    """Load the list of completed analysis IDs from checkpoint"""
    completed = set()
    completed_file = CHECKPOINT_DIR / "completed_analyses.txt"

    if completed_file.exists():
        with open(completed_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    completed.add(line)

    return completed


def analyze_missing_marks():
    """Analyze what marks are missing and why"""

    logger.info("=" * 60)
    logger.info("MISSING MARKS DIAGNOSTIC")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        # 1. Get total counts
        total_marks = db.mark.count_documents({})
        total_analyses = db.analysis.count_documents({})

        logger.info(f"Total marks in MongoDB: {total_marks:,}")
        logger.info(f"Total analyses in MongoDB: {total_analyses:,}")

        # 2. Load completed analyses from checkpoint
        completed_analyses = load_completed_analyses()
        logger.info(f"Completed analyses from checkpoint: {len(completed_analyses):,}")

        # 3. Count marks by analysis_id presence
        logger.info("\nAnalyzing mark distribution...")

        # Sample to check for orphaned marks (marks without analysis)
        defaultdict(int)

        # Get all unique analysis_ids from marks collection
        logger.info(
            "Getting unique analysis_ids from marks collection (this may take a while)..."
        )
        unique_analysis_ids = db.mark.distinct("analysis_id")
        logger.info(
            f"Found {len(unique_analysis_ids):,} unique analysis_ids in marks collection"
        )

        # Check which analysis_ids exist in the analysis collection
        missing_analyses = []
        existing_analyses = []

        logger.info(
            "Checking which analysis_ids have corresponding analysis documents..."
        )
        for aid in unique_analysis_ids:
            if aid:  # Skip None/null values
                # Check if this analysis_id exists in the analysis collection
                exists = db.analysis.count_documents({"_id": aid}, limit=1) > 0
                if exists:
                    existing_analyses.append(aid)
                else:
                    missing_analyses.append(aid)

        logger.info(f"Analysis IDs with analysis documents: {len(existing_analyses):,}")
        logger.info(
            f"Analysis IDs WITHOUT analysis documents: {len(missing_analyses):,}"
        )

        # 4. Count marks for missing analyses
        if missing_analyses:
            logger.info("\nCounting marks for missing analyses (orphaned marks)...")
            orphaned_count = 0
            sample_missing = missing_analyses[:5]  # Sample first 5

            for aid in missing_analyses:
                count = db.mark.count_documents({"analysis_id": aid})
                orphaned_count += count

            logger.info(
                f"Total orphaned marks (no analysis document): {orphaned_count:,}"
            )

            # Show samples
            logger.info("\nSample of missing analysis_ids:")
            for aid in sample_missing:
                count = db.mark.count_documents({"analysis_id": aid})
                logger.info(f"  - {aid}: {count:,} marks")

        # 5. Check for marks with null/empty analysis_id
        null_analysis_marks = db.mark.count_documents({"analysis_id": None})
        empty_analysis_marks = db.mark.count_documents({"analysis_id": ""})

        logger.info(f"\nMarks with null analysis_id: {null_analysis_marks:,}")
        logger.info(f"Marks with empty string analysis_id: {empty_analysis_marks:,}")

        # 6. Count marks for completed vs incomplete analyses
        logger.info("\nAnalyzing completed vs incomplete analyses...")
        completed_marks_count = 0
        incomplete_marks_count = 0

        for aid in existing_analyses:
            mark_count = db.mark.count_documents({"analysis_id": aid})
            if str(aid) in completed_analyses:
                completed_marks_count += mark_count
            else:
                incomplete_marks_count += mark_count

        logger.info(f"Marks in completed analyses: {completed_marks_count:,}")
        logger.info(f"Marks in incomplete analyses: {incomplete_marks_count:,}")

        # 7. Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY OF MISSING MARKS:")
        logger.info("=" * 60)

        accounted_marks = completed_marks_count + incomplete_marks_count
        if missing_analyses:
            accounted_marks += orphaned_count
        accounted_marks += null_analysis_marks + empty_analysis_marks

        logger.info(f"Total marks in MongoDB: {total_marks:,}")
        logger.info(f"Marks accounted for: {accounted_marks:,}")
        logger.info(f"Marks unaccounted: {total_marks - accounted_marks:,}")

        logger.info("\nBreakdown:")
        logger.info(f"  - Marks in completed analyses: {completed_marks_count:,}")
        logger.info(f"  - Marks in incomplete analyses: {incomplete_marks_count:,}")
        if missing_analyses:
            logger.info(f"  - Orphaned marks (no analysis): {orphaned_count:,}")
        logger.info(f"  - Marks with null analysis_id: {null_analysis_marks:,}")
        logger.info(f"  - Marks with empty analysis_id: {empty_analysis_marks:,}")

        # 8. Check markup types
        logger.info("\n" + "=" * 60)
        logger.info("MARKUP TYPE ANALYSIS:")
        logger.info("=" * 60)

        # Get sample of marks to understand structure
        sample_mark = db.mark.find_one()
        if sample_mark:
            logger.info(f"Sample mark keys: {list(sample_mark.keys())}")
            if "properties" in sample_mark and sample_mark["properties"]:
                logger.info(
                    f"Sample properties keys: {list(sample_mark['properties'].keys())}"
                )

        # Try to identify computer vs human marks
        # This depends on your data structure - adjust field names as needed
        computer_marks = db.mark.count_documents(
            {"provenance.analysis.execution_id": {"$exists": True}}
        )
        logger.info(f"Marks with execution_id (likely computer): {computer_marks:,}")

        # Alternative way to check
        human_pattern = db.mark.count_documents(
            {"marktype": {"$exists": True, "$eq": "human"}}
        )
        computer_pattern = db.mark.count_documents(
            {"marktype": {"$exists": True, "$eq": "computer"}}
        )
        logger.info(f"Marks with marktype='human': {human_pattern:,}")
        logger.info(f"Marks with marktype='computer': {computer_pattern:,}")


if __name__ == "__main__":
    analyze_missing_marks()
