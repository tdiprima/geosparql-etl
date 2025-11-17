"""
Diagnostic script to identify missing marks in RDF conversion
FIXED VERSION - Uses correct field paths with available indexes
Author: Assistant
"""

import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import mongo_connection

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
    logger.info("MISSING MARKS DIAGNOSTIC (FIXED VERSION)")
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

        # 3. Analyze mark structure - get sample to understand schema
        logger.info("\n" + "=" * 60)
        logger.info("ANALYZING MARK SCHEMA:")
        logger.info("=" * 60)

        sample_mark = db.mark.find_one()
        if sample_mark:
            logger.info(f"Sample mark keys: {list(sample_mark.keys())}")

            # Check if analysis_id exists at all
            has_analysis_id = "analysis_id" in sample_mark
            logger.info(f"Has 'analysis_id' field: {has_analysis_id}")

            # Check provenance structure
            if "provenance" in sample_mark:
                logger.info(
                    f"Provenance keys: {list(sample_mark['provenance'].keys())}"
                )
                if "analysis" in sample_mark["provenance"]:
                    logger.info(
                        f"Provenance.analysis keys: {list(sample_mark['provenance']['analysis'].keys())}"
                    )

        # 4. Check different possible fields for analysis reference
        logger.info("\n" + "=" * 60)
        logger.info("CHECKING ANALYSIS REFERENCE FIELDS:")
        logger.info("=" * 60)

        # Check marks with execution_id (uses index!)
        marks_with_execution_id = db.mark.count_documents(
            {"provenance.analysis.execution_id": {"$exists": True, "$ne": None}}
        )
        logger.info(
            f"Marks with provenance.analysis.execution_id: {marks_with_execution_id:,}"
        )

        # Check marks without execution_id
        marks_without_execution_id = db.mark.count_documents(
            {
                "$or": [
                    {"provenance.analysis.execution_id": {"$exists": False}},
                    {"provenance.analysis.execution_id": None},
                ]
            }
        )
        logger.info(f"Marks WITHOUT execution_id: {marks_without_execution_id:,}")

        # Check if analysis_id field exists at all
        if has_analysis_id:
            logger.info("\nChecking analysis_id field (WARNING: NO INDEX - SLOW!)...")
            marks_with_analysis_id = db.mark.count_documents(
                {"analysis_id": {"$exists": True, "$ne": None, "$ne": ""}}
            )
            logger.info(f"Marks with non-null analysis_id: {marks_with_analysis_id:,}")

            null_analysis_marks = db.mark.count_documents({"analysis_id": None})
            empty_analysis_marks = db.mark.count_documents({"analysis_id": ""})
            logger.info(f"Marks with null analysis_id: {null_analysis_marks:,}")
            logger.info(
                f"Marks with empty string analysis_id: {empty_analysis_marks:,}"
            )

        # 5. Check mark types using indexed fields
        logger.info("\n" + "=" * 60)
        logger.info("MARK TYPE ANALYSIS (using indexed fields):")
        logger.info("=" * 60)

        # Check by source (uses index!)
        human_marks = db.mark.count_documents({"provenance.analysis.source": "human"})
        computer_marks = db.mark.count_documents(
            {"provenance.analysis.source": {"$ne": "human"}}
        )

        logger.info(f"Marks with source='human': {human_marks:,}")
        logger.info(f"Marks with source != 'human': {computer_marks:,}")

        # Get unique sources (uses index!)
        logger.info("\nGetting unique sources...")
        unique_sources = db.mark.distinct("provenance.analysis.source")
        logger.info(f"Unique sources: {unique_sources}")

        for source in unique_sources:
            count = db.mark.count_documents({"provenance.analysis.source": source})
            logger.info(f"  - {source}: {count:,} marks")

        # 6. Check execution_id distribution (uses index!)
        logger.info("\n" + "=" * 60)
        logger.info("EXECUTION ID ANALYSIS:")
        logger.info("=" * 60)

        logger.info("Getting unique execution_ids (this may take a while)...")
        unique_execution_ids = db.mark.distinct("provenance.analysis.execution_id")
        logger.info(f"Found {len(unique_execution_ids):,} unique execution_ids")

        # Sample a few execution_ids
        sample_size = min(5, len(unique_execution_ids))
        sample_execution_ids = unique_execution_ids[:sample_size]

        logger.info("\nSample execution_ids and their mark counts:")
        for exec_id in sample_execution_ids:
            if exec_id:  # Skip None
                # This uses index!
                count = db.mark.count_documents(
                    {"provenance.analysis.execution_id": exec_id}
                )
                logger.info(f"  - {exec_id}: {count:,} marks")

        # 7. Check if execution_ids match analysis _ids
        logger.info("\n" + "=" * 60)
        logger.info("CHECKING EXECUTION_ID vs ANALYSIS _ID MATCH:")
        logger.info("=" * 60)

        # Get sample of analysis _ids
        sample_analyses = list(db.analysis.find({}, {"_id": 1}).limit(5))

        logger.info("Checking if analysis _ids appear as execution_ids in marks...")
        for analysis in sample_analyses:
            analysis_id = analysis["_id"]
            # This uses index!
            mark_count = db.mark.count_documents(
                {"provenance.analysis.execution_id": analysis_id}
            )
            logger.info(
                f"  Analysis {analysis_id}: {mark_count:,} marks with this execution_id"
            )

        # 8. Recommendations
        logger.info("\n" + "=" * 60)
        logger.info("RECOMMENDATIONS:")
        logger.info("=" * 60)

        logger.info(
            """
Based on the available indexes, your scripts should:

1. USE 'provenance.analysis.execution_id' instead of 'analysis_id'
   - This field is indexed and will be much faster
   - Check if execution_id matches analysis._id

2. USE 'provenance.analysis.source' for filtering human vs computer marks
   - This field is indexed

3. IF you must use 'analysis_id':
   - Create an index first: db.mark.createIndex({"analysis_id": 1})
   - Otherwise queries will be extremely slow (full collection scans)

4. Consider that:
   - execution_id might BE the analysis reference
   - Marks might use execution_id instead of analysis_id
   - You may not have "orphaned" marks - they might just use different fields
        """
        )


if __name__ == "__main__":
    analyze_missing_marks()
