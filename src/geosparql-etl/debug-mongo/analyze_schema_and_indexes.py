"""
Schema analysis and index recommendation script - FAST VERSION
Only uses indexed queries - safe for huge databases
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

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"


def analyze_schema_fast():
    """Analyze schema using ONLY fast, indexed queries"""

    logger.info("=" * 60)
    logger.info("FAST SCHEMA ANALYSIS (Safe for Huge Databases)")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # 1. Get total counts (fast - just metadata)
        # logger.info("\n1. Collection Statistics:")
        # total_marks = db.mark.count_documents({})
        # total_analyses = db.analysis.count_documents({})
        # logger.info(f"Total marks: {total_marks:,}")
        # logger.info(f"Total analyses: {total_analyses:,}")

        # 2. Sample a few marks to understand structure (fast - only 10 docs)
        logger.info("\n2. Sampling Mark Structure (10 samples):")
        samples = list(db.mark.find({}).limit(10))

        if not samples:
            logger.error("No marks found in collection!")
            return

        # Check field presence
        has_analysis_id = False
        has_execution_id = False
        has_source = False

        for mark in samples:
            if "analysis_id" in mark:
                has_analysis_id = True

            if "provenance" in mark and "analysis" in mark["provenance"]:
                if "execution_id" in mark["provenance"]["analysis"]:
                    has_execution_id = True
                if "source" in mark["provenance"]["analysis"]:
                    has_source = True

        logger.info(f"  analysis_id field exists: {has_analysis_id}")
        logger.info(f"  provenance.analysis.execution_id exists: {has_execution_id}")
        logger.info(f"  provenance.analysis.source exists: {has_source}")

        # Show first sample structure
        sample = samples[0]
        logger.info(f"\n  Sample mark top-level keys: {list(sample.keys())}")

        if "analysis_id" in sample:
            logger.info(f"    analysis_id: {sample['analysis_id']}")

        if "provenance" in sample:
            logger.info(f"    provenance keys: {list(sample['provenance'].keys())}")

            if "analysis" in sample["provenance"]:
                logger.info(
                    f"    provenance.analysis keys: {list(sample['provenance']['analysis'].keys())}"
                )

                analysis = sample["provenance"]["analysis"]
                if "execution_id" in analysis:
                    logger.info(f"      execution_id: {analysis['execution_id']}")
                if "source" in analysis:
                    logger.info(f"      source: {analysis['source']}")

        # 3. Test if execution_id matches analysis._id (fast - indexed query)
        logger.info("\n3. Testing execution_id → analysis._id Relationship:")

        if has_execution_id:
            # Get a sample execution_id (uses index!)
            mark_with_exec = db.mark.find_one(
                {"provenance.analysis.execution_id": {"$exists": True, "$ne": None}},
                {"provenance.analysis.execution_id": 1},
            )

            if mark_with_exec:
                exec_id = mark_with_exec["provenance"]["analysis"]["execution_id"]
                logger.info(f"  Sample execution_id: {exec_id}")

                # Check if this exists as analysis._id (uses index!)
                analysis_exists = db.analysis.find_one({"_id": exec_id})
                if analysis_exists:
                    logger.info("  ✅ SUCCESS: execution_id MATCHES analysis._id")
                    logger.info(
                        "  → Use provenance.analysis.execution_id for queries (INDEXED)"
                    )
                else:
                    logger.info("  ✗ execution_id does NOT match analysis._id")
        else:
            logger.info("  No execution_id field found")

        # 4. Check current indexes
        logger.info("\n4. Current Indexes on mark collection:")
        indexes = db.mark.index_information()

        has_analysis_id_index = False

        for name, info in indexes.items():
            key_str = str(info["key"])
            logger.info(f"  [{name}]: {info['key']}")

            if "analysis_id" in key_str:
                has_analysis_id_index = True
            if "execution_id" in key_str:
                pass

        # 5. Count using INDEXED fields only (fast!)
        # logger.info("\n5. Mark Counts Using INDEXED Fields:")

        # if has_execution_id:
        #     # This uses index - FAST!
        #     marks_with_exec = db.mark.count_documents(
        #         {"provenance.analysis.execution_id": {"$exists": True, "$ne": None}}
        #     )
        #     logger.info(f"  Marks with execution_id: {marks_with_exec:,}")

        # if has_source:
        #     # This uses index - FAST!
        #     human_marks = db.mark.count_documents(
        #         {"provenance.analysis.source": "human"}
        #     )
        #     computer_marks = db.mark.count_documents(
        #         {"provenance.analysis.source": {"$ne": "human"}}
        #     )
        #     logger.info(f"  Human marks: {human_marks:,}")
        #     logger.info(f"  Computer marks: {computer_marks:,}")

        # 6. RECOMMENDATIONS
        logger.info("\n" + "=" * 60)
        logger.info("RECOMMENDATIONS:")
        logger.info("=" * 60)

        if has_execution_id and not has_analysis_id:
            logger.info(
                """
✅ GOOD NEWS: Your marks use execution_id (which is INDEXED)

RECOMMENDATION: Use execution_id instead of analysis_id
- Change your queries from: {"analysis_id": X}
- To: {"provenance.analysis.execution_id": X}
- This field is already indexed - queries will be FAST
- No index creation needed!

Update your scripts to use:
  db.mark.find({"provenance.analysis.execution_id": analysis_id})
            """
            )

        elif has_analysis_id and not has_analysis_id_index:
            logger.info(
                """
⚠️  WARNING: analysis_id field exists but has NO INDEX

CRITICAL: You MUST create an index before running ETL scripts!

To create index, run ONE of these:

Option 1 - Using this script:
  python analyze_schema_and_indexes.py --create-index

Option 2 - Manually in mongo shell:
  mongo camic
  > db.mark.createIndex({"analysis_id": 1}, {background: true, name: "analysis_id_1"})

This will take time but is essential for performance.
Without this index, queries will take HOURS or DAYS.
            """
            )

        elif has_analysis_id and has_analysis_id_index:
            logger.info(
                """
✅ EXCELLENT: analysis_id field exists AND is indexed

Your database is ready for ETL processing!
Queries on analysis_id will be fast.
            """
            )

        else:
            logger.info(
                """
⚠️  UNCLEAR: Need to investigate further

Neither analysis_id nor execution_id seem to be the primary reference.
Check your sample mark structure above to understand the schema.
            """
            )

        # 7. Show correct query patterns
        logger.info("\n" + "=" * 60)
        logger.info("CORRECT QUERY PATTERNS FOR YOUR DATABASE:")
        logger.info("=" * 60)

        if has_execution_id:
            logger.info(
                """
For querying by analysis (USES INDEX - FAST):
  db.mark.find({"provenance.analysis.execution_id": some_analysis_id})
  db.mark.count_documents({"provenance.analysis.execution_id": some_analysis_id})
            """
            )

        if has_analysis_id and has_analysis_id_index:
            logger.info(
                """
For querying by analysis_id (USES INDEX - FAST):
  db.mark.find({"analysis_id": some_id})
  db.mark.count_documents({"analysis_id": some_id})
            """
            )

        if has_source:
            logger.info(
                """
For filtering by human vs computer (USES INDEX - FAST):
  db.mark.find({"provenance.analysis.source": "human"})
  db.mark.find({"provenance.analysis.source": {"$ne": "human"}})
            """
            )

        # 8. WARNING about slow queries
        logger.info("\n" + "=" * 60)
        logger.info("⚠️  QUERIES TO AVOID (SLOW - NO INDEX):")
        logger.info("=" * 60)

        if has_analysis_id and not has_analysis_id_index:
            logger.info(
                """
NEVER run these until index is created:
  ❌ db.mark.find({"analysis_id": X})
  ❌ db.mark.count_documents({"analysis_id": None})
  ❌ db.mark.distinct("analysis_id")

These will cause FULL COLLECTION SCAN (hours/days on huge database)
            """
            )


def create_analysis_id_index():
    """Create index on analysis_id"""

    logger.info("\n" + "=" * 60)
    logger.info("CREATING analysis_id INDEX...")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # Check if field exists first (sample only - fast)
        sample = db.mark.find_one({"analysis_id": {"$exists": True}})

        if not sample:
            logger.info("✗ analysis_id field doesn't exist - no index needed")
            logger.info("  Your marks probably use execution_id instead")
            return

        # Check if index already exists
        indexes = db.mark.index_information()
        has_index = any("analysis_id" in str(info["key"]) for info in indexes.values())

        if has_index:
            logger.info("✓ analysis_id index already exists")
            return

        # Create the index
        logger.info("Creating index on analysis_id...")
        logger.info(
            "This may take several minutes to hours depending on collection size..."
        )
        logger.info(
            "The index is created in background mode, so database remains available."
        )

        try:
            result = db.mark.create_index(
                [("analysis_id", 1)], background=True, name="analysis_id_1"
            )
            logger.info(f"✅ Index created successfully: {result}")
            logger.info("\nYou can now run your ETL scripts with fast queries!")

        except Exception as e:
            logger.error(f"✗ Error creating index: {e}")
            logger.error("\nTry creating manually in mongo shell:")
            logger.error("  mongo camic")
            logger.error(
                '  > db.mark.createIndex({"analysis_id": 1}, {background: true})'
            )


if __name__ == "__main__":
    import sys

    # Run fast analysis
    analyze_schema_fast()

    # If --create-index flag, create the index
    if "--create-index" in sys.argv:
        create_analysis_id_index()
    else:
        logger.info("\n" + "=" * 60)
        logger.info("To create index, run:")
        logger.info("  python analyze_schema_and_indexes_fast.py --create-index")
        logger.info("=" * 60)
