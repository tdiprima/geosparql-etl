"""
Schema analysis and index recommendation script
Helps understand the actual data structure and creates needed indexes
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

# MongoDB connection settings
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"


def analyze_schema():
    """Analyze the actual schema to understand how marks reference analyses"""

    logger.info("=" * 60)
    logger.info("SCHEMA ANALYSIS & INDEX RECOMMENDATIONS")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # 1. Sample multiple marks to understand schema
        logger.info("\n1. Sampling 10 marks to understand schema...")
        samples = list(db.mark.find({}).limit(10))

        # Check which fields exist across samples
        field_presence = {
            "analysis_id": 0,
            "provenance.analysis.execution_id": 0,
            "provenance.analysis": 0,
            "provenance.analysis.source": 0,
        }

        for mark in samples:
            if "analysis_id" in mark:
                field_presence["analysis_id"] += 1

            if "provenance" in mark and "analysis" in mark["provenance"]:
                field_presence["provenance.analysis"] += 1

                if "execution_id" in mark["provenance"]["analysis"]:
                    field_presence["provenance.analysis.execution_id"] += 1

                if "source" in mark["provenance"]["analysis"]:
                    field_presence["provenance.analysis.source"] += 1

        logger.info("\nField presence in 10 sample marks:")
        for field, count in field_presence.items():
            logger.info(f"  {field}: {count}/10 marks")

        # 2. Show a complete sample mark structure
        logger.info("\n2. Sample mark structure:")
        if samples:
            sample = samples[0]
            logger.info(f"Top-level keys: {list(sample.keys())}")

            if "analysis_id" in sample:
                logger.info(f"  analysis_id: {sample['analysis_id']}")

            if "provenance" in sample:
                logger.info(f"  provenance keys: {list(sample['provenance'].keys())}")

                if "analysis" in sample["provenance"]:
                    logger.info(
                        f"  provenance.analysis keys: {list(sample['provenance']['analysis'].keys())}"
                    )

                    # Show some values
                    analysis = sample["provenance"]["analysis"]
                    if "execution_id" in analysis:
                        logger.info(f"    execution_id: {analysis['execution_id']}")
                    if "source" in analysis:
                        logger.info(f"    source: {analysis['source']}")

        # 3. Compare execution_id with analysis _id
        logger.info("\n3. Checking if execution_id matches analysis _id...")

        # Get a mark with execution_id
        mark_with_exec = db.mark.find_one(
            {"provenance.analysis.execution_id": {"$exists": True, "$ne": None}}
        )

        if mark_with_exec:
            exec_id = mark_with_exec["provenance"]["analysis"]["execution_id"]
            logger.info(f"Sample execution_id: {exec_id}")

            # Check if this exists as analysis _id
            analysis_exists = db.analysis.find_one({"_id": exec_id})
            if analysis_exists:
                logger.info("✓ Execution_id MATCHES analysis._id")
                logger.info("  This means execution_id IS the analysis reference!")
            else:
                logger.info("✗ Execution_id does NOT match any analysis._id")
                logger.info("  Need to investigate further...")

        # 4. Check if analysis_id field exists and what it contains
        logger.info("\n4. Checking analysis_id field (if it exists)...")

        mark_with_analysis_id = db.mark.find_one({"analysis_id": {"$exists": True}})
        if mark_with_analysis_id:
            logger.info("✓ analysis_id field EXISTS")
            analysis_id_value = mark_with_analysis_id.get("analysis_id")
            logger.info(f"  Sample value: {analysis_id_value}")

            # Check if this matches analysis _id
            if analysis_id_value:
                analysis_exists = db.analysis.find_one({"_id": analysis_id_value})
                if analysis_exists:
                    logger.info("  ✓ analysis_id MATCHES analysis._id")
                else:
                    logger.info("  ✗ analysis_id does NOT match any analysis._id")
        else:
            logger.info("✗ analysis_id field does NOT exist in marks")

        # 5. Index recommendations
        logger.info("\n" + "=" * 60)
        logger.info("INDEX RECOMMENDATIONS:")
        logger.info("=" * 60)

        # Check current indexes
        current_indexes = db.mark.index_information()
        logger.info("\nCurrent indexes on mark collection:")
        for name, info in current_indexes.items():
            logger.info(f"  - {name}: {info['key']}")

        # Determine what indexes are needed
        logger.info("\n" + "=" * 60)
        logger.info("RECOMMENDED ACTIONS:")
        logger.info("=" * 60)

        if mark_with_analysis_id:
            # Check if analysis_id index exists
            has_analysis_id_index = any(
                "analysis_id" in str(info["key"]) for info in current_indexes.values()
            )

            if not has_analysis_id_index:
                logger.info(
                    """
1. CREATE INDEX on analysis_id:
   
   Run in mongo shell:
   use camic
   db.mark.createIndex({"analysis_id": 1}, {background: true})
   
   This will enable your scripts to query by analysis_id efficiently.
   Using background:true means it won't block other operations.
   This may take a while depending on collection size.
                """
                )
            else:
                logger.info("✓ analysis_id index already exists")

        # Recommend using execution_id if it's the actual reference
        if mark_with_exec and exec_id:
            logger.info(
                """
2. CONSIDER using execution_id instead of analysis_id:
   
   If execution_id matches analysis._id, you should:
   - Query marks using: {"provenance.analysis.execution_id": analysis_id}
   - This field is already indexed!
   - Much faster than analysis_id queries
                """
            )

        # 6. Generate corrected query examples
        logger.info("\n" + "=" * 60)
        logger.info("CORRECTED QUERY EXAMPLES:")
        logger.info("=" * 60)

        logger.info(
            """
Instead of:
  db.mark.find({"analysis_id": some_id})

Use one of these (depending on your schema):

Option A - If using execution_id (INDEXED):
  db.mark.find({"provenance.analysis.execution_id": some_id})

Option B - If using analysis_id (requires index):
  # First create index:
  db.mark.createIndex({"analysis_id": 1}, {background: true})
  # Then query:
  db.mark.find({"analysis_id": some_id})

For human vs computer marks (INDEXED):
  db.mark.find({"provenance.analysis.source": "human"})
  db.mark.find({"provenance.analysis.source": {"$ne": "human"}})
        """
        )


def create_analysis_id_index_if_needed():
    """Create index on analysis_id if the field exists and index doesn't"""

    logger.info("\n" + "=" * 60)
    logger.info("CHECKING IF INDEX CREATION IS NEEDED:")
    logger.info("=" * 60)

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

        # Check if field exists
        has_field = db.mark.find_one({"analysis_id": {"$exists": True}}) is not None

        if not has_field:
            logger.info("✗ analysis_id field doesn't exist - no index needed")
            return

        # Check if index exists
        indexes = db.mark.index_information()
        has_index = any("analysis_id" in str(info["key"]) for info in indexes.values())

        if has_index:
            logger.info("✓ analysis_id index already exists")
            return

        # Ask user if they want to create index
        logger.info(
            """
⚠ analysis_id field exists but has NO index!

This will make your queries EXTREMELY slow.

To create the index, run this in mongo shell:
  use camic
  db.mark.createIndex({"analysis_id": 1}, {background: true, name: "analysis_id_1"})

Or run this script with --create-index flag to do it automatically.
        """
        )


if __name__ == "__main__":
    import sys

    analyze_schema()
    create_analysis_id_index_if_needed()

    # If --create-index flag, actually create the index
    if "--create-index" in sys.argv:
        logger.info("\n" + "=" * 60)
        logger.info("CREATING analysis_id INDEX...")
        logger.info("=" * 60)

        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            logger.info("Creating index... this may take several minutes...")
            try:
                result = db.mark.create_index(
                    [("analysis_id", 1)], background=True, name="analysis_id_1"
                )
                logger.info(f"✓ Index created successfully: {result}")
            except Exception as e:
                logger.error(f"✗ Error creating index: {e}")
