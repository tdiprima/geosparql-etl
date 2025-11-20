"""
Fast Mark Recovery Pipeline for MongoDB to RDF ETL
Optimized version that uses sampling and aggregation instead of full counts
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Add parent directory to path if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from utils import mongo_connection
except ImportError:
    print("Warning: utils module not found, using pymongo directly")
    from contextlib import contextmanager

    from pymongo import MongoClient

    @contextmanager
    def mongo_connection(uri, db_name):
        client = MongoClient(uri)
        try:
            yield client[db_name]
        finally:
            client.close()


# Configuration
MONGO_HOST = "172.18.0.2"
MONGO_PORT = 27017
MONGO_DB = "camic"
CHECKPOINT_DIR = Path("checkpoints")
OUTPUT_DIR = Path("recovery_reports")

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "recovery_pipeline_fast.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("FastRecovery")


class FastMarkRecoveryAnalyzer:
    """Fast analyzer using aggregation and sampling"""

    def __init__(self, checkpoint_dir: Path, mongo_uri: str, db_name: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.failed_analyses = []
        self.completed_analyses = set()
        self.stats = {}

    def load_checkpoint_data(self) -> Tuple[Set[str], List[Dict]]:
        """Load completed and failed analyses from checkpoint files"""

        # Load completed analyses
        completed_file = self.checkpoint_dir / "completed_analyses.txt"
        if completed_file.exists():
            logger.info(f"Reading completed analyses from {completed_file}...")
            with open(completed_file, "r") as f:
                self.completed_analyses = {line.strip() for line in f if line.strip()}

        # Load failed analyses with error details
        failed_file = self.checkpoint_dir / "failed_analyses.txt"
        if failed_file.exists():
            logger.info(f"Reading failed analyses from {failed_file}...")
            with open(failed_file, "r") as f:
                for line in f:
                    if "|" in line:
                        parts = line.strip().split("|", 1)
                        analysis_id = parts[0]
                        error_msg = parts[1] if len(parts) > 1 else "Unknown error"
                        self.failed_analyses.append(
                            {"analysis_id": analysis_id, "error": error_msg}
                        )

        logger.info(f"Loaded {len(self.completed_analyses)} completed analyses")
        logger.info(f"Loaded {len(self.failed_analyses)} failed analyses")

        return self.completed_analyses, self.failed_analyses

    def analyze_missing_marks_fast(self) -> Dict:
        """Fast analysis using aggregation and estimation"""

        logger.info("Connecting to MongoDB for fast analysis...")

        with mongo_connection(self.mongo_uri, self.db_name) as db:
            start_time = time.time()

            # Use estimatedDocumentCount for speed (uses metadata, very fast)
            logger.info("Getting estimated document counts...")
            total_analyses_estimate = db.analysis.estimated_document_count()
            total_marks_estimate = db.mark.estimated_document_count()

            logger.info(f"Estimated total analyses: {total_analyses_estimate:,}")
            logger.info(f"Estimated total marks: {total_marks_estimate:,}")

            # For failed analyses, get the actual list we have
            failed_analysis_details = []
            if self.failed_analyses:
                logger.info(f"Analyzing {len(self.failed_analyses)} failed analyses...")

                # Just get basic info for failed analyses (no mark counting yet)
                for failed in self.failed_analyses[:10]:  # Sample first 10
                    analysis_id = failed["analysis_id"]
                    failed_analysis_details.append(
                        {"analysis_id": analysis_id, "error": failed["error"]}
                    )

            # Quick sampling to estimate marks per analysis
            logger.info("Estimating marks per analysis using sampling...")
            sample_pipeline = [
                {"$sample": {"size": 10}},  # Just sample 10 analyses
                {
                    "$lookup": {
                        "from": "mark",
                        "let": {
                            "exec_id": "$analysis.execution_id",
                            "img_id": "$image.imageid",
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$provenance.analysis.execution_id",
                                                    "$$exec_id",
                                                ]
                                            },
                                            {
                                                "$eq": [
                                                    "$provenance.image.imageid",
                                                    "$$img_id",
                                                ]
                                            },
                                        ]
                                    }
                                }
                            },
                            {"$count": "mark_count"},
                        ],
                        "as": "mark_info",
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "mark_count": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$mark_info.mark_count", 0]},
                                0,
                            ]
                        },
                    }
                },
            ]

            try:
                logger.info("Running aggregation pipeline...")
                sample_results = list(
                    db.analysis.aggregate(sample_pipeline, allowDiskUse=True)
                )

                if sample_results:
                    avg_marks = sum(r["mark_count"] for r in sample_results) / len(
                        sample_results
                    )
                    logger.info(
                        f"Average marks per analysis (from sample): {avg_marks:.0f}"
                    )
                else:
                    avg_marks = 1000000  # Default estimate if sampling fails
                    logger.warning(
                        "Could not sample, using default estimate of 1M marks per analysis"
                    )
            except Exception as e:
                logger.warning(f"Sampling failed: {e}, using default estimates")
                avg_marks = 1000000

            # Calculate estimates
            unprocessed_analyses = (
                total_analyses_estimate
                - len(self.completed_analyses)
                - len(self.failed_analyses)
            )

            self.stats = {
                "total_analyses_estimate": total_analyses_estimate,
                "total_marks_estimate": total_marks_estimate,
                "completed_analyses": len(self.completed_analyses),
                "failed_analyses": len(self.failed_analyses),
                "unprocessed_analyses": max(0, unprocessed_analyses),
                "avg_marks_per_analysis": avg_marks,
                "estimated_marks_in_completed": int(
                    len(self.completed_analyses) * avg_marks
                ),
                "estimated_marks_in_failed": int(len(self.failed_analyses) * avg_marks),
                "estimated_marks_in_unprocessed": int(
                    max(0, unprocessed_analyses) * avg_marks
                ),
                "failed_analysis_details": failed_analysis_details,
                "analysis_time_seconds": time.time() - start_time,
            }

            return self.stats

    def generate_recovery_plan(self) -> Dict:
        """Generate a recovery plan for missing marks"""

        recovery_plan = {
            "timestamp": datetime.now().isoformat(),
            "summary": self.stats,
            "recovery_strategies": [],
        }

        # Strategy 1: Retry failed analyses
        if self.failed_analyses:
            recovery_plan["recovery_strategies"].append(
                {
                    "name": "Retry Failed Analyses",
                    "description": "Reprocess analyses that failed due to file system errors",
                    "target_count": len(self.failed_analyses),
                    "estimated_marks": self.stats.get("estimated_marks_in_failed", 0),
                    "priority": "HIGH",
                    "implementation": "Check failed_analyses.txt for IDs",
                }
            )

        # Strategy 2: Process unprocessed analyses
        unprocessed = self.stats.get("unprocessed_analyses", 0)
        if unprocessed > 0:
            recovery_plan["recovery_strategies"].append(
                {
                    "name": "Process Unprocessed Analyses",
                    "description": "Process analyses that were never attempted",
                    "target_count": unprocessed,
                    "estimated_marks": self.stats.get(
                        "estimated_marks_in_unprocessed", 0
                    ),
                    "priority": "MEDIUM",
                    "implementation": "Run main ETL with current checkpoint",
                }
            )

        # Calculate total missing
        total_missing = self.stats.get("estimated_marks_in_failed", 0) + self.stats.get(
            "estimated_marks_in_unprocessed", 0
        )

        recovery_plan["total_missing_marks_estimate"] = total_missing
        recovery_plan["missing_percentage"] = (
            total_missing / self.stats.get("total_marks_estimate", 1) * 100
        )

        return recovery_plan

    def save_report(self, report_path: Path = None):
        """Save analysis report to JSON file"""

        if report_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = OUTPUT_DIR / f"recovery_report_{timestamp}.json"

        recovery_plan = self.generate_recovery_plan()

        with open(report_path, "w") as f:
            json.dump(recovery_plan, f, indent=2, default=str)

        logger.info(f"Recovery report saved to: {report_path}")
        return report_path


def check_mongodb_connection():
    """Quick connection test"""
    try:
        logger.info(f"Testing MongoDB connection to {MONGO_HOST}:{MONGO_PORT}...")
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
            # Just try to list collections
            collections = db.list_collection_names()
            logger.info(
                f"Connected successfully. Found collections: {', '.join(collections[:5])}..."
            )
            return True
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False


def main():
    """Main entry point for fast recovery pipeline"""

    logger.info("=" * 60)
    logger.info("Fast Mark Recovery Pipeline Starting")
    logger.info("=" * 60)

    # Test connection first
    if not check_mongodb_connection():
        logger.error("Cannot connect to MongoDB. Please check connection settings.")
        return

    # MongoDB connection string
    mongo_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

    # Step 1: Analyze missing marks
    analyzer = FastMarkRecoveryAnalyzer(CHECKPOINT_DIR, mongo_uri, MONGO_DB)

    logger.info("Step 1: Loading checkpoint data...")
    analyzer.load_checkpoint_data()

    logger.info("Step 2: Performing fast analysis...")
    stats = analyzer.analyze_missing_marks_fast()

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total marks (estimated): {stats['total_marks_estimate']:,}")
    logger.info(f"Total analyses (estimated): {stats['total_analyses_estimate']:,}")
    logger.info(f"Completed analyses: {stats['completed_analyses']:,}")
    logger.info(f"Failed analyses: {stats['failed_analyses']:,}")
    logger.info(f"Unprocessed analyses: {stats['unprocessed_analyses']:,}")
    logger.info(f"Average marks per analysis: {stats['avg_marks_per_analysis']:,.0f}")

    logger.info(
        f"\nEstimated marks in completed: {stats['estimated_marks_in_completed']:,}"
    )
    logger.info(f"Estimated marks in failed: {stats['estimated_marks_in_failed']:,}")
    logger.info(
        f"Estimated marks in unprocessed: {stats['estimated_marks_in_unprocessed']:,}"
    )

    total_missing = (
        stats["estimated_marks_in_failed"] + stats["estimated_marks_in_unprocessed"]
    )
    logger.info(f"\nEstimated missing marks: {total_missing:,}")
    logger.info(
        f"Missing percentage: {(total_missing/stats['total_marks_estimate']*100):.2f}%"
    )
    logger.info(f"Analysis completed in {stats['analysis_time_seconds']:.1f} seconds")

    # Step 3: Generate recovery plan
    logger.info("\nStep 3: Generating recovery plan...")
    report_path = analyzer.save_report()

    # Print failed analysis IDs if any
    if analyzer.failed_analyses:
        logger.info("\n" + "=" * 60)
        logger.info("FAILED ANALYSIS IDS")
        logger.info("=" * 60)
        for failed in analyzer.failed_analyses[:20]:  # Show first 20
            logger.info(f"  {failed['analysis_id']}")
        if len(analyzer.failed_analyses) > 20:
            logger.info(f"  ... and {len(analyzer.failed_analyses) - 20} more")

    logger.info("\n" + "=" * 60)
    logger.info("NEXT STEPS")
    logger.info("=" * 60)
    logger.info(f"1. Review the recovery report: {report_path}")

    if analyzer.failed_analyses:
        logger.info(f"2. Reprocess {len(analyzer.failed_analyses)} failed analyses")

    if stats["unprocessed_analyses"] > 0:
        logger.info(
            f"3. Process {stats['unprocessed_analyses']:,} unprocessed analyses"
        )

    logger.info("\n" + "=" * 60)
    logger.info("Recovery Pipeline Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
