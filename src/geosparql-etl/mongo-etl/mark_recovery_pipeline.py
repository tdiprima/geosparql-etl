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

    # Step 4: Create recovery script for failed analyses
    if analyzer.failed_analyses:
        logger.info("\nStep 4: Creating recovery script...")
        recovery = FailedAnalysisRecovery(analyzer.failed_analyses, mongo_uri, MONGO_DB)
        script_path = recovery.create_recovery_script()

        logger.info("\n" + "=" * 60)
        logger.info("RECOVERY INSTRUCTIONS")
        logger.info("=" * 60)
        logger.info(f"1. Review the recovery report: {report_path}")
        logger.info(f"2. Run the recovery script: python3 {script_path}")
        logger.info("3. Monitor progress in recovery_checkpoints/")
        logger.info("4. Re-run this analysis after recovery to verify")
    else:
        # Print failed analysis IDs if any
        logger.info("\n" + "=" * 60)
        logger.info("NEXT STEPS")
        logger.info("=" * 60)
        logger.info(f"1. Review the recovery report: {report_path}")

        if stats["unprocessed_analyses"] > 0:
            logger.info(
                f"2. Process {stats['unprocessed_analyses']:,} unprocessed analyses"
            )

    logger.info("\n" + "=" * 60)
    logger.info("Recovery Pipeline Complete")
    logger.info("=" * 60)


class FailedAnalysisRecovery:
    """Handles recovery of failed analyses"""

    def __init__(self, failed_analyses: List[Dict], mongo_uri: str, db_name: str):
        self.failed_analyses = failed_analyses
        self.mongo_uri = mongo_uri
        self.db_name = db_name

    def create_recovery_script(self, output_path: Path = None) -> Path:
        """Create a script to reprocess only failed analyses"""

        if output_path is None:
            output_path = OUTPUT_DIR / "reprocess_failed.py"

        # Extract analysis IDs
        failed_ids = [f["analysis_id"] for f in self.failed_analyses]

        script_content = f'''#!/usr/bin/env python3
"""
Recovery script for failed analyses
Generated: {datetime.now().isoformat()}
Total failed analyses to reprocess: {len(failed_ids)}
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mongodb_to_rdf_updated import process_analysis_worker, setup_worker_logger
from utils import mongo_connection
from multiprocessing import Pool
import logging
import time

# Configuration
MONGO_HOST = "{MONGO_HOST}"
MONGO_PORT = {MONGO_PORT}
MONGO_DB = "{MONGO_DB}"
NUM_WORKERS = 10  # Reduced for recovery
CHECKPOINT_DIR = Path("recovery_checkpoints")

# Failed analysis IDs to reprocess
FAILED_IDS = {failed_ids}

def main():
    # Create checkpoint directory
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    
    # Fix the in_progress.txt issue
    (CHECKPOINT_DIR / "in_progress.txt").touch()
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Recovery")
    
    logger.info(f"Starting recovery of {{len(FAILED_IDS)}} failed analyses")
    
    start_time = time.time()
    
    with mongo_connection(f"mongodb://{{MONGO_HOST}}:{{MONGO_PORT}}/", MONGO_DB) as db:
        # Fetch full documents for failed analyses
        failed_docs = []
        for aid in FAILED_IDS:
            doc = db.analysis.find_one({{"_id": aid}})
            if doc:
                failed_docs.append(doc)
            else:
                logger.warning(f"Analysis {{aid}} not found in database")
        
        logger.info(f"Found {{len(failed_docs)}} analyses to reprocess")
        
        if not failed_docs:
            logger.warning("No valid analyses found to reprocess!")
            return
        
        # Process with reduced parallelism to avoid overwhelming system
        with Pool(processes=NUM_WORKERS) as pool:
            worker_args = []
            for i, doc in enumerate(failed_docs):
                worker_id = i % NUM_WORKERS
                worker_args.append((worker_id, doc, str(CHECKPOINT_DIR)))
            
            logger.info(f"Starting parallel processing with {{NUM_WORKERS}} workers...")
            
            results = []
            for i, result in enumerate(pool.imap_unordered(process_analysis_worker, worker_args, chunksize=1), 1):
                results.append(result)
                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed
                    eta = (len(worker_args) - i) / rate if rate > 0 else 0
                    logger.info(f"Progress: {{i}}/{{len(worker_args)}} - ETA: {{eta:.1f}} seconds")
            
            # Count successes
            success_count = sum(1 for r in results if r and r[0] == "completed")
            failed_count = sum(1 for r in results if r and r[0] == "failed")
            
            elapsed_total = time.time() - start_time
            logger.info(f"Recovery complete in {{elapsed_total:.1f}} seconds")
            logger.info(f"Results: {{success_count}} succeeded, {{failed_count}} failed out of {{len(failed_docs)}} total")
            
            # Log any new failures
            if failed_count > 0:
                logger.warning("The following analyses failed again:")
                for r in results:
                    if r and r[0] == "failed":
                        logger.warning(f"  - {{r[1]}}: {{r[4] if len(r) > 4 else 'Unknown error'}}")

if __name__ == "__main__":
    main()
'''

        with open(output_path, "w") as f:
            f.write(script_content)

        output_path.chmod(0o755)  # Make executable
        logger.info(f"Recovery script created: {output_path}")

        return output_path


if __name__ == "__main__":
    main()
