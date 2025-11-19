"""
Mark Recovery Pipeline for MongoDB to RDF ETL
Identifies and reprocesses marks that were missed during initial processing
Author: Assistant
"""

import json
import logging
import sys
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "recovery_pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("RecoveryPipeline")


class MarkRecoveryAnalyzer:
    """Analyzes missing marks and creates recovery plan"""

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
            with open(completed_file, "r") as f:
                self.completed_analyses = {line.strip() for line in f if line.strip()}

        # Load failed analyses with error details
        failed_file = self.checkpoint_dir / "failed_analyses.txt"
        if failed_file.exists():
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

    def analyze_missing_marks(self) -> Dict:
        """Analyze which marks are missing and why"""

        logger.info("Connecting to MongoDB to analyze missing marks...")

        with mongo_connection(self.mongo_uri, self.db_name) as db:
            # Get total counts
            total_analyses = db.analysis.count_documents({})
            total_marks = db.mark.count_documents({})

            logger.info(f"Total analyses in database: {total_analyses:,}")
            logger.info(f"Total marks in database: {total_marks:,}")

            # Analyze failed analyses
            failed_analysis_details = []
            total_marks_in_failed = 0

            for failed in self.failed_analyses[:10]:  # Analyze first 10 for detail
                analysis_id = failed["analysis_id"]

                # Get analysis document
                analysis_doc = db.analysis.find_one({"_id": {"$eq": analysis_id}})
                if not analysis_doc:
                    logger.warning(f"Analysis {analysis_id} not found in database")
                    continue

                # Get execution_id and image info
                exec_id = analysis_doc.get("analysis", {}).get("execution_id", "")
                image_id = analysis_doc.get("image", {}).get("imageid", "")
                slide = analysis_doc.get("image", {}).get("slide", "")

                # Count marks for this analysis
                mark_count = db.mark.count_documents(
                    {
                        "provenance.analysis.execution_id": exec_id,
                        "provenance.image.imageid": image_id,
                    }
                )

                total_marks_in_failed += mark_count

                failed_analysis_details.append(
                    {
                        "analysis_id": analysis_id,
                        "execution_id": exec_id,
                        "image_id": image_id,
                        "slide": slide,
                        "mark_count": mark_count,
                        "error": failed["error"],
                    }
                )

            # Estimate total marks in all failed analyses
            if len(failed_analysis_details) > 0:
                avg_marks_per_failed = total_marks_in_failed / len(
                    failed_analysis_details
                )
                estimated_total_failed_marks = int(
                    avg_marks_per_failed * len(self.failed_analyses)
                )
            else:
                estimated_total_failed_marks = 0

            # Get mark count for completed analyses (sample)
            sample_size = min(100, len(self.completed_analyses))
            sample_completed = list(self.completed_analyses)[:sample_size]
            total_marks_in_sample = 0

            for analysis_id in sample_completed:
                analysis_doc = db.analysis.find_one({"_id": {"$eq": analysis_id}})
                if analysis_doc:
                    exec_id = analysis_doc.get("analysis", {}).get("execution_id", "")
                    image_id = analysis_doc.get("image", {}).get("imageid", "")

                    mark_count = db.mark.count_documents(
                        {
                            "provenance.analysis.execution_id": exec_id,
                            "provenance.image.imageid": image_id,
                        }
                    )
                    total_marks_in_sample += mark_count

            avg_marks_per_completed = (
                total_marks_in_sample / sample_size if sample_size > 0 else 0
            )
            estimated_completed_marks = int(
                avg_marks_per_completed * len(self.completed_analyses)
            )

            # Check for orphaned marks (marks without corresponding analysis)
            logger.info("Checking for orphaned marks...")
            orphaned_count = self._estimate_orphaned_marks(db)

            self.stats = {
                "total_analyses": total_analyses,
                "total_marks": total_marks,
                "completed_analyses": len(self.completed_analyses),
                "failed_analyses": len(self.failed_analyses),
                "unprocessed_analyses": total_analyses
                - len(self.completed_analyses)
                - len(self.failed_analyses),
                "estimated_marks_in_completed": estimated_completed_marks,
                "estimated_marks_in_failed": estimated_total_failed_marks,
                "estimated_orphaned_marks": orphaned_count,
                "failed_analysis_details": failed_analysis_details,
                "avg_marks_per_analysis": (
                    total_marks / total_analyses if total_analyses > 0 else 0
                ),
            }

            return self.stats

    def _estimate_orphaned_marks(self, db) -> int:
        """Estimate orphaned marks using sampling"""

        # Sample marks to check if they have valid analyses
        sample_pipeline = [
            {"$sample": {"size": 1000}},
            {
                "$project": {
                    "exec_id": "$provenance.analysis.execution_id",
                    "image_id": "$provenance.image.imageid",
                }
            },
        ]

        sample_marks = list(db.mark.aggregate(sample_pipeline))
        orphaned_in_sample = 0

        for mark in sample_marks:
            # Check if corresponding analysis exists
            analysis_exists = db.analysis.find_one(
                {
                    "analysis.execution_id": mark.get("exec_id"),
                    "image.imageid": mark.get("image_id"),
                },
                {"_id": 1},
            )

            if not analysis_exists:
                orphaned_in_sample += 1

        # Extrapolate to total
        if len(sample_marks) > 0:
            orphan_rate = orphaned_in_sample / len(sample_marks)
            total_marks = db.mark.count_documents({})
            return int(total_marks * orphan_rate)

        return 0

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
                    "implementation": "Use recovery script with fixed checkpoint handling",
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
                    "estimated_marks": int(
                        unprocessed * self.stats.get("avg_marks_per_analysis", 0)
                    ),
                    "priority": "MEDIUM",
                    "implementation": "Run main ETL with updated checkpoint",
                }
            )

        # Strategy 3: Handle orphaned marks
        orphaned = self.stats.get("estimated_orphaned_marks", 0)
        if orphaned > 1000:  # Only if significant number
            recovery_plan["recovery_strategies"].append(
                {
                    "name": "Handle Orphaned Marks",
                    "description": "Process marks without corresponding analyses",
                    "target_count": orphaned,
                    "priority": "LOW",
                    "implementation": "Create synthetic analyses or skip based on requirements",
                }
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
    
    with mongo_connection(f"mongodb://{{MONGO_HOST}}:{{MONGO_PORT}}/", MONGO_DB) as db:
        # Fetch full documents for failed analyses
        failed_docs = []
        for aid in FAILED_IDS:
            doc = db.analysis.find_one({{"_id": aid}})
            if doc:
                failed_docs.append(doc)
        
        logger.info(f"Found {{len(failed_docs)}} analyses to reprocess")
        
        # Process with reduced parallelism to avoid overwhelming system
        with Pool(processes=NUM_WORKERS) as pool:
            worker_args = []
            for i, doc in enumerate(failed_docs):
                worker_id = i % NUM_WORKERS
                worker_args.append((worker_id, doc, str(CHECKPOINT_DIR)))
            
            results = pool.map(process_analysis_worker, worker_args, chunksize=1)
            
            # Count successes
            success_count = sum(1 for r in results if r and r[0] == "completed")
            logger.info(f"Recovery complete: {{success_count}}/{{len(failed_docs)}} succeeded")

if __name__ == "__main__":
    main()
'''

        with open(output_path, "w") as f:
            f.write(script_content)

        output_path.chmod(0o755)  # Make executable
        logger.info(f"Recovery script created: {output_path}")

        return output_path


def main():
    """Main entry point for recovery pipeline"""

    logger.info("=" * 60)
    logger.info("Mark Recovery Pipeline Starting")
    logger.info("=" * 60)

    # MongoDB connection string
    mongo_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

    # Step 1: Analyze missing marks
    analyzer = MarkRecoveryAnalyzer(CHECKPOINT_DIR, mongo_uri, MONGO_DB)

    logger.info("Step 1: Loading checkpoint data...")
    analyzer.load_checkpoint_data()

    logger.info("Step 2: Analyzing missing marks...")
    stats = analyzer.analyze_missing_marks()

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total marks in MongoDB: {stats['total_marks']:,}")
    logger.info(f"Estimated processed marks: {stats['estimated_marks_in_completed']:,}")
    logger.info(f"Estimated failed marks: {stats['estimated_marks_in_failed']:,}")
    logger.info(f"Estimated orphaned marks: {stats['estimated_orphaned_marks']:,}")

    estimated_total = (
        stats["estimated_marks_in_completed"] + stats["estimated_marks_in_failed"]
    )
    missing_marks = stats["total_marks"] - stats["estimated_marks_in_completed"]

    logger.info(f"\nEstimated missing marks: {missing_marks:,}")
    logger.info(f"Missing percentage: {(missing_marks/stats['total_marks']*100):.2f}%")

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

    logger.info("\n" + "=" * 60)
    logger.info("Recovery Pipeline Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
