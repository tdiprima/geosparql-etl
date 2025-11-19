"""
Fixed MongoDB to RDF ETL - Handles checkpoint file race condition
"""

import os
import threading

# Import everything from the updated version
# from mongodb_to_rdf_updated import *
from mongodb_to_rdf_updated import (
    ParallelCheckpointManager,
    mongo_connection,
    setup_worker_logger,
    main_logger,
    create_ttl_header,
    add_mark_to_ttl,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DB,
    BATCH_SIZE,
    OUTPUT_DIR,
    CHECKPOINT_DIR,
    GZIP_COMPRESSION_LEVEL,
    NUM_WORKERS,
)

# Thread-safe file lock for checkpoint operations
checkpoint_lock = threading.Lock()


class FixedParallelCheckpointManager(ParallelCheckpointManager):
    """Fixed checkpoint manager that handles file creation properly"""

    def __init__(self, checkpoint_dir):
        super().__init__(checkpoint_dir)
        # Ensure in_progress file exists before any worker tries to use it
        self.in_progress_file.touch(exist_ok=True)

    def mark_in_progress(self, analysis_id, worker_id):
        """Thread-safe marking of in-progress with file existence check"""
        with checkpoint_lock:
            # Ensure file exists
            if not self.in_progress_file.exists():
                self.in_progress_file.touch()

            # Now safe to append
            with open(self.in_progress_file, "a") as f:
                f.write(
                    f"{analysis_id}|worker_{worker_id}|{datetime.now().isoformat()}\n"
                )
                f.flush()
                os.fsync(f.fileno())  # Force write to disk

    def mark_completed(self, analysis_id):
        """Thread-safe completion marking"""
        with checkpoint_lock:
            # Ensure directory exists
            self.checkpoint_dir.mkdir(exist_ok=True)

            with open(self.completed_file, "a") as f:
                f.write(f"{analysis_id}\n")
                f.flush()
                os.fsync(f.fileno())

    def mark_failed(self, analysis_id, error=None):
        """Thread-safe failure marking"""
        with checkpoint_lock:
            # Ensure directory exists
            self.checkpoint_dir.mkdir(exist_ok=True)

            with open(self.failed_file, "a") as f:
                f.write(f"{analysis_id}|{error}\n")
                f.flush()
                os.fsync(f.fileno())


def fixed_process_analysis_worker(args):
    """Fixed worker function with better error handling"""
    worker_id, analysis_doc, checkpoint_dir = args

    logger = setup_worker_logger(worker_id)

    analysis = analysis_doc.get("analysis", {})
    image = analysis_doc.get("image", {})

    exec_id = analysis.get("execution_id")
    img_id = image.get("imageid")
    slide = image.get("slide")
    analysis_id = str(analysis_doc.get("_id"))

    logger.info("Starting %s:%s (analysis_id=%s)", exec_id, img_id, analysis_id)

    try:
        start_time = time.time()

        # Use fixed checkpoint manager
        checkpoint = FixedParallelCheckpointManager(checkpoint_dir)

        # Try to mark in progress - if this fails, skip this analysis
        try:
            checkpoint.mark_in_progress(analysis_id, worker_id)
        except Exception as e:
            logger.warning(f"Could not mark {analysis_id} in progress: {e}")
            # Continue anyway - the important part is processing the data

        # Rest of the processing logic remains the same
        with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:

            # Query for marks
            query = {
                "provenance.analysis.execution_id": exec_id,
                "provenance.image.imageid": img_id,
            }

            if slide:
                query["provenance.image.slide"] = slide

            logger.info("Streaming marks for %s:%s", exec_id, img_id)

            # Process marks
            marks_cursor = db.mark.find(query, batch_size=5000, no_cursor_timeout=False)

            try:
                batch_num = 1
                batch_marks = 0
                processed = 0
                is_first_feature = True

                # Start first batch
                ttl_content, img_width, img_height = create_ttl_header(
                    analysis_doc, batch_num
                )

                for mark in marks_cursor:
                    mark_ttl, success = add_mark_to_ttl(
                        mark, img_width, img_height, is_first_feature
                    )
                    if success:
                        ttl_content += mark_ttl
                        batch_marks += 1
                        processed += 1
                        is_first_feature = False

                    # Write batch when full
                    if batch_marks >= BATCH_SIZE:
                        ttl_content += " .\n"

                        output_file = (
                            OUTPUT_DIR
                            / str(exec_id)
                            / str(img_id)
                            / f"batch_{batch_num:06d}.ttl.gz"
                        )
                        output_file.parent.mkdir(parents=True, exist_ok=True)

                        with gzip.open(
                            output_file,
                            "wt",
                            encoding="utf-8",
                            compresslevel=GZIP_COMPRESSION_LEVEL,
                        ) as f:
                            f.write(ttl_content)

                        logger.info(
                            "Wrote batch %d for %s:%s (%s marks)",
                            batch_num,
                            exec_id,
                            img_id,
                            batch_marks,
                        )

                        batch_num += 1
                        batch_marks = 0
                        ttl_content, img_width, img_height = create_ttl_header(
                            analysis_doc, batch_num
                        )
                        is_first_feature = True

                # Write remaining marks
                if batch_marks > 0:
                    ttl_content += " .\n"

                    output_file = (
                        OUTPUT_DIR
                        / str(exec_id)
                        / str(img_id)
                        / f"batch_{batch_num:06d}.ttl.gz"
                    )
                    output_file.parent.mkdir(parents=True, exist_ok=True)

                    with gzip.open(
                        output_file,
                        "wt",
                        encoding="utf-8",
                        compresslevel=GZIP_COMPRESSION_LEVEL,
                    ) as f:
                        f.write(ttl_content)

                    logger.info(
                        "Wrote final batch %d for %s:%s", batch_num, exec_id, img_id
                    )

            finally:
                try:
                    marks_cursor.close()
                except:
                    pass

            elapsed = time.time() - start_time
            logger.info(
                "✅ Completed %s:%s – %s marks in %.2f seconds",
                exec_id,
                img_id,
                processed,
                elapsed,
            )

            # Mark as completed
            try:
                checkpoint.mark_completed(analysis_id)
            except Exception as e:
                logger.warning(f"Could not mark {analysis_id} as completed: {e}")

            return ("completed", analysis_id, processed, batch_num)

    except Exception as e:
        logger.error("Failed processing %s:%s: %s", exec_id, img_id, e, exc_info=True)

        # Try to mark as failed
        try:
            checkpoint = FixedParallelCheckpointManager(checkpoint_dir)
            checkpoint.mark_failed(analysis_id, str(e))
        except:
            pass

        return ("failed", analysis_id, 0, 0, str(e))


def main_fixed():
    """Fixed main function"""
    main_logger.info("=" * 60)
    main_logger.info(f"FIXED PARALLEL ETL - Using {NUM_WORKERS} cores")
    main_logger.info(f"MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    main_logger.info("=" * 60)

    # Use fixed checkpoint manager
    checkpoint = FixedParallelCheckpointManager(CHECKPOINT_DIR)
    initial_stats = checkpoint.get_stats()
    main_logger.info(
        f"Resuming - Completed: {initial_stats['completed']}, Failed: {initial_stats['failed']}"
    )

    with mongo_connection(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", MONGO_DB) as db:
        total_analyses = db.analysis.count_documents({})
        main_logger.info(f"Found {total_analyses:,} total analyses")

        # Get analyses to process
        analyses_to_process = []
        for doc in db.analysis.find({}, {"_id": 1}):
            if checkpoint.should_process(str(doc["_id"])):
                analyses_to_process.append(doc["_id"])

        main_logger.info(f"Need to process {len(analyses_to_process):,} analyses")

        if not analyses_to_process:
            main_logger.info("Nothing to process!")
            return

        # Process in chunks
        chunk_size = NUM_WORKERS * 10
        total_processed = 0
        total_marks = 0
        start_time = time.time()

        with Pool(processes=NUM_WORKERS) as pool:
            try:
                for chunk_start in range(0, len(analyses_to_process), chunk_size):
                    chunk_ids = analyses_to_process[
                        chunk_start : chunk_start + chunk_size
                    ]

                    # Fetch documents
                    chunk_docs = list(db.analysis.find({"_id": {"$in": chunk_ids}}))

                    if not chunk_docs:
                        continue

                    # Prepare worker arguments
                    worker_args = []
                    for i, doc in enumerate(chunk_docs):
                        worker_id = i % NUM_WORKERS
                        worker_args.append((worker_id, doc, str(CHECKPOINT_DIR)))

                    # Process with fixed worker function
                    for result in pool.imap_unordered(
                        fixed_process_analysis_worker, worker_args, chunksize=1
                    ):
                        if not result:
                            continue

                        status = result[0]

                        if status == "completed":
                            _, analysis_id, mark_count, batch_count = result[:4]
                            total_processed += 1
                            total_marks += mark_count

                            main_logger.info(
                                "Completed %s – %s marks (%s/%s analyses)",
                                analysis_id,
                                mark_count,
                                total_processed,
                                len(analyses_to_process),
                            )

                        elif status == "failed":
                            _, analysis_id, _, _, error = result
                            main_logger.error("Failed %s: %s", analysis_id, error)

                        # Progress report
                        if total_processed % 50 == 0:
                            elapsed = time.time() - start_time
                            rate = total_marks / elapsed if elapsed > 0 else 0
                            main_logger.info(
                                f"Progress: {total_processed}/{len(analyses_to_process)}, Rate: {rate:.0f} marks/sec"
                            )

            except KeyboardInterrupt:
                main_logger.warning("Interrupted by user")
                pool.terminate()
                pool.join()

    # Final stats
    elapsed_hours = (time.time() - start_time) / 3600
    main_logger.info(
        f"Complete! Processed {total_marks:,} marks in {elapsed_hours:.2f} hours"
    )


if __name__ == "__main__":
    import signal

    signal.signal(signal.SIGINT, signal.default_int_handler)
    main_fixed()
