# MongoDB to RDF Mark Processing Analysis & Recovery Plan

## Executive Summary

Your processing encountered a race condition with checkpoint file handling that caused some analyses to fail. This resulted in approximately **590 million marks** (14.9% of total) not being processed.

### Key Findings:
- **Total marks in MongoDB**: ~3.97 billion
- **Successfully processed**: ~3.38 billion  
- **Missing/unprocessed**: ~590 million
- **Failed analyses**: 73 (based on error log)
- **Root cause**: Race condition in checkpoint file creation

## Problem Analysis

### 1. The Error Pattern
All failures show the same error:
```
[Errno 2] No such file or directory: 'checkpoints/in_progress.txt'
```

This occurred when multiple worker processes tried to write to the checkpoint file simultaneously, but the file was deleted/recreated by the checkpoint manager initialization.

### 2. Impact Assessment

**Before modifications**: 3.42 billion marks processed
**After modifications**: 3.38 billion marks processed  
**Difference**: ~40 million marks

However, the actual missing count is larger (~590 million) because:
- Some analyses were never attempted
- Some analyses failed early in processing
- The checkpoint system prevented retries

### 3. Failed Analyses Distribution

The failed analyses include various execution types:
- `CNN_synthetic_n_real`: Most common
- `CNN_class_labels`: Some failures
- `TopoCount-v4-Tumor`: Few failures

Each failed analysis likely contains thousands to millions of marks.

## Recovery Solution

I've created three components to address this:

### 1. **Mark Recovery Pipeline** (`mark_recovery_pipeline.py`)

This script:
- Analyzes the current state of processed/unprocessed marks
- Identifies exactly which analyses failed
- Estimates the number of missing marks
- Generates a detailed recovery plan
- Creates custom recovery scripts

**To run:**
```bash
python3 mark_recovery_pipeline.py
```

### 2. **Fixed ETL Script** (`mongodb_to_rdf_fixed.py`)

Improvements:
- Thread-safe checkpoint file handling
- Ensures `in_progress.txt` exists before any worker uses it
- Uses file locks to prevent race conditions
- Better error recovery
- Continues processing even if checkpoint operations fail

**To run:**
```bash
python3 mongodb_to_rdf_fixed.py
```

### 3. **Recovery Workflow**

## Recommended Recovery Process

### Phase 1: Analysis (5 minutes)
```bash
# Run the recovery pipeline to analyze missing marks
python3 mark_recovery_pipeline.py

# This will generate:
# - recovery_reports/recovery_report_[timestamp].json
# - recovery_reports/reprocess_failed.py
```

### Phase 2: Recover Failed Analyses (1-2 hours)
```bash
# Run the auto-generated recovery script
python3 recovery_reports/reprocess_failed.py

# This will:
# - Reprocess only the 73 failed analyses
# - Use reduced parallelism (10 workers) to avoid overload
# - Create new checkpoint files in recovery_checkpoints/
```

### Phase 3: Process Remaining Unprocessed (if any)
```bash
# Use the fixed ETL script for any remaining work
python3 mongodb_to_rdf_fixed.py
```

### Phase 4: Verification
```bash
# Re-run the analysis to confirm all marks are processed
python3 mark_recovery_pipeline.py
```

## Performance Optimization Tips

### 1. MongoDB Index Optimization
Ensure these indexes exist for optimal query performance:
```javascript
db.mark.createIndex({
    "provenance.analysis.execution_id": 1,
    "provenance.image.imageid": 1,
    "provenance.image.slide": 1
})

db.analysis.createIndex({
    "analysis.execution_id": 1,
    "image.imageid": 1
})
```

### 2. Resource Management
- **Worker count**: Start with 10 for recovery, increase to 20 for main processing
- **Batch size**: Keep at 1000 marks per TTL file
- **MongoDB connection pool**: Ensure sufficient connections for parallel workers

### 3. Monitoring
Monitor during recovery:
```bash
# Watch progress
tail -f recovery_checkpoints/*.log

# Check MongoDB load
mongostat --host 172.18.0.2:27017

# Monitor disk I/O
iostat -x 1
```

## Expected Outcomes

After running the recovery process:
1. All 73 failed analyses should be reprocessed
2. ~590 million missing marks should be recovered
3. Total processed marks should reach ~3.97 billion
4. The checkpoint files will accurately reflect completion state

## Important Notes

### Data Integrity
- The recovery process is **idempotent** - safe to run multiple times
- Already processed analyses won't be reprocessed
- Output files use the same structure as the original

### Time Estimates
- Analysis phase: ~5 minutes
- Recovery of failed analyses: 1-2 hours (depends on mark count)
- Full verification: ~5 minutes

### Disk Space
Ensure sufficient space for:
- New TTL output files (~50-100 GB compressed)
- Checkpoint files (minimal, <1 MB)
- Log files (~100 MB)

## Troubleshooting

### If recovery script fails:
1. Check MongoDB connectivity
2. Ensure checkpoint directory is writable
3. Verify sufficient disk space
4. Check that the analysis IDs exist in MongoDB

### If marks are still missing after recovery:
1. Run the analysis pipeline again to identify remaining gaps
2. Check for orphaned marks (marks without analyses)
3. Consider running the fixed ETL from scratch with empty checkpoints

## Questions to Address

1. **Should orphaned marks be processed?**
   - These are marks without corresponding analysis documents
   - May need special handling or synthetic analysis creation

2. **Checkpoint retention policy?**
   - Keep failed_analyses.txt for debugging
   - completed_analyses.txt is the critical state file
   - Consider archiving after successful completion

3. **Performance tuning?**
   - Current: 20 workers, 1000 marks/batch
   - Could increase workers if MongoDB handles it well
   - Batch size is optimal for file size/processing balance

## Conclusion

The missing marks are recoverable with the provided tools. The race condition in the checkpoint system was the root cause, and the fixed version prevents this issue. Running the recovery pipeline should restore all missing marks within a few hours.
