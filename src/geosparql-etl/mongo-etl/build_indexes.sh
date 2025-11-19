#!/bin/bash

# Fail on errors
set -e

LOGFILE="index_build_$(date +%Y%m%d_%H%M%S).log"

echo "Starting index builds..." | tee -a "$LOGFILE"
echo "Log: $LOGFILE"

# --- CONFIG ---
MONGO_HOST="localhost:27017"
DB_NAME="camic"

# --- INDEX COMMANDS ---
echo "Creating mark.provenance.image.imageid index..." | tee -a "$LOGFILE"

mongo --host "$MONGO_HOST" <<EOF | tee -a "$LOGFILE"
use $DB_NAME

db.mark.createIndex(
  { "provenance.image.imageid": 1 },
  { name: "idx_imageid", background: true }
)

db.analysis.createIndex(
  { "analysis.execution_id": 1 },
  { name: "idx_execid", background: true }
)

db.analysis.createIndex(
  { "image.imageid": 1 },
  { name: "idx_analysis_imageid", background: true }
)

EOF

echo "Index builds completed!" | tee -a "$LOGFILE"
