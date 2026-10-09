#!/usr/bin/env bash
# Downloads all *_polygon folders from the $RCLONE_REMOTE rclone remote.
# Requires: rclone, parallel

set -euo pipefail
set -a; source "$(dirname "$0")/../../../../.env"; set +a
REMOTE="${RCLONE_REMOTE:?set RCLONE_REMOTE in .env}"

# List all the *_polygon folders under cvpr-data
rclone lsd "$REMOTE" | awk '{print $5}' | grep '_polygon$' > polygon_dirs.txt

# Run up to 32 copies of rclone at once (tune this)
parallel --jobs 32 "echo 'Copying {}' && rclone copy $REMOTE/{} ./cvpr-data/{} --progress" :::: polygon_dirs.txt
