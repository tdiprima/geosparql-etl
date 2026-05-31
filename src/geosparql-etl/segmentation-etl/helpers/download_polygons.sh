#!/usr/bin/env bash
# Downloads all *_polygon folders from the ${RCLONE_REMOTE} remote using rclone.
# Requires: rclone, parallel

set -euo pipefail

# List all the *_polygon folders under cvpr-data
rclone lsd ${RCLONE_REMOTE} | awk '{print $5}' | grep '_polygon$' > polygon_dirs.txt

# Run up to 32 copies of rclone at once (tune this)
parallel --jobs 32 "echo 'Copying {}' && rclone copy ${RCLONE_REMOTE}/{} ./cvpr-data/{} --progress" :::: polygon_dirs.txt
