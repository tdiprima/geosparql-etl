#!/bin/bash

# List all the *_polygon folders under cvpr-data
rclone lsd ${RCLONE_REMOTE} | awk '{print $5}' | grep '_polygon$' > polygon_dirs.txt

# Run up to 32 copies of rclone at once (tune this)
cat polygon_dirs.txt | parallel -j32 "echo '⚡️ Copying {}' && rclone copy ${RCLONE_REMOTE}/{} ./cvpr-data/{} --progress"
