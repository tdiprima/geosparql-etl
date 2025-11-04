#!/bin/bash

# List all the *_polygon folders under cvpr-data
rclone lsd boxsbu:cvpr-data | awk '{print $5}' | grep '_polygon$' > polygon_dirs.txt

# Run up to 32 copies of rclone at once (tune this)
cat polygon_dirs.txt | parallel -j32 "echo '⚡️ Copying {}' && rclone copy boxsbu:cvpr-data/{} ./cvpr-data/{} --progress"
