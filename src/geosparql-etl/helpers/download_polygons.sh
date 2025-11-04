#!/bin/bash
# rclone lsd boxsbu:cvpr-data | awk '{print $5}' | grep '_polygon$'

# List all subfolders in cvpr-data
rclone lsd boxsbu:cvpr-data | awk '{print $5}' | grep '_polygon$' | while read -r folder; do
    echo "Copying $folder..."
    rclone copy "boxsbu:cvpr-data/$folder" ./cvpr-data/ --progress
done

echo "Done :)"
