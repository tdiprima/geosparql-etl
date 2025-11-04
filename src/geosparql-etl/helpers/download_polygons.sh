#!/bin/bash
# rclone lsd ${RCLONE_REMOTE} | awk '{print $5}' | grep '_polygon$'

# List all subfolders in cvpr-data
rclone lsd ${RCLONE_REMOTE} | awk '{print $5}' | grep '_polygon$' | while read -r folder; do
    echo "Copying $folder..."
    rclone copy "${RCLONE_REMOTE}/$folder" ./cvpr-data/ --progress
done

echo "Done :)"
