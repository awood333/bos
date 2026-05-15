#!/bin/bash
# Change to the directory where this script is located
cd "$(dirname "$0")"


# THIS IS THE AWS BACKUP FOR UBUUNTU

# =====================================================================
# G-DRIVE TO AWS S3 BACKSTOP BACKUP
# Destination: cow-bucket-613211402323-ap-southeast-7-an (Thailand)
# =====================================================================

echo "Starting Backup: Google Drive -> AWS S3..."
echo

rclone copy gdrive:COWS AWS_S3:cow-bucket-613211402323-ap-southeast-7-an/gdrive-backup \
    --update \
    --checksum \
    --transfers 4 \
    --checkers 8 \
    --contimeout 60s \
    --timeout 300s \
    --retries 3 \
    --low-level-retries 10 \
    --stats 1s \
    -P

echo
echo "====================================================================="
echo "BACKUP FINISHED."
echo "AWS Versioning will keep historical copies of modified files."
echo "====================================================================="
echo
exit 0
