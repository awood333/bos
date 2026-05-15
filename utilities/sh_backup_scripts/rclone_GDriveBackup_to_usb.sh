#!/bin/bash
# =====================================================================
# GDRIVE -> USB STICK BACKUP  (with versioning)
# USB mounted at: /mnt/usb
#
# RUN FROM TERMINAL:
#   bash /home/alanw/git_repos/bos/utilities/gdrive_backup_usb.sh
#
# WHAT IT DOES:
#   - Mounts USB stick (/dev/sda1) at /mnt/usb
#   - Copies new/changed files from gdrive_backup: remote to USB
#     (never deletes anything from USB)
#   - Archives old versions of overwritten files to /mnt/usb/ARCHIVE/<timestamp>
#   - Ejects USB when done
# =====================================================================

echo "1. Mounting USB..."
sudo mount /dev/sda1 /mnt/usb || { echo "ERROR: Could not mount USB. Check it is plugged in."; exit 1; }

echo "2. Copying & Archiving old versions..."
rclone copy gdrive_backup: /mnt/usb/google_drive_backup \
    --backup-dir /mnt/usb/ARCHIVE/$(date +%Y-%m-%d_%H-%M) \
    --progress

echo "3. Ejecting..."
sudo umount /mnt/usb && echo "SUCCESS: Safe to remove USB." || echo "WARNING: Could not eject cleanly."

read -n 1 -s -r -p "Press any key to close..."
echo
