#!/system/bin/sh

SRC_DIR="/sdcard/ToProcess"
DEST_DIR="/sdcard/DCIM/Camera"

echo "[DEBUG] Starting script."
echo "[DEBUG] Source directory: $SRC_DIR"
echo "[DEBUG] Destination directory: $DEST_DIR"

FOLDER=$(ls -d $SRC_DIR/batch_* 2>/dev/null | head -n 1)

if [ -n "$FOLDER" ]; then
  BASENAME=$(basename "$FOLDER")
  echo "[DEBUG] Found folder to move: $BASENAME"
  
  echo "[DEBUG] Moving folder..."
  mv "$FOLDER" "$DEST_DIR"/
  if [ $? -eq 0 ]; then
    echo "[DEBUG] Move successful."
  else
    echo "[ERROR] Move failed!"
    exit 1
  fi
  
  echo "[DEBUG] Waiting 5 seconds before triggering media scanner..."
  sleep 5
  
  echo "[DEBUG] Triggering media scanner on $DEST_DIR"
  am broadcast -a android.intent.action.MEDIA_SCANNER_SCAN_FILE -d "file://$DEST_DIR"
  
  echo "[DEBUG] Script completed successfully."
else
  echo "[DEBUG] No batch folders found in $SRC_DIR."
fi
