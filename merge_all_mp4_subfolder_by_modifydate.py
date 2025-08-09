import subprocess
from pathlib import Path
import os
import logging

input_folder = r"D:\video"
output_file = r"D:\merged_output.mkv"
file_list_path = "file_list.txt"
log_file = "merge_mp4.log"

# Set up logging
logging.basicConfig(
    filename=log_file,
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def is_valid_mp4(file_path):
    """Use ffprobe to check if the file has a valid video stream."""
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=codec_type',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                str(file_path)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        is_valid = result.returncode == 0 and result.stdout.decode().strip() == 'video'
        if not is_valid:
            logging.warning(f"Invalid or corrupted video: {file_path}")
        return is_valid
    except Exception as e:
        logging.error(f"Error checking file {file_path}: {e}")
        return False

# Collect all mp4 files recursively
all_mp4_files = list(Path(input_folder).rglob("*.mp4"))
logging.info(f"Found {len(all_mp4_files)} mp4 files under {input_folder}")

# Filter out corrupted files using ffprobe and sort by modified date
mp4_files = sorted(
    [f for f in all_mp4_files if f.stat().st_size > 0 and is_valid_mp4(f)],
    key=lambda x: x.stat().st_mtime
)

logging.info(f"{len(mp4_files)} valid mp4 files will be merged.")

if not mp4_files:
    print("No valid MP4 files found.")
    logging.error("No valid MP4 files found.")
    exit(1)

# Create file list for ffmpeg
with open(file_list_path, "w", encoding="utf-8") as f:
    for mp4 in mp4_files:
        f.write(f"file '{mp4.as_posix()}'\n")

# Run ffmpeg command for lossless stream copy merge
cmd = [
    "ffmpeg",
    "-f", "concat",
    "-safe", "0",
    "-i", file_list_path,
    "-c", "copy",
    output_file
]

try:
    subprocess.run(cmd, check=True)
    print(f"Successfully merged {len(mp4_files)} files into {output_file}")
    logging.info(f"Successfully merged {len(mp4_files)} files into {output_file}")
except subprocess.CalledProcessError as e:
    print(f"ffmpeg failed: {e}")
    logging.error(f"ffmpeg failed: {e}")
finally:
    if os.path.exists(file_list_path):
        os.remove(file_list_path)

# Log skipped files
skipped_files = [str(f) for f in all_mp4_files if f not in mp4_files]
if skipped_files:
    print("Skipped corrupted or invalid files:")
    logging.warning("Skipped corrupted or invalid files:")
    for sf in skipped_files:
        print(sf)
        logging.warning(sf)
else:
    print("No files were skipped.")
    logging.info("No files were skipped.")
