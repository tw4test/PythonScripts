import os
import subprocess
from pathlib import Path

input_folder = r"E:\TO_NAS_temp\MIJIA_RECORD_VIDEO"
output_file = r"E:\TO_NAS_temp\merged_output.mkv"

# Collect all mp4 files recursively
mp4_files = list(Path(input_folder).rglob("*.mp4"))

# Create a text file listing all mp4 files for ffmpeg
with open("file_list.txt", "w") as f:
    for mp4 in mp4_files:
        f.write(f"file '{mp4}'\n")

# Run ffmpeg command for lossless stream copy merge
cmd = [
    "ffmpeg",
    "-f", "concat",
    "-safe", "0",
    "-i", "file_list.txt",
    "-c", "copy",
    output_file
]
subprocess.run(cmd)

# Clean up
#os.remove("file_list.txt")