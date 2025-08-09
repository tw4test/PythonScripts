import os
import sys
import subprocess
from math import ceil
from datetime import datetime
import time
import locale
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

# This program will:
# 1. Scans all files in a specified local folder
# 2. Splits files into batches (default: 1000 per batch)
# 3. Creates a unique timestamped folder on the Android device for each batch
# 4. Pushes files to corresponding remote folders via ADB

if len(sys.argv) < 1:
    print("Usage: python batchAdbPush.py <source_folder>")
    sys.exit(1)
    
#local_root = r"N:\2021\05"  # <-- Change to your source folder
local_root = sys.argv[1]
remote_root = "/sdcard/ToProcess"
batch_size = 1000
adb_path = r"D:\Apps\QtScrcpy-win-x64-v3.2.0\adb.exe"  # <-- Your adb.exe location

# Get system encoding for subprocess output
SYSTEM_ENCODING = locale.getpreferredencoding(False)

def run_adb_command(cmd):
    """Run an adb command and return output."""
    full_cmd = [adb_path] + cmd
    # Explicitly set encoding to avoid UnicodeDecodeError
    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",  # Use 'utf-8' for cross-platform safety; can use SYSTEM_ENCODING if needed
        errors="replace"   # Replace any problematic chars
    )
    if result.returncode != 0:
        print(f"ADB command failed: {' '.join(full_cmd)}")
        print(result.stderr)
        raise RuntimeError("ADB command failed")
    # result.stdout may be None if the command produces no output
    return result.stdout.strip() if result.stdout else ""

def get_all_files(root):
    """Recursively get all file paths under root."""
    file_list = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            full_path = os.path.join(dirpath, f)
            file_list.append(full_path)
    return file_list

def create_remote_folder(folder_name):
    """Create folder on Android device via adb shell mkdir."""
    remote_path = f"{remote_root}/{folder_name}"
    print(f"Creating remote folder: {remote_path}")
    run_adb_command(["shell", "mkdir", "-p", remote_path])
    return remote_path

def push_file_to_remote(local_path, remote_folder):
    """Push a single file to remote folder."""
    filename = os.path.basename(local_path)
    remote_path = f"{remote_folder}/{filename}"
    # Only print if you want verbose logs; comment out for cleaner progress
    # print(f"Pushing {local_path} -> {remote_path}")
    run_adb_command(["push", local_path, remote_path])

def main():
    # Step 1: Get all files
    all_files = get_all_files(local_root)
    total_files = len(all_files)
    print(f"Total files found: {total_files}")

    # Step 2: Calculate number of batches
    num_batches = ceil(total_files / batch_size)
    print(f"Splitting into {num_batches} batches of up to {batch_size} files each")

    # Step 3: Process each batch with Rich progress bar
    for i in range(num_batches):
        batch_files = all_files[i*batch_size:(i+1)*batch_size]
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        batch_folder_name = f"batch_{timestamp}"
        remote_batch_folder = create_remote_folder(batch_folder_name)

        print(f"Pushing batch {i+1}/{num_batches} ({len(batch_files)} files)...")

        # Rich progress bar for this batch
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=True,  # Remove bar after completion
        ) as progress:
            task = progress.add_task(f"Batch {i+1}/{num_batches}", total=len(batch_files))
            for file_path in batch_files:
                push_file_to_remote(file_path, remote_batch_folder)
                progress.update(task, advance=1)
        # Ensure unique timestamp for each batch
        time.sleep(1)

    print("All files pushed successfully.")

if __name__ == "__main__":
    main()
