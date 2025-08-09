import os
import sys
import subprocess
import sqlite3
from datetime import datetime
from math import ceil
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
import time

####################
# CONFIGURATION
####################

ADB_PATH = r"D:\Apps\QtScrcpy-win-x64-v3.2.0\adb.exe"   # Change this to your adb executable
REMOTE_ROOT = "/sdcard/ToProcess"
MAX_FILES_PER_BATCH = 1000
MAX_BATCH_SIZE_BYTES = 90 * 1_000_000_000    # 90 GB max per batch folder (for folder size control)
MAX_TOTAL_TRANSFER_BYTES = 100 * 1_000_000_000  # 100 GB max per run


####################
# DATABASE HELPERS
####################

def init_db(db_path="filetransfer.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            size INTEGER,
            mtime INTEGER,
            status TEXT DEFAULT 'pending',
            batch_id INTEGER NULL,
            transfer_time TEXT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            total_size INTEGER,
            file_count INTEGER
        )
    """)
    conn.commit()
    return conn


def scan_and_prepare(conn, source_root):
    print(f"Scanning files in {source_root} ...")
    all_files = []
    for dirpath, _, filenames in os.walk(source_root):
        for f in filenames:
            full_path = os.path.join(dirpath, f)
            try:
                size = os.path.getsize(full_path)
                mtime = int(os.path.getmtime(full_path))
                all_files.append((full_path, size, mtime))
            except Exception as e:
                print(f"Skipping unreadable file: {full_path} ({e})")

    print(f"Found {len(all_files)} files. Inserting into database...")

    cur = conn.cursor()
    inserted, skipped = 0, 0
    for path, size, mtime in all_files:
        try:
            cur.execute("INSERT OR IGNORE INTO files (path, size, mtime) VALUES (?, ?, ?)", (path, size, mtime))
            if cur.rowcount:
                inserted += 1
            else:
                skipped +=1
        except Exception as e:
            print(f"Error inserting {path}: {e}")
    conn.commit()
    print(f"Inserted {inserted} new files, skipped {skipped} already recorded.")


####################
# ADB HELPERS
####################

def run_adb_command(cmd):
    full_cmd = [ADB_PATH] + cmd
    result = subprocess.run(full_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"ADB command failed: {' '.join(full_cmd)}\n{result.stderr.strip()}")
    return result.stdout.strip() if result.stdout else ""

def adb_create_remote_folder(remote_path):
    print(f"Creating remote folder: {remote_path}")
    run_adb_command(["shell", "mkdir", "-p", remote_path])

def adb_push_file(local_path, remote_folder):
    filename = os.path.basename(local_path)
    remote_path = f"{remote_folder}/{filename}"
    # Uncomment to see each push log
    print(f"Pushing {local_path} -> {remote_path}")
    run_adb_command(["push", local_path, remote_path])

####################
# PROCESSING PHASE
####################


def process_files(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, path, size FROM files WHERE status='pending' ORDER BY id")
    pending_files = cur.fetchall()

    if not pending_files:
        print("No pending files to process.")
        return

    total_transferred = 0
    batch_files = []
    batch_size = 0
    batch_index = 1

    def create_remote_folder(batch_index):
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        batch_folder_name = f"batch_{timestamp}_{batch_index}"
        remote_batch_folder = f"{REMOTE_ROOT}/{batch_folder_name}"
        adb_create_remote_folder(remote_batch_folder)
        return remote_batch_folder

    remote_batch_folder = create_remote_folder(batch_index)

    print("\nStarting file transfers with progress bar:")

    # Rich progress bar context
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=True,
    ) as progress:
        task = None

        for file_id, filepath, filesize in pending_files:
            # Stop if total transferred exceeds max allowed
            if total_transferred + filesize > MAX_TOTAL_TRANSFER_BYTES:
                print(f"\nReached total transfer limit of ~{MAX_TOTAL_TRANSFER_BYTES / 1e9:.1f} GB. Stopping processing.")
                break

            # Check batch size and count limits for folder organization
            if len(batch_files) >= MAX_FILES_PER_BATCH or batch_size + filesize > MAX_BATCH_SIZE_BYTES:
                batch_index += 1
                remote_batch_folder = create_remote_folder(batch_index)
                batch_files = []
                batch_size = 0
                if task is not None:
                    progress.remove_task(task)
                    task = None  # Reset for new batch

            # Add new progress task when batch starts or after batch rollover
            if task is None:
                # Get total number of files in the batch ahead (up to limits)
                # Since we do not precompute batch files here, we can assign total=1, and update total dynamically,
                # or just keep total unknown by estimating remaining files of this batch.
                # For simplicity, set total=max batch size in files or unknown. Or set total to 1 temporarily.
                task = progress.add_task(f"Batch {batch_index} files", total=None)  # total unknown

            try:
                adb_push_file(filepath, remote_batch_folder)

                # Update DB immediately after successful push
                cur.execute(
                    "UPDATE files SET status='transferred', batch_id=?, transfer_time=datetime('now') WHERE id=?",
                    (batch_index, file_id)
                )
                conn.commit()
                total_transferred += filesize
                batch_files.append(file_id)
                batch_size += filesize

                # Increment progress task by 1 file
                progress.update(task, advance=1)
            except Exception as e:
                print(f"\nError pushing file {filepath}: {e}")
                # Optionally mark as failed:
                # cur.execute("UPDATE files SET status='failed' WHERE id=?", (file_id,))
                conn.commit()
                # Continue despite errors
                continue

        # End of loop
    print(f"Finished processing session. Total transferred: {total_transferred / 1_000_000_000:.2f} GB\n")



def dry_run(conn):
    MAX_FILES_PER_BATCH = 1000
    MAX_BATCH_SIZE_BYTES = 90 * 1_000_000_000
    MAX_TOTAL_TRANSFER_BYTES = 100 * 1_000_000_000

    cur = conn.cursor()
    cur.execute("SELECT id, path, size FROM files WHERE status='pending' ORDER BY id")
    pending_files = cur.fetchall()

    if not pending_files:
        print("No pending files to process.")
        return

    total_tracked = 0
    batch_files = []
    batch_size = 0
    batch_index = 1

    print("Dry run: showing batches of files that would be transferred, up to ~100GB total")

    for file_id, filepath, filesize in pending_files:
        # 1. Stop before adding file that would exceed the run limit
        if total_tracked + filesize > MAX_TOTAL_TRANSFER_BYTES:
            if batch_files:
                print(f"\nBatch {batch_index}: {len(batch_files)} files, total size: {batch_size / 1_000_000_000:.2f} GB")
                for f in batch_files[:5]:
                    print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
                if len(batch_files) > 5:
                    print(f"  ... and {len(batch_files)-5} more files")
            print(f"\nReached total dry-run transfer limit of ~{MAX_TOTAL_TRANSFER_BYTES / 1e9:.1f} GB.")
            break

        # 2. If batch would overflow by file count or batch size, print batch summary and start new batch
        if len(batch_files) >= MAX_FILES_PER_BATCH or batch_size + filesize > MAX_BATCH_SIZE_BYTES:
            print(f"\nBatch {batch_index}: {len(batch_files)} files, total size: {batch_size / 1_000_000_000:.2f} GB")
            for f in batch_files[:5]:
                print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
            if len(batch_files) > 5:
                print(f"  ... and {len(batch_files)-5} more files")
            batch_index += 1
            batch_files = []
            batch_size = 0

        # 3. Now add the file to the batch
        batch_files.append((file_id, filepath, filesize))
        batch_size += filesize
        total_tracked += filesize

    # After loop, print any remaining files in the last batch
    # (only if we didn't hit the session total limit)
    # Note: Now this is correct, since breaking out above will skip this after session end
    else:
        if batch_files:
            print(f"\nBatch {batch_index}: {len(batch_files)} files, total size: {batch_size / 1_000_000_000:.2f} GB")
            for f in batch_files[:5]:
                print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
            if len(batch_files) > 5:
                print(f"  ... and {len(batch_files)-5} more files")

    print(f"\nTotal files selected for this dry run transfer: {total_tracked / 1_000_000_000:.2f} GB")



def dry_run_simulate_full(conn):
    MAX_FILES_PER_BATCH = 1000
    MAX_BATCH_SIZE_BYTES = 90 * 1_000_000_000
    MAX_TOTAL_TRANSFER_BYTES = 100 * 1_000_000_000

    cur = conn.cursor()
    cur.execute("SELECT id, path, size FROM files WHERE status='pending' ORDER BY id")
    all_pending_files = cur.fetchall()

    if not all_pending_files:
        print("No pending files to process.")
        return

    print("Dry run (simulated full transfer): showing batches grouped by sessions capped at ~100GB")

    # Simulate file status in memory
    simulated_pending = list(all_pending_files)
    session_count = 1

    while simulated_pending:
        total_tracked = 0
        batch_files = []
        batch_size = 0
        batch_index = 1

        print(f"\n=== Simulated session #{session_count} ===")

        i = 0
        while i < len(simulated_pending):
            file_id, filepath, filesize = simulated_pending[i]

            # Session cap: stop if session would be exceeded
            if total_tracked + filesize > MAX_TOTAL_TRANSFER_BYTES:
                break

            # Batch boundary: print and move to next batch if limit would be exceeded
            if len(batch_files) >= MAX_FILES_PER_BATCH or batch_size + filesize > MAX_BATCH_SIZE_BYTES:
                print_batch_summary(batch_index, batch_files, batch_size)
                batch_index += 1
                batch_files = []
                batch_size = 0
                continue  # Do not increment i, re-check this file in new batch

            # Add file to current batch
            batch_files.append((file_id, filepath, filesize))
            batch_size += filesize
            total_tracked += filesize

            # Remove from pending — simulating successful transfer
            simulated_pending.pop(i)
            # Don't increment i: we just shortened the list

        # Print the last batch if it has files
        if batch_files:
            print_batch_summary(batch_index, batch_files, batch_size)

        print(f"\nSimulated session #{session_count} total transfer: {total_tracked / 1_000_000_000:.2f} GB")

        if not simulated_pending:
            print("\nAll pending files simulated as transferred.")
            break
        session_count += 1

def print_batch_summary(batch_index, batch_files, batch_size):
    print(f"\nBatch {batch_index}: {len(batch_files)} files, total size: {batch_size / 1_000_000_000:.2f} GB")
    if len(batch_files) <= 4:
        for f in batch_files:
            print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
    else:
        # Print first two
        for f in batch_files[:2]:
            print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
        print("....")
        print("...")
        # Print last two
        for f in batch_files[-2:]:
            print(f"  {f[1]} ({f[2] / 1_000_000:.1f} MB)")
    print(f"  (total {len(batch_files)} files)")



def estimate_sessions(conn):
    MAX_FILES_PER_BATCH = 1000
    MAX_BATCH_SIZE_BYTES = 90 * 1_000_000_000
    MAX_TOTAL_TRANSFER_BYTES = 100 * 1_000_000_000

    cur = conn.cursor()
    cur.execute("SELECT id, path, size FROM files WHERE status='pending' ORDER BY id")
    pending_files = cur.fetchall()

    if not pending_files:
        print("No pending files to process.")
        return

    # Copy to mutable list to simulate processing
    simulated_pending = list(pending_files)
    sessions = []
    session_count = 1

    while simulated_pending:
        total_transferred = 0
        batch_files = []
        batch_size = 0
        batch_index = 1

        # For session size accumulation
        session_size = 0

        i = 0
        while i < len(simulated_pending):
            file_id, filepath, filesize = simulated_pending[i]

            # Stop adding files if this file would exceed session total limit
            if total_transferred + filesize > MAX_TOTAL_TRANSFER_BYTES:
                break

            # Start new batch if limits exceeded (batch file count or batch size)
            if len(batch_files) >= MAX_FILES_PER_BATCH or batch_size + filesize > MAX_BATCH_SIZE_BYTES:
                batch_index += 1
                batch_files = []
                batch_size = 0
                # Continue without incrementing i to reprocess the file in new batch
                continue

            # Add file to batch and session total
            batch_files.append((file_id, filepath, filesize))
            batch_size += filesize
            total_transferred += filesize

            # Remove it from pending files to simulate transfer progress
            simulated_pending.pop(i)
            # Do not increment i since we removed current file

        sessions.append(total_transferred)
        print(f"Session {session_count}: {total_transferred / 1_000_000_000:.2f} GB to transfer")
        session_count += 1

    print(f"\nTotal sessions needed: {len(sessions)}")



####################
# MAIN ENTRYPOINT
####################

def usage():
    print("Usage:")
    print("  python batchAdbPush.py prepare <source_folder>")
    print("  python batchAdbPush.py process")

def main():
    conn = init_db()

    def print_menu():
        print("\nSelect an operation:")
        print("  1) Prepare data (scan folder and insert into database)")
        print("  2) Process data (transfer files in batches)")
        print("  3) Dry run (show next files to be transferred)")
        print("  4) Dry run simulate full (simulate multiple transfer sessions)")
        print("  5) Estimate sessions (show how many sessions and sizes needed)")
        print("  0) Exit")

    while True:
        print_menu()
        choice = input("Enter choice: ").strip()

        if choice == '1':
            source_folder = input("Enter source folder path to scan: ").strip()
            if not os.path.isdir(source_folder):
                print(f"Error: source folder '{source_folder}' does not exist or is not a directory")
                continue
            scan_and_prepare(conn, source_folder)

        elif choice == '2':
            process_files(conn)

        elif choice == '3':
            dry_run(conn)

        elif choice == '4':
            dry_run_simulate_full(conn)

        elif choice == '5':
            estimate_sessions(conn)

        elif choice == '0':
            print("Exiting program.")
            break

        else:
            print("Invalid choice. Please try again.")

    conn.close()


if __name__ == "__main__":
    main()
