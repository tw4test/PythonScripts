import subprocess
import time
import threading
import os
import sqlite3
import hashlib
from collections import deque
from datetime import datetime
import re

# Try to import rich modules
try:
    from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Initialize console
if RICH_AVAILABLE:
    console = Console()
else:
    class SimpleConsole:
        def print(self, text, **kwargs):
            print(text)
    console = SimpleConsole()

# Global variables and configuration
DB_PATH = "filetransfer_new.db"
REMOTE_ROOT = "/sdcard/ToProcess"
CAMERA_ROOT = "/sdcard/DCIM/Camera"
BATCH_PREFIX = "batch_"

# CPU monitoring data
cpu_data = deque(maxlen=60)
cpu_status_lock = threading.Lock()
cpu_monitoring = False
batch_processing = False
cpu_active_flag = False
batch_in_process = False
status_text = "Idle"

# Parameters
params = {
    'batch_size': 500,
    'batch_size_gb': 90,
    'cpu_threshold': 50.0,
    'monitor_interval': 2.0,
    'backup_stable_time': 30,
    'max_rounds': 9999
}

def log(msg):
    """Unified logging function with timestamp"""
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

# ADB Functions
def run_adb_command(cmd):
    full_cmd = ["adb"] + cmd
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"ADB command failed: {' '.join(full_cmd)}\n{result.stderr.strip()}")
        return result.stdout.strip()
    except Exception as e:
        log(f"ADB execution error: {e}")
        raise

def adb_create_remote_folder(remote_path):
    log(f"ADB: Creating remote directory: {remote_path}")
    run_adb_command(["shell", "mkdir", "-p", remote_path])

def run_remote_shell_script(script_path):
    """Execute remote shell script"""
    try:
        log(f"[ADB] Executing script: {script_path}")
        output = run_adb_command(["shell", "sh", script_path])
        console.print(f"[green]✓ Script executed successfully: {script_path}[/green]")
        if output:
            console.print(f"[blue]{output}[/blue]")
    except Exception as e:
        console.print(f"[red]✗ Script execution failed: {e}[/red]")

# Database functions (simplified versions from original)
def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Create tables if not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            size INTEGER,
            mtime INTEGER,
            status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'pushed', 'completed', 'failed')),
            file_hash TEXT NULL,
            push_time TEXT NULL,
            completed_time TEXT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            virtual_batch_id TEXT,
            start_time TEXT,
            end_time TEXT NULL,
            file_count INTEGER,
            total_size INTEGER,
            success_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'processing' CHECK(status IN ('processing', 'completed', 'failed', 'interrupted')),
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    log("[Database] Initialization completed")
    return conn

def query_pending_files_count():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM files WHERE status IN ('pending', 'failed')")
        pending_count = cur.fetchone()[0]
        conn.close()
        return pending_count
    except Exception as e:
        log(f"Failed to query pending files count: {e}")
        return 0

# CPU Monitoring Functions
def get_pid():
    try:
        pid_output = run_adb_command(['shell', 'pidof', 'com.google.android.apps.photos'])
        if pid_output:
            return pid_output.split()[0]
    except Exception:
        return None

def get_cpu_usage():
    try:
        pid = get_pid()
        if not pid:
            return 0.0
        
        output = run_adb_command(['shell', 'top', '-n', '1'])
        for line in output.splitlines():
            if pid in line and 'grep' not in line:
                parts = line.split()
                if len(parts) > 8:
                    cpu_str = parts[8]
                    try:
                        return float(cpu_str.strip('%'))
                    except Exception:
                        return 0.0
        return 0.0
    except Exception as e:
        log(f"Error getting CPU usage: {e}")
        return 0.0

def cpu_monitor_thread():
    """CPU monitoring thread for CLI"""
    global cpu_monitoring, status_text, cpu_active_flag
    log("[CPU Monitor] Thread started")
    
    while cpu_monitoring:
        try:
            cpu = get_cpu_usage()
            with cpu_status_lock:
                cpu_data.append(cpu)
                avg_cpu = sum(cpu_data) / len(cpu_data) if cpu_data else 0.0
                cpu_active_flag = avg_cpu > params['cpu_threshold']
            
            if cpu_active_flag:
                status_text = f"Active (Avg CPU: {avg_cpu:.1f}%)"
            else:
                status_text = f"Idle (Avg CPU: {avg_cpu:.1f}%)"
            
            time.sleep(params['monitor_interval'])
            
        except Exception as e:
            log(f"[CPU Monitor] Error: {e}")
            time.sleep(5)
    
    log("[CPU Monitor] Thread ended")

# CLI CPU Display Functions
def create_cpu_chart_ascii(width=50):
    """Create ASCII art CPU chart"""
    if not cpu_data:
        return "No CPU data available"
    
    chart = []
    max_cpu = max(max(cpu_data), params['cpu_threshold'])
    scale = width / max_cpu if max_cpu > 0 else 1
    
    # Current CPU bar
    current_cpu = cpu_data[-1] if cpu_data else 0
    bar_length = int(current_cpu * scale)
    bar = "█" * bar_length + "░" * (width - bar_length)
    
    # Threshold indicator
    threshold_pos = int(params['cpu_threshold'] * scale)
    bar_list = list(bar)
    if threshold_pos < len(bar_list):
        bar_list[threshold_pos] = "|"
    
    chart.append(f"CPU: {current_cpu:5.1f}% {''.join(bar_list)} ({params['cpu_threshold']:.0f}%)")
    
    # Mini history (last 10 readings)
    if len(cpu_data) >= 10:
        mini_history = list(cpu_data)[-10:]
        history_scale = 10 / max(mini_history) if max(mini_history) > 0 else 1
        history_bars = []
        for cpu_val in mini_history:
            bar_height = int(cpu_val * history_scale)
            if bar_height >= 8:
                history_bars.append("█")
            elif bar_height >= 6:
                history_bars.append("▆")
            elif bar_height >= 4:
                history_bars.append("▄")
            elif bar_height >= 2:
                history_bars.append("▂")
            else:
                history_bars.append("▁")
        
        chart.append(f"Hist: {''.join(history_bars)} (last 10 readings)")
    
    return "\n".join(chart)

def display_cpu_status():
    """Display current CPU status in CLI"""
    if RICH_AVAILABLE:
        # Rich version with table
        table = Table(title="Google Photos CPU Monitor")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        current_cpu = cpu_data[-1] if cpu_data else 0
        avg_cpu = sum(cpu_data) / len(cpu_data) if cpu_data else 0
        status = "🟢 Active" if cpu_active_flag else "🔴 Idle"
        
        table.add_row("Current CPU", f"{current_cpu:.1f}%")
        table.add_row("Average CPU", f"{avg_cpu:.1f}%")
        table.add_row("Threshold", f"{params['cpu_threshold']:.1f}%")
        table.add_row("Status", status)
        table.add_row("Data Points", str(len(cpu_data)))
        
        console.print(table)
        console.print(create_cpu_chart_ascii())
    else:
        # Simple text version
        current_cpu = cpu_data[-1] if cpu_data else 0
        avg_cpu = sum(cpu_data) / len(cpu_data) if cpu_data else 0
        status = "Active" if cpu_active_flag else "Idle"
        
        print("\n=== Google Photos CPU Monitor ===")
        print(f"Current CPU: {current_cpu:.1f}%")
        print(f"Average CPU: {avg_cpu:.1f}%")
        print(f"Threshold:   {params['cpu_threshold']:.1f}%")
        print(f"Status:      {status}")
        print(f"Data Points: {len(cpu_data)}")
        print(create_cpu_chart_ascii())
        print("=" * 35)

# Simplified batch processing (core functions from original)
class DynamicBatchManager:
    def __init__(self, conn):
        self.conn = conn
        self.current_batch_id = None
        self.batch_start_time = None
        self.batch_files = []
        self.successful_pushes = 0
    
    def start_new_batch(self):
        self.current_batch_id = f"batch_{int(time.time())}_{os.getpid()}"
        self.batch_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.batch_files = []
        self.successful_pushes = 0
        log(f"[Dynamic Batch] Started batch: {self.current_batch_id}")
        return self.current_batch_id
    
    def get_next_file_batch(self, max_files=None, max_size_gb=None):
        max_files = params.get('batch_size', 1000) if max_files is None else max_files
        max_size_gb = params.get('batch_size_gb', 90) if max_size_gb is None else max_size_gb
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, path, size
            FROM files
            WHERE status='pending'
            ORDER BY id ASC
            LIMIT ?
        """, (max_files * 2,))
        
        pending_files = cur.fetchall()
        if not pending_files:
            return []
        
        selected_files = []
        current_size = 0
        
        for file_id, path, size in pending_files:
            if (len(selected_files) >= max_files or current_size + size > max_size_bytes):
                break
            selected_files.append({'id': file_id, 'path': path, 'size': size})
            current_size += size
        
        self.batch_files = selected_files
        log(f"[Dynamic Batch] Selected {len(selected_files)} files, total size {current_size/1024/1024:.1f}MB")
        return selected_files

# CLI Menu System
def show_main_menu():
    """Display main menu"""
    if RICH_AVAILABLE:
        panel = Panel.fit(
            "[bold cyan]Google Photos File Transfer - CLI Version[/bold cyan]\n\n"
            "[1] Start Dynamic Batch Transfer\n"
            "[2] Scan Local Folder\n"
            "[3] Show CPU Status\n"
            "[4] Show Pending Files Count\n"
            "[5] Execute Phone Scripts\n"
            "[6] Configure Parameters\n"
            "[7] Show Statistics\n"
            "[8] Start/Stop CPU Monitoring\n"
            "[0] Exit\n",
            title="Main Menu"
        )
        console.print(panel)
    else:
        print("\n" + "="*50)
        print("Google Photos File Transfer - CLI Version")
        print("="*50)
        print("[1] Start Dynamic Batch Transfer")
        print("[2] Scan Local Folder")
        print("[3] Show CPU Status")
        print("[4] Show Pending Files Count")
        print("[5] Execute Phone Scripts")
        print("[6] Configure Parameters")
        print("[7] Show Statistics")
        print("[8] Start/Stop CPU Monitoring")
        print("[0] Exit")
        print("="*50)

def show_scripts_menu():
    """Display scripts menu"""
    print("\n=== Phone Scripts ===")
    print("[1] Execute scan.sh")
    print("[2] Execute clean.sh")
    print("[3] Execute refresh.sh")
    print("[0] Back to main menu")

def configure_parameters():
    """Configure system parameters"""
    global params
    
    print("\n=== Configure Parameters ===")
    print(f"Current parameters:")
    for key, value in params.items():
        print(f"  {key}: {value}")
    
    while True:
        print("\nSelect parameter to change:")
        print("[1] batch_size")
        print("[2] batch_size_gb")
        print("[3] cpu_threshold")
        print("[4] monitor_interval")
        print("[5] backup_stable_time")
        print("[6] max_rounds")
        print("[0] Back")
        
        choice = input("Enter choice: ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            try:
                new_val = int(input(f"Enter new batch_size (current: {params['batch_size']}): "))
                params['batch_size'] = new_val
                log(f"Updated batch_size to {new_val}")
            except ValueError:
                print("Invalid input")
        elif choice == "2":
            try:
                new_val = float(input(f"Enter new batch_size_gb (current: {params['batch_size_gb']}): "))
                params['batch_size_gb'] = new_val
                log(f"Updated batch_size_gb to {new_val}")
            except ValueError:
                print("Invalid input")
        elif choice == "3":
            try:
                new_val = float(input(f"Enter new cpu_threshold (current: {params['cpu_threshold']}): "))
                params['cpu_threshold'] = new_val
                log(f"Updated cpu_threshold to {new_val}")
            except ValueError:
                print("Invalid input")
        elif choice == "4":
            try:
                new_val = float(input(f"Enter new monitor_interval (current: {params['monitor_interval']}): "))
                params['monitor_interval'] = new_val
                log(f"Updated monitor_interval to {new_val}")
            except ValueError:
                print("Invalid input")
        elif choice == "5":
            try:
                new_val = int(input(f"Enter new backup_stable_time (current: {params['backup_stable_time']}): "))
                params['backup_stable_time'] = new_val
                log(f"Updated backup_stable_time to {new_val}")
            except ValueError:
                print("Invalid input")
        elif choice == "6":
            try:
                new_val = int(input(f"Enter new max_rounds (current: {params['max_rounds']}): "))
                params['max_rounds'] = new_val
                log(f"Updated max_rounds to {new_val}")
            except ValueError:
                print("Invalid input")

def show_statistics():
    """Show system statistics"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # File statistics
        cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
        file_stats = dict(cur.fetchall())
        
        # Batch statistics
        cur.execute("SELECT status, COUNT(*) FROM batch_history GROUP BY status")
        batch_stats = dict(cur.fetchall())
        
        conn.close()
        
        if RICH_AVAILABLE:
            table = Table(title="System Statistics")
            table.add_column("Category", style="cyan")
            table.add_column("Status", style="yellow")
            table.add_column("Count", style="green")
            
            for status, count in file_stats.items():
                table.add_row("Files", status, str(count))
            
            for status, count in batch_stats.items():
                table.add_row("Batches", status, str(count))
            
            console.print(table)
        else:
            print("\n=== System Statistics ===")
            print("Files:")
            for status, count in file_stats.items():
                print(f"  {status}: {count}")
            print("Batches:")
            for status, count in batch_stats.items():
                print(f"  {status}: {count}")
    
    except Exception as e:
        log(f"Error getting statistics: {e}")

def start_cpu_monitoring():
    """Start CPU monitoring"""
    global cpu_monitoring
    if not cpu_monitoring:
        cpu_monitoring = True
        threading.Thread(target=cpu_monitor_thread, daemon=True).start()
        log("[CPU Monitor] Started")
    else:
        log("[CPU Monitor] Already running")

def stop_cpu_monitoring():
    """Stop CPU monitoring"""
    global cpu_monitoring
    cpu_monitoring = False
    log("[CPU Monitor] Stopped")

def scan_folder_cli():
    """CLI version of folder scanning"""
    folder_path = input("Enter folder path to scan: ").strip()
    
    if not os.path.exists(folder_path):
        print("Folder does not exist!")
        return
    
    print(f"Scanning folder: {folder_path}")
    
    # Simplified scanning logic
    conn = init_db()
    files_added = 0
    
    try:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    stat_info = os.stat(file_path)
                    
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM files WHERE path=?", (file_path,))
                    
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO files (path, size, mtime, status)
                            VALUES (?, ?, ?, 'pending')
                        """, (file_path, stat_info.st_size, int(stat_info.st_mtime)))
                        files_added += 1
        
        conn.commit()
        conn.close()
        
        log(f"Scan completed: {files_added} new files added")
        
    except Exception as e:
        log(f"Scan error: {e}")

def main_cli():
    """Main CLI loop"""
    print("Initializing system...")
    
    # Initialize database
    conn = init_db()
    conn.close()
    
    # Start CPU monitoring by default
    start_cpu_monitoring()
    
    while True:
        show_main_menu()
        choice = input("\nEnter your choice: ").strip()
        
        if choice == "0":
            print("Exiting...")
            stop_cpu_monitoring()
            break
        elif choice == "1":
            if batch_processing:
                print("Batch processing already running!")
            else:
                print("Starting dynamic batch transfer...")
                # Here you would implement the batch processing
                # For now, just show a message
                print("Batch transfer would start here (implementation needed)")
        elif choice == "2":
            scan_folder_cli()
        elif choice == "3":
            display_cpu_status()
            input("\nPress Enter to continue...")
        elif choice == "4":
            count = query_pending_files_count()
            print(f"Pending files: {count:,}")
            input("Press Enter to continue...")
        elif choice == "5":
            while True:
                show_scripts_menu()
                script_choice = input("Enter choice: ").strip()
                if script_choice == "0":
                    break
                elif script_choice == "1":
                    run_remote_shell_script("/sdcard/ToProcess/scan.sh")
                elif script_choice == "2":
                    run_remote_shell_script("/sdcard/ToProcess/clean.sh")
                elif script_choice == "3":
                    run_remote_shell_script("/sdcard/ToProcess/refresh.sh")
        elif choice == "6":
            configure_parameters()
        elif choice == "7":
            show_statistics()
            input("Press Enter to continue...")
        elif choice == "8":
            if cpu_monitoring:
                stop_cpu_monitoring()
            else:
                start_cpu_monitoring()
        else:
            print("Invalid choice!")

if __name__ == "__main__":
    main_cli()