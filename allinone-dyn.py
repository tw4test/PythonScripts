import subprocess
import time
import re
import os
import sqlite3
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
import hashlib
from queue import Queue

from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.console import Console

import matplotlib.patches as patches
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.gridspec import GridSpec
from matplotlib.widgets import Button, TextBox
import tkinter as tk
from tkinter import filedialog
import tkinter.messagebox as msgbox
from matplotlib import rcParams

def run_adb_command(cmd):
    full_cmd = ["adb"] + cmd
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"ADB 命令失敗: {' '.join(full_cmd)}\n{result.stderr.strip()}")
        return result.stdout.strip()
    except Exception as e:
        log(f"ADB 執行錯誤: {e}")
        raise

def adb_create_remote_folder(remote_path):
    log(f"ADB: 建立遠端目錄: {remote_path}")
    run_adb_command(["shell", "mkdir", "-p", remote_path])

def check_all_files_processed_with_retry(conn, max_retries=3):
    """檢查是否所有文件都已處理 - 重試版本"""
    for attempt in range(max_retries):
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM files WHERE status IN ('pending', 'processing')")
            unfinished_count = cur.fetchone()[0]
            if unfinished_count == 0:
                return True
            if attempt < max_retries - 1:
                time.sleep(0.2)
        except Exception as e:
            log(f"檢查完成狀態失敗 (嘗試 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(0.5)

# 嘗試導入 rich 模塊
try:
    from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# 統一的日誌函數
def log(msg):
    """統一的時間戳日誌函數"""
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

# 初始化 console 對象
if RICH_AVAILABLE:
    console = Console()

    def rich_log(text):
        clean_text = re.sub(r'\[.*?\]', '', str(text))
        log(clean_text)
else:
    class SimpleConsole:
        def print(self, text, **kwargs):
            clean_text = re.sub(r'\[.*?\]', '', str(text))
            log(clean_text)
    console = SimpleConsole()

# 設定中文字型，視系統調整
rcParams['font.family'] = ['Microsoft JhengHei']
rcParams['axes.unicode_minus'] = False

# 參數與全局變數
DB_PATH = "filetransfer_new.db"
REMOTE_ROOT = "/sdcard/ToProcess"
CAMERA_ROOT = "/sdcard/DCIM/Camera"
BATCH_PREFIX = "batch_"

# CPU數據和狀態
cpu_data = deque(maxlen=60)
cpu_status_lock = threading.Lock()

# 分離的控制狀態
cpu_monitoring = False          # CPU監控狀態
batch_processing = False        # 批次處理狀態

status_text = "Idle"
params = {
    'batch_size': 500,
    'batch_size_gb': 90,
    'cpu_threshold': 50.0,
    'monitor_interval': 2.0,
    'backup_stable_time': 30,  # 30 secs is ok
    'quick_backup_detection': False,  # Disabled smart detect
    'duplicate_handling': 'smart',
    'hash_small_files_only': True,
    'small_file_threshold': 50 * 1024 * 1024,
    'max_rounds': 9999
}

# 控制旗標與狀態
cpu_active_flag = False
batch_processing_lock = threading.Lock()
batch_in_process = False
operation_lock = threading.Lock()

# /////////////////////////////////////////////////////////////////////////////
# UI狀態管理系統
class UIStateManager:
    """UI狀態管理器"""

    def __init__(self):
        self.current_state = 'idle'  # idle, processing, scanning
        self.state_lock = threading.Lock()
        self.last_button_clicks = {}

    def can_perform_action(self, action, min_interval=2.0):
        """檢查是否可以執行操作"""
        current_time = time.time()

        with self.state_lock:
            # 檢查點擊頻率
            last_click = self.last_button_clicks.get(action, 0)
            if current_time - last_click < min_interval:
                return False, f"請等待 {min_interval:.1f} 秒後再試"

            # 檢查狀態衝突
            if action == 'start_transfer':
                if self.current_state in ['processing', 'scanning']:
                    return False, f"當前狀態 '{self.current_state}' 不允許開始傳輸"

            elif action == 'scan_folder':
                if self.current_state == 'processing':
                    return False, "傳輸進行中，無法掃描資料夾"

            elif action == 'refresh':
                if self.current_state == 'processing':
                    return False, "處理中，建議稍後刷新"

            # 記錄點擊時間
            self.last_button_clicks[action] = current_time
            return True, "操作允許"

    def set_state(self, new_state):
        """設置新狀態"""
        with self.state_lock:
            old_state = self.current_state
            self.current_state = new_state
            log(f"[狀態變更] {old_state} -> {new_state}")
            self.update_ui_for_state()

    def get_state(self):
        """獲取當前狀態"""
        with self.state_lock:
            return self.current_state

    def update_ui_for_state(self):
        """根據狀態更新UI"""
        state_configs = {
            'idle': {
                'start_button': {'text': '開始傳輸', 'color': 'lightgreen', 'enabled': True},
                'scan_button': {'text': '掃描本地資料夾', 'color': 'lightblue', 'enabled': True},
                'stop_button': {'text': '停止傳輸', 'color': 'lightgray', 'enabled': False},
                'refresh_button': {'color': 'lightyellow', 'enabled': True}
            },
            'processing': {
                'start_button': {'text': '傳輸中...', 'color': 'orange', 'enabled': False},
                'scan_button': {'text': '掃描已禁用', 'color': 'lightgray', 'enabled': False},
                'stop_button': {'text': '停止傳輸', 'color': 'lightcoral', 'enabled': True},
                'refresh_button': {'color': 'lightgray', 'enabled': False}
            },
            'scanning': {
                'start_button': {'text': '開始傳輸', 'color': 'lightgray', 'enabled': False},
                'scan_button': {'text': '掃描中...', 'color': 'orange', 'enabled': False},
                'stop_button': {'text': '停止傳輸', 'color': 'lightgray', 'enabled': False},
                'refresh_button': {'color': 'lightgray', 'enabled': False}
            }
        }

        config = state_configs.get(self.current_state, state_configs['idle'])
        self.apply_button_config(config)

    def apply_button_config(self, config):
        """應用按鈕配置"""
        try:
            # 更新開始按鈕
            start_config = config.get('start_button', {})
            if 'text' in start_config:
                button_start.label.set_text(start_config['text'])
            if 'color' in start_config:
                button_start.color = start_config['color']
                button_start.hovercolor = start_config['color']

            # 更新掃描按鈕
            scan_config = config.get('scan_button', {})
            if 'text' in scan_config:
                button_scan.label.set_text(scan_config['text'])
            if 'color' in scan_config:
                button_scan.color = scan_config['color']
                button_scan.hovercolor = scan_config['color']

            # 更新停止按鈕
            stop_config = config.get('stop_button', {})
            if 'text' in stop_config:
                button_stop.label.set_text(stop_config['text'])
            if 'color' in stop_config:
                button_stop.color = stop_config['color']
                button_stop.hovercolor = stop_config['color']

            # 更新刷新按鈕
            refresh_config = config.get('refresh_button', {})
            if 'color' in refresh_config:
                button_refresh.color = refresh_config['color']
                button_refresh.hovercolor = refresh_config['color']

            # 重繪界面
            fig.canvas.draw_idle()

        except Exception as e:
            log(f"[UI錯誤] 更新按鈕狀態失敗: {e}")

# 創建全局狀態管理器
ui_state = UIStateManager()

# /////////////////////////////////////////////////////////////////////////////
# Storage-Aware Batch Manager
class StorageAwareBatchManager:
    """Storage-aware batch manager with dynamic sizing"""
    
    def __init__(self, conn):
        self.conn = conn
        self.current_batch_id = None
        self.batch_start_time = None
        self.batch_files = []
        self.batch_total_size = 0
        self.successful_pushes = 0
        
        # Storage management
        self.min_batch_size_gb = 5   # Minimum viable batch
        self.max_batch_size_gb = params.get('batch_size_gb', 90)
        self.storage_buffer_gb = 10  # Always keep 10GB free
        self.last_storage_check = 0
        self.storage_check_interval = 15  # Check every 15 seconds
        
    def get_phone_storage_info(self):
        """Get detailed phone storage information"""
        try:
            # Method 1: Use df command
            output = run_adb_command(['shell', 'df', '/sdcard'])
            
            for line in output.strip().split('\n'):
                if '/sdcard' in line or '/storage/emulated' in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        # df output: Filesystem 1K-blocks Used Available Use% Mounted
                        total_kb = int(parts[1])
                        available_kb = int(parts[3])
                        used_kb = total_kb - available_kb
                        
                        return {
                            'total_gb': total_kb / (1024 * 1024),
                            'available_gb': available_kb / (1024 * 1024),
                            'used_gb': used_kb / (1024 * 1024),
                            'used_percent': (used_kb / total_kb) * 100 if total_kb > 0 else 0
                        }
            
            return None
            
        except Exception as e:
            log(f"[存储检查] 获取存储信息失败: {e}")
            return None
    
    def calculate_safe_batch_size_adaptive(self, parallel_mode=True):
        """Calculate safe batch size with adaptive logic"""
        current_time = time.time()
        
        # Rate limit storage checks
        if current_time - self.last_storage_check < self.storage_check_interval:
            return min(self.max_batch_size_gb, params.get('batch_size_gb', 90))
        
        self.last_storage_check = current_time
        storage_info = self.get_phone_storage_info()
        
        if not storage_info:
            console.print("[yellow]⚠ 无法获取存储信息，使用保守批次大小[/yellow]")
            return self.min_batch_size_gb * 2  # Conservative fallback
        
        available_gb = storage_info['available_gb']
        used_percent = storage_info['used_percent']
        
        console.print(f"[blue]📱 存储状态: {available_gb:.1f}GB 可用 ({used_percent:.1f}% 已用)[/blue]")
        
        # Calculate safe space accounting for parallel processing
        reserve_space = self.storage_buffer_gb
        if parallel_mode:
            # Account for potential 2 batches + processing overhead
            usable_space = (available_gb - reserve_space) / 2.5
        else:
            # Single batch mode
            usable_space = available_gb - reserve_space
        
        # Apply size constraints
        safe_batch_size = max(self.min_batch_size_gb, usable_space)
        safe_batch_size = min(self.max_batch_size_gb, safe_batch_size)
        
        # Emergency reductions based on usage
        if used_percent > 95:
            safe_batch_size = self.min_batch_size_gb
            console.print(f"[red]🚨 存储危险 ({used_percent:.1f}%)，最小批次: {safe_batch_size}GB[/red]")
        elif used_percent > 90:
            safe_batch_size = min(safe_batch_size, self.min_batch_size_gb * 2)
            console.print(f"[yellow]⚠ 存储紧张 ({used_percent:.1f}%)，减少批次: {safe_batch_size:.1f}GB[/yellow]")
        elif used_percent > 80:
            safe_batch_size = min(safe_batch_size, safe_batch_size * 0.8)
            console.print(f"[yellow]📊 存储警告 ({used_percent:.1f}%)，调整批次: {safe_batch_size:.1f}GB[/yellow]")
        else:
            console.print(f"[green]✅ 存储充足，批次大小: {safe_batch_size:.1f}GB[/green]")
        
        return safe_batch_size

    def start_new_batch(self):
        """開始新的動態批次"""
        self.current_batch_id = f"batch_{int(time.time())}_{os.getpid()}"
        self.batch_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.batch_files = []
        self.batch_total_size = 0
        self.successful_pushes = 0
        log(f"[動態批次] 開始批次: {self.current_batch_id}")
        return self.current_batch_id

    def get_next_file_batch(self, max_files=None, max_size_gb=None):
        """獲取下一批待處理文件 (always use latest params)"""
        # Always use the latest values from params if not explicitly provided
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

        # 動態組合批次
        selected_files = []
        current_size = 0

        for file_id, path, size in pending_files:
            if (len(selected_files) >= max_files or
                    current_size + size > max_size_bytes):
                break

            selected_files.append({
                'id': file_id,
                'path': path,
                'size': size
            })
            current_size += size

        self.batch_files = selected_files
        self.batch_total_size = current_size

        # 記錄批次歷史
        if selected_files:
            try:
                cur.execute("""
                    INSERT INTO batch_history (virtual_batch_id, start_time, file_count, total_size)
                    VALUES (?, ?, ?, ?)
                """, (self.current_batch_id, self.batch_start_time,
                      len(selected_files), current_size))
                self.conn.commit()
            except Exception as e:
                log(f"[記錄錯誤] 無法記錄批次歷史: {e}")
        log(f"[動態批次] 選擇 {len(selected_files)} 個文件，總大小 {current_size/1024/1024:.1f}MB")
        return selected_files
    
    def get_next_file_batch_with_storage_awareness(self, parallel_mode=True):
        """Get next batch with storage-aware sizing"""
        # Calculate safe batch size
        safe_batch_size_gb = self.calculate_safe_batch_size_adaptive(parallel_mode)
        
        # Pre-flight storage check
        storage_info = self.get_phone_storage_info()
        if storage_info:
            if storage_info['available_gb'] < (safe_batch_size_gb + self.storage_buffer_gb):
                console.print("[red]⏸ 存储空间不足，等待清理[/red]")
                return []
        
        # Update current params with calculated size
        original_size = params.get('batch_size_gb', 90)
        if abs(safe_batch_size_gb - original_size) > 1:
            console.print(f"[cyan]🔄 动态调整: {original_size}GB → {safe_batch_size_gb:.1f}GB[/cyan]")
        
        # Get batch with calculated size
        return self.get_next_file_batch(
            max_files=params.get('batch_size', 1000),
            max_size_gb=safe_batch_size_gb
        )
    
    def verify_storage_after_cleanup(self, expected_freed_gb):
        """Verify storage was actually freed after cleanup"""
        try:
            time.sleep(2)  # Wait for filesystem sync
            storage_info = self.get_phone_storage_info()
            
            if storage_info:
                available_gb = storage_info['available_gb']
                console.print(f"[blue]📊 清理后存储: {available_gb:.1f}GB 可用[/blue]")
                
                # Check if we have reasonable space for next batch
                if available_gb > (self.min_batch_size_gb + self.storage_buffer_gb):
                    return True
                else:
                    console.print(f"[yellow]⚠ 清理后存储仍不足: {available_gb:.1f}GB[/yellow]")
                    return False
            return False
        except Exception as e:
            console.print(f"[red]存储验证失败: {e}[/red]")
            return False
    
    def emergency_storage_check(self):
        """Emergency check if storage is critically low"""
        storage_info = self.get_phone_storage_info()
        if storage_info:
            if storage_info['used_percent'] > 98:
                console.print("[red]🚨 存储严重不足，强制暂停[/red]")
                return False
            elif storage_info['available_gb'] < 2:
                console.print("[red]🚨 可用空间不足2GB，强制暂停[/red]")
                return False
        return True

    def mark_file_pushed(self, file_path):
        """標記文件為已推送 - 靜默版本"""
        cur = self.conn.cursor()
        push_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            # 檢查 push_time 列是否存在
            cur.execute("PRAGMA table_info(files)")
            columns = {row[1] for row in cur.fetchall()}

            if 'push_time' in columns:
                cur.execute("""
                    UPDATE files SET status='pushed', push_time=?, updated_at=CURRENT_TIMESTAMP
                    WHERE path=?
                """, (push_time, file_path))
            else:
                cur.execute("""
                    UPDATE files SET status='pushed', updated_at=CURRENT_TIMESTAMP
                    WHERE path=?
                """, (file_path,))

            if cur.rowcount > 0:
                self.successful_pushes += 1
                self.conn.commit()
                return True
            else:
                return False

        except Exception as e:
            console.print(f"[red]數據庫錯誤: {e}[/red]")
            return False

    def mark_file_failed(self, file_path, error_msg=None):
        """標記文件推送失敗 - 靜默版本"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE files SET status='failed', updated_at=CURRENT_TIMESTAMP
                WHERE path=?
            """, (file_path,))
            self.conn.commit()
        except Exception as e:
            console.print(f"[red]數據庫錯誤: {e}[/red]")

    def complete_batch(self, batch_status='completed'):
        """完成當前批次 - 修復版 (guard against zero-file batch)"""
        if not self.current_batch_id:
            return

        total_files = len(self.batch_files)
        if total_files == 0:
            log(f"[批次完成] {self.current_batch_id}: 無文件，批次略過 (成功推送: {self.successful_pushes})")
            self.successful_pushes = 0
            self.current_batch_id = None
            self.batch_files = []
            return

        try:
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            cur = self.conn.cursor()
            cur.execute("""
                UPDATE batch_history
                SET end_time=?, success_count=?, status=?
                WHERE virtual_batch_id=?
            """, (end_time, self.successful_pushes, batch_status, self.current_batch_id))
            self.conn.commit()

            success_rate = (self.successful_pushes / total_files) * 100
            log(f"[批次完成] {self.current_batch_id}: {self.successful_pushes}/{total_files} ({success_rate:.1f}%)")

        except Exception as e:
            log(f"[批次記錄錯誤] 無法更新批次狀態: {e}")
        finally:
            # 重置狀態
            self.current_batch_id = None
            self.batch_files = []
            self.successful_pushes = 0
# ...existing code...

# /////////////////////////////////////////////////////////////////////////////
# Safe Parallel Batch Scheduler with Storage Management
class SafeParallelBatchScheduler:
    """Safe parallel scheduler with comprehensive storage management"""
    
    def __init__(self, conn):
        self.conn = conn
        self.storage_manager = StorageAwareBatchManager(conn)
        self.push_queue = Queue(maxsize=1)  # More conservative queue
        self.running = False
        self.total_batches_pushed = 0
        self.total_batches_processed = 0
        
        # Thread management
        self.push_thread = None
        self.process_thread = None
        
    def _push_worker(self):
        """Enhanced push worker with storage monitoring"""
        consecutive_failures = 0
        max_failures = 3
        
        while self.running and batch_processing:
            try:
                # Emergency storage check
                if not self.storage_manager.emergency_storage_check():
                    console.print("[red]🛑 存储紧急暂停，等待60秒[/red]")
                    time.sleep(60)
                    continue
                
                # Check if we can push (queue not full)
                if self.push_queue.qsize() == 0:
                    # Get storage-aware batch
                    file_batch = self.storage_manager.get_next_file_batch_with_storage_awareness(
                        parallel_mode=True
                    )
                    
                    if file_batch:
                        # Pre-push storage verification
                        batch_size_gb = sum(f['size'] for f in file_batch) / (1024**3)
                        
                        # Double-check storage before push
                        storage_info = self.storage_manager.get_phone_storage_info()
                        if storage_info:
                            required_space = batch_size_gb + self.storage_manager.storage_buffer_gb
                            if storage_info['available_gb'] < required_space:
                                console.print(f"[yellow]⏸ 推送前检查: 需要{required_space:.1f}GB，仅有{storage_info['available_gb']:.1f}GB[/yellow]")
                                time.sleep(30)
                                continue
                        
                        # Proceed with push
                        batch_id = self.storage_manager.start_new_batch()
                        console.print(f"[cyan]📤 推送批次 {self.total_batches_pushed + 1}: {len(file_batch)} 文件 ({batch_size_gb:.1f}GB)[/cyan]")
                        
                        remote_temp_folder = f"{REMOTE_ROOT}/batch_temp_{int(time.time())}"
                        success_count = push_files_individually(
                            self.storage_manager, file_batch, remote_temp_folder
                        )
                        
                        if success_count > 0:
                            batch_info = {
                                'batch_id': batch_id,
                                'batch_manager': self.storage_manager,
                                'file_batch': file_batch,
                                'remote_temp_folder': remote_temp_folder,
                                'success_count': success_count,
                                'batch_size_gb': batch_size_gb
                            }
                            
                            try:
                                self.push_queue.put(batch_info, timeout=30)
                                self.total_batches_pushed += 1
                                consecutive_failures = 0
                                console.print(f"[green]✅ 批次 {self.total_batches_pushed} 推送完成[/green]")
                            except:
                                console.print("[yellow]⚠ 处理队列满，等待处理[/yellow]")
                                time.sleep(10)
                        else:
                            console.print(f"[red]❌ 批次推送失败: {batch_id}[/red]")
                            self.storage_manager.complete_batch('failed')
                            consecutive_failures += 1
                            
                            if consecutive_failures >= max_failures:
                                console.print(f"[red]🛑 连续{max_failures}次推送失败，暂停推送[/red]")
                                time.sleep(120)
                                consecutive_failures = 0
                    else:
                        # No more files to process
                        if check_all_files_processed(self.conn):
                            console.print("[green]📤 所有文件推送完成[/green]")
                            break
                        time.sleep(5)
                else:
                    # Queue full, wait for processing
                    time.sleep(15)
                    
            except Exception as e:
                console.print(f"[red]推送线程错误: {e}[/red]")
                consecutive_failures += 1
                time.sleep(min(10 * consecutive_failures, 60))
                
    def _process_worker(self):
        """Enhanced process worker with cleanup verification"""
        while self.running and batch_processing:
            try:
                # Check CPU status
                with cpu_status_lock:
                    cpu_idle = not cpu_active_flag
                
                if cpu_idle and not self.push_queue.empty():
                    try:
                        batch_info = self.push_queue.get(timeout=5)
                        
                        console.print(f"[yellow]📱 处理批次 {self.total_batches_processed + 1}: {batch_info['batch_id']}[/yellow]")
                        
                        # Move to Camera with storage verification
                        camera_folder = f"{CAMERA_ROOT}/batch_{int(time.time())}"
                        
                        if move_remote_folder_safe(batch_info['remote_temp_folder'], camera_folder):
                            mark_pushed_files_completed(self.conn, batch_info['file_batch'])
                            
                            console.print("[yellow]⏳ 等待 Google Photos 处理...[/yellow]")
                            backup_completed = wait_for_backup_complete()
                            
                            if backup_completed:
                                # Enhanced cleanup with verification
                                console.print(f"[cyan]🧹 清理 Camera 目录: {camera_folder}[/cyan]")
                                cleanup_camera_folder(camera_folder)
                                
                                # Verify cleanup freed space
                                if self.storage_manager.verify_storage_after_cleanup(batch_info['batch_size_gb']):
                                    self.total_batches_processed += 1
                                    batch_info['batch_manager'].complete_batch('completed')
                                    console.print(f"[green]✅ 批次 {self.total_batches_processed} 完成，存储已释放[/green]")
                                else:
                                    console.print("[yellow]⚠ 清理验证失败，但标记为完成[/yellow]")
                                    self.total_batches_processed += 1
                                    batch_info['batch_manager'].complete_batch('completed')
                            else:
                                console.print("[yellow]⚠ 备份被中断[/yellow]")
                                batch_info['batch_manager'].complete_batch('interrupted')
                                self.total_batches_processed += 1
                        else:
                            console.print("[red]❌ 批次移动失败[/red]")
                            batch_info['batch_manager'].complete_batch('failed')
                            
                    except:
                        # Queue was empty, continue
                        pass
                        
                elif not cpu_idle:
                    time.sleep(params['monitor_interval'])
                else:
                    # No batch to process, wait
                    if self.total_batches_pushed > self.total_batches_processed:
                        time.sleep(2)
                    else:
                        if not self.running or not batch_processing:
                            break
                        time.sleep(5)
                        
            except Exception as e:
                console.print(f"[red]处理线程错误: {e}[/red]")
                time.sleep(10)
                
    def start_safe_parallel_processing(self):
        """Start safe parallel processing"""
        if self.running:
            return True
            
        # Initial storage check
        storage_info = self.storage_manager.get_phone_storage_info()
        if storage_info:
            if storage_info['used_percent'] > 95:
                console.print("[red]⚠ 警告: 存储空间严重不足，建议先清理手机[/red]")
                return False
            elif storage_info['available_gb'] < 15:
                console.print("[red]⚠ 警告: 可用空间少于15GB，不建议并行处理[/red]")
                return False
        
        self.running = True
        console.print("[bold green]🚀 安全并行处理启动[/bold green]")
        
        # Start worker threads
        self.push_thread = threading.Thread(target=self._push_worker, daemon=True)
        self.process_thread = threading.Thread(target=self._process_worker, daemon=True)
        
        self.push_thread.start()
        self.process_thread.start()
        
        return True
        
    def stop_safe_parallel_processing(self):
        """Stop safe parallel processing"""
        self.running = False
        console.print("[yellow]⏹ 停止安全并行处理[/yellow]")
        
    def get_status(self):
        """Get current processing status"""
        return {
            'running': self.running,
            'queue_size': self.push_queue.qsize(),
            'total_pushed': self.total_batches_pushed,
            'total_processed': self.total_batches_processed
        }

# /////////////////////////////////////////////////////////////////////////////
# Enhanced Parallel Processing Thread
def safe_parallel_batch_process_thread():
    """Safe parallel batch processing with storage management"""
    global batch_in_process, batch_processing
    
    try:
        conn = init_db()
        scheduler = SafeParallelBatchScheduler(conn)
        
        # Start safe parallel processing
        if not scheduler.start_safe_parallel_processing():
            console.print("[red]❌ 无法启动并行处理 - 存储空间不足[/red]")
            return
        
        # Monitor processing
        last_status_time = time.time()
        while batch_processing:
            status = scheduler.get_status()
            
            # Status reporting every 30 seconds
            if time.time() - last_status_time > 30:
                storage_info = scheduler.storage_manager.get_phone_storage_info()
                if storage_info:
                    console.print(f"[blue]📊 进度: 推送{status['total_pushed']}/处理{status['total_processed']}, 存储:{storage_info['available_gb']:.1f}GB[/blue]")
                last_status_time = time.time()
            
            # Check completion
            if (status['total_pushed'] > 0 and 
                status['total_pushed'] == status['total_processed'] and 
                status['queue_size'] == 0):
                
                if check_all_files_processed(conn):
                    console.print(f"[bold green]🎉 安全并行处理完成! 处理{status['total_processed']}个批次[/bold green]")
                    show_completion_notification(status['total_processed'])
                    break
                    
            time.sleep(3)
            
        scheduler.stop_safe_parallel_processing()
        conn.close()
        
    except Exception as e:
        console.print(f"[red]安全并行处理错误: {e}[/red]")
    finally:
        batch_processing = False
        ui_state.set_state('idle')
        update_pending_count_text()

# /////////////////////////////////////////////////////////////////////////////
# Helper Functions for Storage Management
def get_current_batch_size():
    """Get current batch size for optimization"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM files WHERE status='pending'")
        pending_count = cur.fetchone()[0]
        conn.close()
        return pending_count
    except:
        return 0

def enhanced_batch_completion_check(conn, batch_manager, total_processed_batches):
    """Enhanced completion check with storage awareness"""
    try:
        if check_all_files_processed(conn):
            storage_info = batch_manager.get_phone_storage_info() if hasattr(batch_manager, 'get_phone_storage_info') else None
            
            console.print(f"[bold green]🎉 所有文件处理完成！总共处理 {total_processed_batches} 个批次[/bold green]")
            
            if storage_info:
                console.print(f"[blue]📱 最终存储状态: {storage_info['available_gb']:.1f}GB 可用 ({storage_info['used_percent']:.1f}% 已用)[/blue]")
            
            show_completion_notification(total_processed_batches)
            return True
        return False
    except Exception as e:
        console.print(f"[red]完成检查错误: {e}[/red]")
        return False

# /////////////////////////////////////////////////////////////////////////////
# Updated UI Callbacks for Safe Parallel Processing
def on_start_safe_parallel(event):
    """Start safe parallel processing"""
    apply_params_from_ui()
    
    can_start, message = ui_state.can_perform_action('start_transfer', 3.0)
    if not can_start:
        print(f"[防护] {message}")
        return

    global batch_processing
    if batch_processing:
        print("[提示] 传输已在进行中")
        return

    # Check prerequisites
    pending_count = query_pending_files_count()
    if pending_count == 0:
        print("[提示] 没有待处理文件，请先扫描资料夹")
        return

    # Ensure CPU monitoring
    if not cpu_monitoring:
        print("[警告] CPU监控未启动，正在自动启动...")
        auto_start_cpu_monitoring()
        time.sleep(1)

    # Check ADB
    try:
        run_adb_command(['devices'])
        print("[检查] ADB连接正常")
    except Exception as e:
        print(f"[错误] ADB连接失败: {e}")
        return

    ui_state.set_state('processing')
    batch_processing = True
    
    log("[UI] 启动安全并行批次处理")
    threading.Thread(target=safe_parallel_batch_process_thread, daemon=True).start()
    
    print(f"[成功] 安全并行处理已启动，待处理: {pending_count}")

# /////////////////////////////////////////////////////////////////////////////
# Storage Monitoring UI Functions
def display_storage_status():
    """Display current storage status in console"""
    try:
        storage_manager = StorageAwareBatchManager(sqlite3.connect(DB_PATH))
        storage_info = storage_manager.get_phone_storage_info()
        
        if storage_info:
            console.print(f"[blue]📱 当前存储状态:[/blue]")
            console.print(f"  总容量: {storage_info['total_gb']:.1f}GB")
            console.print(f"  可用空间: {storage_info['available_gb']:.1f}GB")
            console.print(f"  已用空间: {storage_info['used_gb']:.1f}GB ({storage_info['used_percent']:.1f}%)")
            
            # Storage recommendations
            if storage_info['used_percent'] > 90:
                console.print("[red]⚠ 建议: 存储空间紧张，建议清理手机存储[/red]")
            elif storage_info['used_percent'] > 80:
                console.print("[yellow]💡 建议: 存储使用率较高，注意空间管理[/yellow]")
            else:
                console.print("[green]✅ 存储空间充足[/green]")
        else:
            console.print("[red]❌ 无法获取存储信息，请检查ADB连接[/red]")
            
    except Exception as e:
        console.print(f"[red]存储状态检查失败: {e}[/red]")

def on_check_storage(event):
    """Check storage status button callback"""
    can_check, message = ui_state.can_perform_action('check_storage', 2.0)
    if not can_check:
        print(f"[防护] {message}")
        return
        
    display_storage_status()

# /////////////////////////////////////////////////////////////////////////////
# Updated Button Bindings and UI Layout
def setup_enhanced_ui():
    """Setup enhanced UI with storage management"""
    global button_start, button_storage_check
    
    # Update the start button to use safe parallel processing
    button_start.on_clicked(on_start_safe_parallel)
    
    # Add storage check button
    ax_storage_check = plt.axes([0.44, 0.12, 0.1, 0.05])
    button_storage_check = Button(ax_storage_check, '检查存储')
    button_storage_check.label.set_fontsize(10)
    button_storage_check.on_clicked(on_check_storage)
    
    return button_storage_check

# /////////////////////////////////////////////////////////////////////////////
# Enhanced Startup and Initialization
def enhanced_startup_initialization():
    """Enhanced startup with storage awareness"""
    log("[系统启动] 正在初始化存储感知系统...")
    
    # Test storage detection
    try:
        test_manager = StorageAwareBatchManager(sqlite3.connect(DB_PATH))
        storage_info = test_manager.get_phone_storage_info()
        
        if storage_info:
            log(f"[存储检测] 手机存储: {storage_info['available_gb']:.1f}GB 可用")
            if storage_info['used_percent'] > 90:
                log("[存储警告] 手机存储空间不足，建议先清理")
        else:
            log("[存储警告] 无法检测手机存储，将使用保守模式")
            
    except Exception as e:
        log(f"[存储错误] 存储检测失败: {e}")
    
    log("[系统启动] 存储感知系统已就绪")

# /////////////////////////////////////////////////////////////////////////////
# Replace the existing main execution with enhanced version
if __name__ == "__main__":
    log("[系统启动] 正在初始化...")

    # 修复现有数据庫結構
    log("[系统启动] 检查并修复数据库...")
    fix_existing_database()

    # 初始化数据庫
    conn = init_db()
    update_pending_count_text()
    conn.close()

    # 初始化UI状态管理
    ui_state.set_state('idle')

    # Enhanced startup initialization
    enhanced_startup_initialization()

    # Setup enhanced UI
    button_storage_check = setup_enhanced_ui()

    # 自动启动CPU监控
    log("[系统启动] 正在启动CPU监控...")
    auto_start_cpu_monitoring()
    log("[系统启动] CPU监控已启动")
    
    # Enhanced startup messages
    log("[系统提示] 点击'开始传输'按钮开始安全并行批次处理")
    log("[系统提示] UI状态管理已启用 - 防止重复操作")
    log("[系统说明] 存储感知批次管理 - 动态调整批次大小")
    log("[系统说明] 并行处理模式 - 推送与处理重叠执行")
    log("[安全特性] 存储空间监控 - 防止手机存储溢出")

    # plt.tight_layout()
    plt.subplots_adjust()
    plt.show()            