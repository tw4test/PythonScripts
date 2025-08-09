import subprocess
import time
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
import re
import os
import sqlite3
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
import hashlib

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


def log_without_timestamp(msg):
    """無時間戳的日誌函數（用於已有時間戳的日誌）"""
    print(msg)


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
# 數據庫修復函數
def fix_existing_database():
    """修復現有數據庫結構"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # 添加缺失的列
        columns_to_add = ['push_time', 'completed_time', 'file_hash']

        for column in columns_to_add:
            try:
                cur.execute(f"ALTER TABLE files ADD COLUMN {column} TEXT")
                log(f"[修復] 已添加列: {column}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    log(f"[跳過] 列 {column} 已存在")
                else:
                    log(f"[錯誤] 添加列 {column} 失敗: {e}")

        conn.commit()
        conn.close()
        log("[修復] 數據庫結構修復完成")
    except Exception as e:
        log(f"[錯誤] 數據庫修復失敗: {e}")


# /////////////////////////////////////////////////////////////////////////////
# SQLite 操作
def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 檢查並升級現有的 files 表
    try:
        # 先檢查現有表結構
        cur.execute("PRAGMA table_info(files)")
        existing_columns = {row[1] for row in cur.fetchall()}

        # 如果表不存在，創建新表
        if not existing_columns:
            cur.execute("""
                CREATE TABLE files (
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
            log("[數據庫] 創建新的 files 表")
        else:
            log(f"[數據庫] 現有列: {existing_columns}")

            # 添加缺失的列
            columns_to_add = {
                'push_time': 'TEXT NULL',
                'completed_time': 'TEXT NULL',
                'file_hash': 'TEXT NULL'
            }

            for column, definition in columns_to_add.items():
                if column not in existing_columns:
                    try:
                        cur.execute(
                            f"ALTER TABLE files ADD COLUMN {column} {definition}")
                        log(f"[數據庫升級] 添加列: {column}")
                    except sqlite3.OperationalError as e:
                        log(f"[警告] 添加列 {column} 失敗: {e}")

        conn.commit()

    except Exception as e:
        log(f"[數據庫錯誤] files 表處理失敗: {e}")
        raise

    # 創建動態批次歷史表
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

    # 創建索引
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_batch_history_status ON batch_history(status)")

        # 檢查 file_hash 列是否存在後再創建索引
        cur.execute("PRAGMA table_info(files)")
        columns = {row[1] for row in cur.fetchall()}
        if 'file_hash' in columns:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_hash ON files(file_hash)")
    except Exception as e:
        log(f"[警告] 創建索引失敗: {e}")

    conn.commit()
    log("[數據庫] 初始化完成")
    return conn


def calculate_file_hash(file_path, chunk_size=8192):
    """計算文件的MD5哈希值"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        log(f"計算哈希失敗 {file_path}: {e}")
        return None


def scan_and_add_files(conn, source_root):
    """極簡版文件掃描 - 純單行進度顯示"""

    stats = {'new_files': 0, 'updated_files': 0,
        'duplicate_files': 0, 'error_files': 0}
    cur = conn.cursor()

    # 收集文件
    all_files = []
    for dirpath, _, filenames in os.walk(source_root):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            if os.path.exists(full_path):
                stat_info = os.stat(full_path)
                all_files.append({
                    'full_path': full_path,
                    'filename': filename,
                    'size': stat_info.st_size,
                    'mtime': int(stat_info.st_mtime),
                    'file_hash': calculate_file_hash(full_path) if stat_info.st_size < params.get('small_file_threshold', 50*1024*1024) else None
                })

    if not all_files:
        console.print("[yellow]沒有找到任何文件[/yellow]")
        return stats

    # 處理文件 - 只顯示一行進度
    if RICH_AVAILABLE:
        with Progress(
            TextColumn("掃描: {task.fields[current_file]}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "({task.completed}/{task.total})",
            TimeElapsedColumn(),
            console=console,
            transient=False,
        ) as progress:

            task = progress.add_task("掃描文件", total=len(
                all_files), current_file="準備中...")

            for i, file_info in enumerate(all_files):
                filename = file_info['filename']

                # 更新當前文件名
                progress.update(task, current_file=filename[:50])

                # 處理文件邏輯（簡化版）
                try:
                    cur.execute(
                        "SELECT id, status FROM files WHERE path=?", (file_info['full_path'],))
                    existing = cur.fetchone()

                    if not existing:
                        # 新文件
                        cur.execute("""
                            INSERT INTO files (path, size, mtime, file_hash, status)
                            VALUES (?, ?, ?, ?, 'pending')
                        """, (file_info['full_path'], file_info['size'], file_info['mtime'], file_info['file_hash']))
                        stats['new_files'] += 1
                    elif existing[1] != 'completed':
                        # 更新現有文件為待處理
                        cur.execute(
                            "UPDATE files SET status='pending' WHERE path=?", (file_info['full_path'],))
                        stats['updated_files'] += 1
                    else:
                        stats['duplicate_files'] += 1

                    progress.update(task, advance=1)

                except Exception as e:
                    stats['error_files'] += 1
                    progress.update(task, advance=1)

            # 最終顯示
            progress.update(task, current_file=f"完成! {stats['new_files']} 新增")

    conn.commit()

    # 只顯示一行總結
    console.print(
        f"[bold green]📁 掃描完成: {stats['new_files']} 新增, {stats['updated_files']} 更新, {stats['duplicate_files']} 重複[/bold green]")

    return stats


def query_pending_files_count():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # 詳細查詢各種狀態的文件數
        cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
        status_counts = dict(cur.fetchall())

        pending_count = status_counts.get('pending', 0)

       # print(f"[調試] 文件狀態統計: {status_counts}")
       # print(f"[調試] 待處理文件數: {pending_count}")

        conn.close()
        return pending_count
    except Exception as e:
        log(f"查詢待處理文件數目失敗: {e}")
        return 0


# /////////////////////////////////////////////////////////////////////////////
# 動態批次管理器
class DynamicBatchManager:
    """動態批次管理器"""

    def __init__(self, conn):
        self.conn = conn
        self.current_batch_id = None
        self.batch_start_time = None
        self.batch_files = []
        self.batch_total_size = 0
        self.successful_pushes = 0

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
                # 移除單個文件的成功日誌，由 progress bar 統一管理
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
            # 錯誤信息由調用方的 rich console 處理
        except Exception as e:
            console.print(f"[red]數據庫錯誤: {e}[/red]")

    def complete_batch(self, batch_status='completed'):
        """完成當前批次 - 修復版 (guard against zero-file batch)"""
        if not self.current_batch_id:
            return

        total_files = len(self.batch_files)
        if total_files == 0:
            log(f"[批次完成] {self.current_batch_id}: 無文件，批次略過 (成功推送: {self.successful_pushes})")
            # Optionally reset successful_pushes to zero for safety
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


class TrueDynamicBatchManager:
    """真正的动态批次管理器 - 无数据库依赖"""

    def __init__(self, conn):
        self.conn = conn
        self.current_virtual_batch = []
        self.batch_stats = {
            'total_size': 0,
            'file_count': 0,
            'successful_pushes': 0
        }

    def get_next_virtual_batch(self, max_files=None, max_size_gb=None):
        """纯内存操作 - 从数据库读取但不修改状态 (always use latest params)"""
        # Always use the latest values from params if not explicitly provided
        max_files = params.get('batch_size', 1000) if max_files is None else max_files
        max_size_gb = params.get('batch_size_gb', 90) if max_size_gb is None else max_size_gb

        cur = self.conn.cursor()

        # 只读查询，不修改数据库
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

        # 🟢 纯内存中的批次组合
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
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

        # 🟢 只更新内存中的统计
        self.current_virtual_batch = selected_files
        self.batch_stats = {
            'total_size': current_size,
            'file_count': len(selected_files),
            'successful_pushes': 0
        }
        log(f"[虚拟批次] 内存中组建 {len(selected_files)} 个文件，{current_size/1024/1024:.1f}MB")
        return selected_files

    def mark_file_pushed_virtual(self, file_path):
        """虚拟标记 - 只在内存中记录，不修改数据库"""
        self.batch_stats['successful_pushes'] += 1
        # print(f"[虚拟推送] {os.path.basename(file_path)} (内存计数: {self.batch_stats['successful_pushes']})")
        return True

    def commit_batch_to_database(self):
        """批次完成后一次性提交到数据库"""
        if not self.current_virtual_batch:
            return 0

        cur = self.conn.cursor()
        push_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 🟢 一次性批量更新所有成功的文件
        successful_files = self.current_virtual_batch[:self.batch_stats['successful_pushes']]

        for file_info in successful_files:
            cur.execute("""
                UPDATE files
                SET status='completed', push_time=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
            """, (push_time, file_info['id']))

        self.conn.commit()
        success_count = len(successful_files)
        log(f"[数据库提交] 一次性更新 {success_count} 个文件状态")
        # 清理内存
        self.current_virtual_batch = []
        self.batch_stats = {'total_size': 0, 'file_count': 0, 'successful_pushes': 0}
        return success_count
        try:
            # 更新開始按鈕
            start_config = config.get('start_button', {})
            if 'text' in start_config and 'button_start' in globals():
                button_start.label.set_text(start_config['text'])
            if 'color' in start_config and 'button_start' in globals():
                button_start.color = start_config['color']
                button_start.hovercolor = start_config['color']

            # 更新掃描按鈕
            scan_config = config.get('scan_button', {})
            if 'text' in scan_config and 'button_scan' in globals():
                button_scan.label.set_text(scan_config['text'])
            if 'color' in scan_config and 'button_scan' in globals():
                button_scan.color = scan_config['color']
                button_scan.hovercolor = scan_config['color']

            # 更新停止按鈕
            stop_config = config.get('stop_button', {})
            if 'text' in stop_config and 'button_stop' in globals():
                button_stop.label.set_text(stop_config['text'])
            if 'color' in stop_config and 'button_stop' in globals():
                button_stop.color = stop_config['color']
                button_stop.hovercolor = stop_config['color']

            # 更新刷新按鈕
            refresh_config = config.get('refresh_button', {})
            if 'color' in refresh_config and 'button_refresh' in globals():
                button_refresh.color = refresh_config['color']
                button_refresh.hovercolor = refresh_config['color']

            # 重繪界面
            if 'fig' in globals():
                fig.canvas.draw_idle()

        except Exception as e:
            log(f"[UI錯誤] 更新按鈕狀態失敗: {e}")


def adb_move_remote_folder(src, dst):
    log(f"[ADB] 移動: {src} -> {dst}")
    run_adb_command(["shell", "mv", src, dst])


def adb_remove_remote_folder(folder):
    log(f"[ADB] 刪除遠端目錄: {folder}")
    run_adb_command(["shell", "rm", "-rf", folder])


def adb_trigger_media_scan(path):
    uri_path = f"file://{path}"
    log(f"[ADB] 觸發媒體掃描: {uri_path}")
    run_adb_command(["shell", "am", "broadcast", "-a",
                    "android.intent.action.MEDIA_SCANNER_SCAN_FILE", "-d", uri_path])


def move_remote_folder_safe(src_folder, dst_folder):
    """安全地移動遠端資料夾"""
    try:
        # 確保目標目錄的父目錄存在
        dst_parent = os.path.dirname(dst_folder)
        if dst_parent:
            run_adb_command(["shell", "mkdir", "-p", dst_parent])

        # 移動資料夾
        adb_move_remote_folder(src_folder, dst_folder)

        # 觸發媒體掃描
        adb_trigger_media_scan(dst_folder)

        log(f"[搬移成功] {src_folder} -> {dst_folder}")
        return True
    except Exception as e:
        log(f"[搬移失敗] {src_folder} -> {dst_folder}: {e}")
        return False


def cleanup_camera_folder(camera_folder):
    """清理Camera目錄中的批次資料夾"""
    try:
        log(f"[清理] 刪除Camera資料夾: {camera_folder}")
        adb_remove_remote_folder(camera_folder)
        # 觸發媒體掃描
        adb_trigger_media_scan(CAMERA_ROOT)
        log(f"[清理成功] {camera_folder}")
    except Exception as e:
        log(f"[清理失敗] {camera_folder}: {e}")


# /////////////////////////////////////////////////////////////////////////////
# Google Photos CPU 監控
def get_pid():
    try:
        pid_output = run_adb_command(
            ['shell', 'pidof', 'com.google.android.apps.photos'])
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
        log(f"取得 CPU 使用率錯誤: {e}")
        return 0.0


def get_current_batch_size():
    """Get the file count of the currently processing batch"""
    global batch_in_process

    if not batch_in_process:
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Get the currently processing batch
        cur.execute("""
            SELECT virtual_batch_id, file_count 
            FROM batch_history 
            WHERE status='processing' 
            ORDER BY id DESC 
            LIMIT 1
        """)

        result = cur.fetchone()
        conn.close()

        if result:
            batch_id, file_count = result
            return file_count
        else:
            return 0

    except Exception as e:
        log(f"[錯誤] 無法獲取當前批次大小: {e}")
        return 0


# /////////////////////////////////////////////////////////////////////////////
# 批次推送及管理流程
def clean_camera_batch():
    try:
        output = run_adb_command(['shell', 'ls', CAMERA_ROOT])
        batch_dirs = [line.strip() for line in output.splitlines()
                                 if line.startswith(BATCH_PREFIX)]
        for batch_dir in batch_dirs:
            full_path = f"{CAMERA_ROOT}/{batch_dir}"
            log(f"[清理] 刪除資料夾：{full_path}")
            adb_remove_remote_folder(full_path)
        adb_trigger_media_scan(CAMERA_ROOT)
        log("[清理] 完成")
    except Exception as e:
        log(f"[清理] 失敗: {e}")


def push_files_individually(batch_manager, file_batch, remote_folder):
    """逐個推送文件 - 使用 rich.progress 清潔日誌版本"""
    try:
        adb_create_remote_folder(remote_folder)
    except Exception as e:
        console.print(f"[red][推送] 建立遠端目錄失敗: {e}[/red]")
        return 0

    success_count = 0
    total_files = len(file_batch)

    if total_files == 0:
        return 0

    # 使用 rich.progress 顯示推送進度
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "({task.completed}/{task.total})",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,  # 保持進度條可見
    ) as progress:

        # 創建推送任務
        task = progress.add_task(
            f"[cyan]推送批次文件 ({total_files} 個)",
            total=total_files
        )

        for i, file_info in enumerate(file_batch):
            if not batch_processing:
                log("[UI] 停止請求已收到，終止推送循環")
                break
            file_path = file_info['path']
            filename = os.path.basename(file_path)

            # 更新任務描述顯示當前文件
            progress.update(
                task,
                description=f"[cyan]推送: {filename[:40]}{'...' if len(filename) > 40 else ''}"
            )

            try:
                # 推送單個文件 (靜默版本，不打印ADB日誌)
                adb_push_file_silent(file_path, remote_folder)

                # 立即標記為已推送
                if batch_manager.mark_file_pushed(file_path):
                    success_count += 1

                # 更新進度
                progress.update(task, advance=1)

                # 每推送5個文件更新一次UI（避免過於頻繁）
                if (i + 1) % 5 == 0 or (i + 1) == total_files:
                    update_pending_count_text()

            except Exception as e:
                # 使用 rich 顯示錯誤，但不破壞進度條
                console.print(f"[red]✗ {filename}: {str(e)[:50]}[/red]")
                batch_manager.mark_file_failed(file_path, str(e))
                # 仍然推進進度條
                progress.update(task, advance=1)

        # 完成後顯示摘要
        progress.update(
            task,
            description=f"[green]✓ 批次推送完成: {success_count}/{total_files} 成功[/green]"
        )

    # 推送完成後最終更新
    update_pending_count_text()

    success_rate = (success_count / total_files) * 100

    # 使用 rich 顯示最終結果
    if success_rate >= 90:
        console.print(
            f"[green]✓ 推送完成: {success_count}/{total_files} ({success_rate:.1f}%)[/green]")
    else:
        console.print(
            f"[yellow]⚠ 推送完成: {success_count}/{total_files} ({success_rate:.1f}%) - 成功率偏低[/yellow]")

    return success_count


def adb_push_file_silent(local_path, remote_folder):
    """靜默版本的文件推送，不打印詳細日誌"""
    filename = os.path.basename(local_path)
    remote_path = f"{remote_folder}/{filename}"

    import subprocess
    try:
        # 執行推送，但不打印過程信息
        process = subprocess.Popen(["adb", "push", local_path, remote_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        while True:
            if not batch_processing:
                log("[UI] 停止請求已收到，終止ADB推送進程")
                process.terminate()
                process.wait()
                raise Exception("推送被用戶中斷")
            retcode = process.poll()
            if retcode is not None:
                break
            time.sleep(0.2)
        if process.returncode != 0:
            stderr = process.stderr.read()
            raise Exception(f"ADB推送失敗: {stderr}")
    except Exception as e:
        # 檢查文件是否實際存在（有時推送成功但返回錯誤）
        try:
            output = run_adb_command(["shell", "ls", remote_path])
            if filename in output:
                # 文件存在，視為成功
                return
            else:
                raise e
        except Exception:
            raise e


def mark_pushed_files_completed(conn, file_batch):
    """將已推送的文件標記為完成"""
    cur = conn.cursor()
    completed_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 檢查 completed_time 列是否存在
    try:
        cur.execute("PRAGMA table_info(files)")
        columns = {row[1] for row in cur.fetchall()}

        completed_count = 0
        for file_info in file_batch:
            file_path = file_info['path']

            if 'completed_time' in columns:
                cur.execute("""
                    UPDATE files SET status='completed', completed_time=?, updated_at=CURRENT_TIMESTAMP 
                    WHERE path=? AND status='pushed'
                """, (completed_time, file_path))
            else:
                cur.execute("""
                    UPDATE files SET status='completed', updated_at=CURRENT_TIMESTAMP 
                    WHERE path=? AND status='pushed'
                """, (file_path,))

            if cur.rowcount > 0:
                completed_count += 1

        conn.commit()
        print(f"[狀態更新] {completed_count} 個已推送文件標記為完成")
        return completed_count

    except Exception as e:
        print(f"[狀態更新錯誤] {e}")
        return 0


def check_all_files_processed(conn):
    """檢查是否所有文件都已處理"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE status='pending'")
    pending_count = cur.fetchone()[0]
    return pending_count == 0


def wait_for_backup_complete():
    """Enhanced backup completion detection with dynamic timing"""
    stable_seconds = 0
    required_stable = params.get('backup_stable_time', 60)

    # Dynamic adjustment based on batch size
    if params.get('quick_backup_detection', True):
        current_batch_size = get_current_batch_size()

        if current_batch_size < 100:
            required_stable = min(30, required_stable)
            print(
                f"[優化] 小批次 ({current_batch_size} 文件)，縮短等待時間至 {required_stable} 秒")
        elif current_batch_size < 500:
            required_stable = min(45, required_stable)
            print(
                f"[優化] 中等批次 ({current_batch_size} 文件)，調整等待時間至 {required_stable} 秒")
        else:
            print(
                f"[標準] 大批次 ({current_batch_size} 文件)，使用標準等待時間 {required_stable} 秒")

    if RICH_AVAILABLE:
        with Progress(
            TextColumn("[bold blue]備份等待: {task.fields[status]}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "({task.completed}/{task.total})",
            TimeElapsedColumn(),
            console=console,
            transient=False,
        ) as progress:
            task = progress.add_task(
                "備份等待", total=required_stable, status=f"0/{required_stable} 秒 (CPU: 0%)"
            )
            while stable_seconds < required_stable and batch_processing:
                cpu = get_cpu_usage()
                if cpu < params['cpu_threshold']:
                    stable_seconds += params['monitor_interval']
                    progress.update(
                        task,
                        completed=stable_seconds,
                        status=f"{stable_seconds}/{required_stable} 秒 (CPU: {cpu:.1f}%)"
                    )
                else:
                    if stable_seconds > 0:
                        progress.update(
                            task,
                            status=f"CPU活躍 ({cpu:.1f}%)，重置計時器"
                        )
                    stable_seconds = 0
                time.sleep(params['monitor_interval'])
            if stable_seconds >= required_stable:
                progress.update(task, completed=required_stable, status=f"完成! CPU已穩定 {required_stable} 秒")
                return True
            else:
                progress.update(task, status="等待被中斷")
                return False
    else:
        print(f"[備份等待] 等待 {required_stable} 秒的穩定期...")
        while stable_seconds < required_stable and batch_processing:
            cpu = get_cpu_usage()
            if cpu < params['cpu_threshold']:
                stable_seconds += params['monitor_interval']
                if stable_seconds % 10 == 0:
                    print(f"[備份等待] 已穩定 {stable_seconds}/{required_stable} 秒 (CPU: {cpu:.1f}%)")
            else:
                if stable_seconds > 0:
                    print(f"[備份等待] CPU活躍 ({cpu:.1f}%)，重置計時器")
                stable_seconds = 0
            time.sleep(params['monitor_interval'])
        if stable_seconds >= required_stable:
            print(f"[備份完成] CPU已穩定 {required_stable} 秒，認為備份完成")
            return True
        else:
            print(f"[備份中斷] 等待被中斷")
            return False


# /////////////////////////////////////////////////////////////////////////////
# CPU 監控線程
def cpu_monitor_thread():
    global cpu_monitoring, status_text, cpu_active_flag
    log("[CPU監控] 線程啟動")

    while cpu_monitoring:
        try:
            cpu = get_cpu_usage()
            with cpu_status_lock:
                cpu_data.append(cpu)
                avg_cpu = sum(cpu_data) / len(cpu_data) if cpu_data else 0.0
                cpu_active_flag = avg_cpu > params['cpu_threshold']

            # 更新狀態字串與 UI 顯示顏色
            if cpu_active_flag:
                status_text = f"Active (Avg CPU: {avg_cpu:.1f}%)"
            else:
                status_text = f"Idle (Avg CPU: {avg_cpu:.1f}%)"

            # 更新 UI 狀態燈色
            if status_text.startswith("Active"):
                status_circle.set_facecolor('green')
            else:
                status_circle.set_facecolor('red')

            update_status_text()
            time.sleep(params['monitor_interval'])

        except Exception as e:
            print(f"[CPU監控] 錯誤: {e}")
            time.sleep(5)

    log("[CPU監控] 線程結束")


def dynamic_batch_process_thread():
    """動態批次處理線程 - 優化日誌版本"""
    global batch_in_process, batch_processing
    console.print("[bold green]🚀 動態批次處理啟動[/bold green]")

    try:
        conn = init_db()
        batch_manager = DynamicBatchManager(conn)
        total_processed_batches = 0
        max_rounds = params.get('max_rounds', 9999)

        while batch_processing:
            if total_processed_batches >= max_rounds:
                console.print(f"[bold yellow]⏹ 已達最大批次輪數 {max_rounds}，自動停止。[/bold yellow]")
                break
            try:
                with cpu_status_lock:
                    active = cpu_active_flag

                if not active:
                    with batch_processing_lock:
                        if not batch_in_process:
                            file_batch = batch_manager.get_next_file_batch()

                            if file_batch:
                                batch_in_process = True
                                batch_id = batch_manager.start_new_batch()

                                try:
                                    console.print(
                                        f"[bold cyan]📦 處理批次 {total_processed_batches + 1}: {len(file_batch)} 個文件[/bold cyan]")

                                    # 清理和推送（使用 rich progress）
                                    clean_camera_batch()

                                    remote_temp_folder = f"{REMOTE_ROOT}/temp_{int(time.time())}"
                                    success_count = push_files_individually(
                                        batch_manager, file_batch, remote_temp_folder
                                    )

                                    if success_count > 0:
                                        camera_folder = f"{CAMERA_ROOT}/batch_{int(time.time())}"
                                        if move_remote_folder_safe(remote_temp_folder, camera_folder):
                                            mark_pushed_files_completed(
                                                conn, file_batch)

                                            console.print(
                                                "[yellow]⏳ 等待 Google Photos 備份完成...[/yellow]")
                                            wait_for_backup_complete()

                                            cleanup_camera_folder(
                                                camera_folder)
                                            batch_manager.complete_batch(
                                                'completed')
                                            total_processed_batches += 1

                                            console.print(
                                                f"[green]✓ 批次 {total_processed_batches} 完成[/green]")

                                            # if enhanced_batch_completion_check(conn, batch_manager, total_processed_batches):
                                            #    break
                                        else:
                                            console.print(
                                                "[red]✗ 批次搬移失敗[/red]")
                                            batch_manager.complete_batch(
                                                'failed')
                                    else:
                                        console.print("[red]✗ 批次推送失敗[/red]")
                                        batch_manager.complete_batch('failed')

                                except Exception as e:
                                    console.print(f"[red]✗ 批次處理異常: {e}[/red]")
                                    batch_manager.complete_batch('failed')
                                finally:
                                    batch_in_process = False
                            else:
                                if check_all_files_processed(conn):
                                    console.print(
                                        f"[bold green]🎉 所有文件處理完成！總共處理 {total_processed_batches} 個批次[/bold green]")
                                    show_completion_notification(
                                        total_processed_batches)
                                    break
                                else:
                                    time.sleep(1)
                        else:
                            time.sleep(1)
                else:
                    time.sleep(1)

            except Exception as e:
                console.print(f"[red]線程錯誤: {e}[/red]")
                time.sleep(5)

        conn.close()
        console.print("[bold blue]📴 動態批次處理結束[/bold blue]")

    finally:
        batch_processing = False
        ui_state.set_state('idle')
        update_pending_count_text()


def optimized_batch_process_thread():
    """优化的批次处理 - 最小化数据库操作"""
    global batch_in_process, batch_processing
    console.print("[bold green]🚀 优化批次处理启动[/bold green]")

    try:
        conn = init_db()
        batch_manager = TrueDynamicBatchManager(conn)
        total_processed_batches = 0

        while batch_processing:
            try:
                with cpu_status_lock:
                    active = cpu_active_flag

                if not active:
                    with batch_processing_lock:
                        if not batch_in_process:
                            # 🟢 纯虚拟批次选择，不修改数据库
                            virtual_batch = batch_manager.get_next_virtual_batch()

                            if virtual_batch:
                                batch_in_process = True

                                try:
                                    console.print(
                                        f"[bold cyan]📦 处理虚拟批次: {len(virtual_batch)} 个文件[/bold cyan]")

                                    # 推送文件（虚拟标记）
                                    remote_temp_folder = f"{REMOTE_ROOT}/temp_{int(time.time())}"
                                    success_count = push_files_with_virtual_tracking(
                                        batch_manager, virtual_batch, remote_temp_folder
                                    )

                                    if success_count > 0:
                                        # 移动到Camera目录
                                        camera_folder = f"{CAMERA_ROOT}/batch_{int(time.time())}"
                                        if move_remote_folder_safe(remote_temp_folder, camera_folder):

                                            # 等待备份完成
                                            console.print(
                                                "[yellow]⏳ 等待 Google Photos 备份完成...[/yellow]")
                                            wait_for_backup_complete()

                                            # 🟢 只在最后一步提交到数据库
                                            committed_count = batch_manager.commit_batch_to_database()

                                            cleanup_camera_folder(
                                                camera_folder)
                                            total_processed_batches += 1

                                            console.print(
                                                f"[green]✓ 虚拟批次 {total_processed_batches} 完成，已提交 {committed_count} 个文件[/green]")
                                        else:
                                            console.print(
                                                "[red]✗ 批次搬移失败[/red]")
                                    else:
                                        console.print("[red]✗ 批次推送失败[/red]")

                                except Exception as e:
                                    console.print(f"[red]✗ 批次处理异常: {e}[/red]")
                                finally:
                                    batch_in_process = False
                            else:
                                # 检查完成
                                if check_all_files_processed_with_retry(conn):
                                    console.print(
                                        f"[bold green]🎉 所有文件处理完成！总共处理 {total_processed_batches} 个虚拟批次[/bold green]")
                                    show_completion_notification(
                                        total_processed_batches)
                                    break
                                else:
                                    time.sleep(1)
                        else:
                            time.sleep(1)
                else:
                    time.sleep(1)

            except Exception as e:
                console.print(f"[red]线程错误: {e}[/red]")
                time.sleep(5)

        conn.close()
        console.print("[bold blue]📴 优化批次处理结束[/bold blue]")

    finally:
        batch_processing = False
        ui_state.set_state('idle')
        update_pending_count_text()


def push_files_with_virtual_tracking(batch_manager, file_batch, remote_folder):
    """推送文件 - 使用虚拟追踪"""
    try:
        adb_create_remote_folder(remote_folder)
    except Exception as e:
        log(f"推送: 建立远端目录失败: {e}")
        return 0

    success_count = 0

    if RICH_AVAILABLE:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "({task.completed}/{task.total})",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:

            task = progress.add_task(
                f"推送虚拟批次文件",
                total=len(file_batch)
            )


            for i, file_info in enumerate(file_batch):
                if not batch_processing:
                    log("[UI] 停止請求已收到，終止推送循環")
                    break
                file_path = file_info['path']
                filename = os.path.basename(file_path)

                progress.update(
                    task,
                    description=f"推送: {filename[:40]}{'...' if len(filename) > 40 else ''}"
                )

                try:
                    # 实际推送文件
                    adb_push_file_silent(file_path, remote_folder)

                    # 🟢 只在内存中标记，不修改数据库
                    if batch_manager.mark_file_pushed_virtual(file_path):
                        success_count += 1

                    progress.update(task, advance=1)

                except Exception as e:
                    log(f"推送失败: {filename}: {str(e)[:50]}")
                    progress.update(task, advance=1)

            progress.update(
                task,
                description=f"[green]✓ 虚拟批次推送完成: {success_count}/{len(file_batch)} 成功[/green]"
            )

    return success_count


# /////////////////////////////////////////////////////////////////////////////
# 完成通知
def get_completion_statistics_dynamic(conn):
    """獲取動態批次的完成統計信息"""
    cur = conn.cursor()

    try:
        # 文件統計
        cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
        file_stats = dict(cur.fetchall())

        # 動態批次統計
        cur.execute(
            "SELECT status, COUNT(*) FROM batch_history GROUP BY status")
        batch_stats = dict(cur.fetchall())

        # 處理時間統計 - 使用 batch_history 表
        cur.execute("""
            SELECT 
                MIN(start_time) as start_time,
                MAX(end_time) as end_time
            FROM batch_history 
            WHERE status='completed'
        """)
        time_stats = cur.fetchone()

        return {
            'file_stats': file_stats,
            'batch_stats': batch_stats,
            'time_stats': time_stats
        }

    except Exception as e:
        print(f"[統計錯誤] 無法獲取完成統計: {e}")
        # 返回基本統計
        return {
            'file_stats': {'pending': 0, 'completed': 0},
            'batch_stats': {'completed': 0},
            'time_stats': (None, None)
        }


def show_completion_notification(processed_batches):
    """顯示處理完成的通知窗口 - 動態批次版"""
    try:
        # 獲取統計信息
        conn = sqlite3.connect(DB_PATH)
        stats = get_completion_statistics_dynamic(conn)
        conn.close()

        # 創建通知窗口
        root = tk.Tk()
        root.title("傳輸完成")
        root.geometry("500x400")
        root.resizable(False, False)

        # 設置窗口居中
        root.update_idletasks()
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x = (screen_width // 2) - (500 // 2)
        y = (screen_height // 2) - (400 // 2)
        root.geometry(f"500x400+{x}+{y}")

        # 標題
        title_label = tk.Label(root, text="🎉 動態批次傳輸完成！",
                               font=("Microsoft JhengHei", 16, "bold"),
                               fg="green")
        title_label.pack(pady=20)

        # 統計信息框架
        stats_frame = tk.Frame(root)
        stats_frame.pack(pady=10, padx=20, fill="both", expand=True)

        # 動態批次統計
        batch_frame = tk.LabelFrame(stats_frame, text="動態批次統計",
                                    font=("Microsoft JhengHei", 12, "bold"))
        batch_frame.pack(fill="x", pady=5)

        batch_stats = stats['batch_stats']
        tk.Label(batch_frame,
                 text=f"✅ 完成批次: {batch_stats.get('completed', 0)}",
                 font=("Microsoft JhengHei", 10)).pack(anchor="w", padx=10)

        if batch_stats.get('failed', 0) > 0:
            tk.Label(batch_frame,
                     text=f"❌ 失敗批次: {batch_stats.get('failed', 0)}",
                     font=("Microsoft JhengHei", 10), fg="red").pack(anchor="w", padx=10)

        # 文件統計
        file_frame = tk.LabelFrame(stats_frame, text="文件統計",
                                   font=("Microsoft JhengHei", 12, "bold"))
        file_frame.pack(fill="x", pady=5)

        file_stats = stats['file_stats']
        total_files = sum(file_stats.values())
        completed_files = file_stats.get('completed', 0)

        tk.Label(file_frame,
                 text=f"📁 總文件數: {total_files}",
                 font=("Microsoft JhengHei", 10)).pack(anchor="w", padx=10)
        tk.Label(file_frame,
                 text=f"✅ 成功傳輸: {completed_files}",
                 font=("Microsoft JhengHei", 10)).pack(anchor="w", padx=10)

        if file_stats.get('failed', 0) > 0:
            tk.Label(file_frame,
                     text=f"❌ 傳輸失敗: {file_stats.get('failed', 0)}",
                     font=("Microsoft JhengHei", 10), fg="red").pack(anchor="w", padx=10)

        # 按鈕區域
        button_frame = tk.Frame(root)
        button_frame.pack(pady=20)

        # 確定按鈕
        tk.Button(button_frame, text="確定",
                  command=root.destroy,
                  font=("Microsoft JhengHei", 10),
                  bg="lightgreen").pack(padx=10)

        # 設置窗口屬性
        root.attributes('-topmost', True)
        root.focus_force()

        # 播放系統提示音
        try:
            import winsound
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
        except:
            pass

        root.mainloop()

    except Exception as e:
        print(f"[錯誤] 顯示完成通知失敗: {e}")
        # 後備通知方式
        try:
            msgbox.showinfo("傳輸完成", f"動態批次傳輸已完成！\n處理了 {processed_batches} 個批次")
        except:
            print(f"[通知] 動態批次傳輸已完成！處理了 {processed_batches} 個批次")


# /////////////////////////////////////////////////////////////////////////////
# 自動啟動功能
def auto_start_cpu_monitoring():
    """程序啟動時自動開始CPU監控"""
    global cpu_monitoring
    if not cpu_monitoring:
        cpu_monitoring = True
        log("[自動啟動] CPU監控已開始")
        threading.Thread(target=cpu_monitor_thread, daemon=True).start()
    else:
        print("[提示] CPU監控已在運行中")


# /////////////////////////////////////////////////////////////////////////////
# UI 回調函式
def select_folder_with_dynamic_batch():
    """動態批次版的資料夾選擇"""
    root = tk.Tk()
    root.withdraw()
    folder = filedialog.askdirectory()
    if folder:
        print(f"[UI] 選擇資料夾: {folder}")
        conn = init_db()

        # 使用簡化的掃描（不創建批次）
        stats = scan_and_add_files(conn, folder)

        update_pending_count_text()
        conn.close()

        print("[系統] 文件掃描完成，準備動態批次處理")
    else:
        print("[UI] 未選擇資料夾")


def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")


def update_status_text():
    status_txt_obj.set_text(f"狀態: {status_text}")
    ax_status.figure.canvas.draw_idle()


def update_pending_count_text():
    try:
        count = query_pending_files_count()
#        print(f"待處理文件數: {count}")
        pending_count_text.set_text(f"待處理文件數: {count:,}")
        ax_pending_count.figure.canvas.draw_idle()
    except Exception as e:
        print(f"[刷新失敗] 無法更新待處理文件數: {e}")


def update(frame):
    ax_cpu.clear()
    ax_cpu.set_title('Google Photos CPU 使用率 (%)', fontsize=14)
    ax_cpu.set_xlabel('時間 (秒)', fontsize=10)
    ax_cpu.set_ylabel('CPU %', fontsize=10)

    if cpu_data:
        # 動態Y軸縮放
        max_cpu = max(cpu_data)
        if max_cpu <= 100:
            y_max = 100
        else:
            y_max = max(120, int(max_cpu * 1.1))

        ax_cpu.set_ylim(0, y_max)
        ax_cpu.plot(list(range(len(cpu_data))), list(
            cpu_data), color='red', linewidth=1.5)

        # 添加閾值線
        threshold = params['cpu_threshold']
        if threshold <= y_max:
            ax_cpu.axhline(y=threshold, color='orange', linestyle='--', alpha=0.7,
                           label=f'Threshold ({threshold}%)')
            ax_cpu.legend(loc='upper right')
    else:
        ax_cpu.set_ylim(0, 100)

    ax_cpu.grid(True)
    update_status_text()

# 動態批次版回調函數

def on_start_dynamic(event):
    """動態批次版開始傳輸"""
    can_start, message = ui_state.can_perform_action('start_transfer', 3.0)
    if not can_start:
        print(f"[防護] {message}")
        return

    global batch_processing

    if batch_processing:
        print("[提示] 傳輸已在進行中")
        return

    print("[DEBUG] 開始動態批次傳輸")

    # 確保CPU監控已啟動
    if not cpu_monitoring:
        print("[警告] CPU監控未啟動，正在自動啟動...")
        auto_start_cpu_monitoring()
        time.sleep(1)

    # 檢查前置條件
    pending_count = query_pending_files_count()
    if pending_count == 0:
        print("[提示] 沒有待處理文件，請先掃描資料夾")
        return

    # 檢查ADB連接
    try:
        run_adb_command(['devices'])
        print("[檢查] ADB連接正常")
    except Exception as e:
        print(f"[錯誤] ADB連接失敗: {e}")
        return

    # 設置處理狀態
    ui_state.set_state('processing')

    # 啟動動態批次處理
    batch_processing = True
    log("[UI] 開始動態批次文件傳輸")
    threading.Thread(target=dynamic_batch_process_thread, daemon=True).start()
    #threading.Thread(target=optimized_batch_process_thread, daemon=True).start()

    print(f"[成功] 動態批次處理已啟動，待處理文件數: {pending_count}")


def on_scan_folder_final(event):
    """動態批次版掃描資料夾"""
    can_scan, message = ui_state.can_perform_action('scan_folder', 5.0)
    if not can_scan:
        print(f"[防護] {message}")
        return

    # 設置掃描狀態
    ui_state.set_state('scanning')

    def scan_with_state_reset():
        try:
            select_folder_with_dynamic_batch()
        finally:
            ui_state.set_state('idle')

    threading.Thread(target=scan_with_state_reset, daemon=True).start()


def on_stop_final(event):
    """停止傳輸"""
    global batch_processing

    if batch_processing:
        batch_processing = False
        ui_state.set_state('idle')
        log("[UI] 停止動態批次處理")
        print("[成功] 動態批次處理已停止")

        # 顯示停止通知
        try:
            msgbox.showinfo("傳輸停止", "文件傳輸已手動停止")
        except:
            print("[通知] 文件傳輸已手動停止")
    else:
        print("[提示] 批次處理未在運行")


def on_apply_params(event):
    global params
    try:
        batch_size_val = int(text_batch_size.text)
        batch_size_gb_val = float(text_batch_size_gb.text)
        cpu_threshold_val = float(text_cpu_threshold.text)
        monitor_interval_val = float(text_monitor_interval.text)
        max_rounds_val = int(text_max_rounds.text)
        params.update({
            'batch_size': batch_size_val,
            'batch_size_gb': batch_size_gb_val,
            'cpu_threshold': cpu_threshold_val,
            'monitor_interval': monitor_interval_val,
            'max_rounds': max_rounds_val,
        })
        log(f"[UI] 參數更新: batch_size={batch_size_val}, batch_size_gb={batch_size_gb_val}GB, cpu_threshold={cpu_threshold_val}, interval={monitor_interval_val}s, max_rounds={max_rounds_val}")
    except Exception as e:
        log(f"[UI] 參數更新錯誤: {e}")
    update_status_text()


def on_refresh_pending_count_final(event):
    """刷新計數"""
    can_refresh, message = ui_state.can_perform_action('refresh', 1.0)
    if not can_refresh:
        print(f"[防護] {message}")
        return

    update_pending_count_text()


# /////////////////////////////////////////////////////////////////////////////
# 建立 UI 主畫面
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(7, 6, figure=fig)

# CPU 折線圖（頂部佔3格高度）
ax_cpu = fig.add_subplot(gs[0:3, :])

# 狀態燈區（第4行第一列）
ax_status = fig.add_subplot(gs[3, 0])
ax_status.axis('off')
status_circle = patches.Circle((0.5, 0.5), 0.35, color='red')
ax_status.add_patch(status_circle)
status_txt_obj = ax_status.text(
    1.3, 0.5, f"Status: {status_text}", va='center', fontsize=14)

# 待處理文件數顯示（右對齊到屏幕右側）
ax_pending_count = fig.add_subplot(gs[3, 2:])
ax_pending_count.axis('off')
pending_count_text = ax_pending_count.text(
    0.95, 0.5, "待處理文件數: 0", fontsize=12, va='center', ha='right')

# 參數輸入區 - 兩行布局
ax_bs = plt.axes([0.15, 0.30, 0.10, 0.04])
text_batch_size = TextBox(
    ax_bs, 'Batch Size', initial=str(params['batch_size']))
text_batch_size.label.set_fontsize(9)
text_batch_size.text_disp.set_fontsize(9)

ax_bsgb = plt.axes([0.30, 0.30, 0.10, 0.04])
text_batch_size_gb = TextBox(
    ax_bsgb, 'Size (GB)', initial=str(params['batch_size_gb']))
text_batch_size_gb.label.set_fontsize(9)
text_batch_size_gb.text_disp.set_fontsize(9)


ax_cpu_th = plt.axes([0.15, 0.24, 0.10, 0.04])
text_cpu_threshold = TextBox(
    ax_cpu_th, 'CPU Threshold', initial=str(params['cpu_threshold']))
text_cpu_threshold.label.set_fontsize(9)
text_cpu_threshold.text_disp.set_fontsize(9)

# Move Max Rounds directly under CPU Threshold
ax_max_rounds = plt.axes([0.15, 0.19, 0.10, 0.04])
text_max_rounds = TextBox(
    ax_max_rounds, 'Max Rounds', initial=str(params['max_rounds']))
text_max_rounds.label.set_fontsize(9)
text_max_rounds.text_disp.set_fontsize(9)

ax_interval = plt.axes([0.30, 0.24, 0.10, 0.04])
text_monitor_interval = TextBox(
    ax_interval, 'Interval(s)', initial=str(params['monitor_interval']))
text_monitor_interval.label.set_fontsize(9)
text_monitor_interval.text_disp.set_fontsize(9)

# 參數套用按鈕
ax_apply = plt.axes([0.45, 0.28, 0.08, 0.06])
button_apply = Button(ax_apply, '套用')
button_apply.label.set_fontsize(10)
button_apply.on_clicked(on_apply_params)

# 開始與停止按鈕 - 動態批次版本
ax_start = plt.axes([0.58, 0.28, 0.1, 0.06])
button_start = Button(ax_start, '開始傳輸')
button_start.label.set_fontsize(12)
button_start.on_clicked(on_start_dynamic)

ax_stop = plt.axes([0.70, 0.28, 0.1, 0.06])
button_stop = Button(ax_stop, '停止傳輸')
button_stop.label.set_fontsize(12)
button_stop.on_clicked(on_stop_final)

# 掃描本地資料夾按鈕
ax_scan = plt.axes([0.58, 0.20, 0.22, 0.06])
button_scan = Button(ax_scan, '掃描本地資料夾')
button_scan.label.set_fontsize(12)
button_scan.on_clicked(on_scan_folder_final)  # 使用動態批次版本

# 刷新待處理文件數按鈕
ax_refresh = plt.axes([0.44, 0.18, 0.1, 0.06])
button_refresh = Button(ax_refresh, '刷新數字')
button_refresh.label.set_fontsize(10)
button_refresh.on_clicked(on_refresh_pending_count_final)

# 啟動畫面動畫刷新
ani = FuncAnimation(fig, update, interval=1000)

# /////////////////////////////////////////////////////////////////////////////
# 程式啟動初始化
if __name__ == "__main__":
    log("[系統啟動] 正在初始化...")

    # 修復現有數據庫結構
    log("[系統啟動] 檢查並修復數據庫...")
    fix_existing_database()

    # 初始化數據庫
    conn = init_db()
    update_pending_count_text()
    conn.close()

    # 初始化UI狀態管理
    ui_state.set_state('idle')

    # 自動啟動CPU監控
    log("[系統啟動] 正在啟動CPU監控...")
    auto_start_cpu_monitoring()
    log("[系統啟動] CPU監控已啟動")
    log("[系統提示] 點擊'開始傳輸'按鈕開始動態批次處理")
    log("[系統提示] UI狀態管理已啟用 - 防止重複操作")
    log("[系統說明] 動態批次管理 - 真正的斷點續傳功能")

    # plt.tight_layout()
    plt.subplots_adjust()
    plt.show()
