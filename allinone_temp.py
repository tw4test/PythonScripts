import os
import sqlite3
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
import hashlib

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
    print("[警告] rich 模塊未安裝，將使用基本日誌輸出")
    RICH_AVAILABLE = False

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
    'batch_size': 1000,
    'batch_size_gb': 90,
    'cpu_threshold': 50.0,
    'monitor_interval': 2.0,
    'backup_stable_time': 30,         #30 secs is ok
    'quick_backup_detection': False,  #Disabled smart detect
    'duplicate_handling': 'smart',
    'hash_small_files_only': True,
    'small_file_threshold': 50 * 1024 * 1024
}

# 控制旗標與狀態
cpu_active_flag = False
batch_processing_lock = threading.Lock()
batch_in_process = False
operation_lock = threading.Lock()


# /////////////////////////////////////////////////////////////////////////////
# 統一的日誌函數
def log(msg):
    """統一的時間戳日誌函數"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

def log_without_timestamp(msg):
    """無時間戳的日誌函數（用於已有時間戳的日誌）"""
    print(msg)

# 初始化 console 對象
if RICH_AVAILABLE:
    console = Console()
else:
    # 創建一個簡單的替代對象
    class SimpleConsole:
        def print(self, text, **kwargs):
            # 移除 rich 的標記語法並添加時間戳
            import re
            clean_text = re.sub(r'\[.*?\]', '', str(text))
            log(clean_text)
    
    console = SimpleConsole()


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
            log(f"狀態變更: {old_state} -> {new_state}")
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
            log(f"UI錯誤: 更新按鈕狀態失敗: {e}")


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
                log(f"修復: 已添加列: {column}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    log(f"跳過: 列 {column} 已存在")
                else:
                    log(f"錯誤: 添加列 {column} 失敗: {e}")
        
        conn.commit()
        conn.close()
        log("修復: 數據庫結構修復完成")
        
    except Exception as e:
        log(f"錯誤: 數據庫修復失敗: {e}")


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
            log("數據庫: 創建新的 files 表")
        else:
            log(f"數據庫: 現有列: {existing_columns}")
            
            # 添加缺失的列
            columns_to_add = {
                'push_time': 'TEXT NULL',
                'completed_time': 'TEXT NULL', 
                'file_hash': 'TEXT NULL'
            }
            
            for column, definition in columns_to_add.items():
                if column not in existing_columns:
                    try:
                        cur.execute(f"ALTER TABLE files ADD COLUMN {column} {definition}")
                        log(f"數據庫升級: 添加列: {column}")
                    except sqlite3.OperationalError as e:
                        log(f"警告: 添加列 {column} 失敗: {e}")
        
        conn.commit()
        
    except Exception as e:
        log(f"數據庫錯誤: files 表處理失敗: {e}")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_history_status ON batch_history(status)")
        
        # 檢查 file_hash 列是否存在後再創建索引
        cur.execute("PRAGMA table_info(files)")
        columns = {row[1] for row in cur.fetchall()}
        if 'file_hash' in columns:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_file_hash ON files(file_hash)")
    except Exception as e:
        log(f"警告: 創建索引失敗: {e}")
    
    conn.commit()
    log("數據庫: 初始化完成")
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
    """Rich 優化的文件掃描 - 單行進度顯示版本"""
    console.print(f"[bold blue]🔍 掃描文件: {source_root}...[/bold blue]")
    
    stats = {
        'new_files': 0,
        'updated_files': 0,
        'duplicate_files': 0,
        'error_files': 0
    }
    
    cur = conn.cursor()
    
    # 第一階段：收集所有文件信息
    all_files = []
    console.print("[dim]正在收集文件信息...[/dim]")
    
    for dirpath, _, filenames in os.walk(source_root):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            try:
                if os.path.exists(full_path):
                    stat_info = os.stat(full_path)
                    size = stat_info.st_size
                    mtime = int(stat_info.st_mtime)
                    
                    # 計算小文件的哈希
                    file_hash = None
                    if size < params.get('small_file_threshold', 50 * 1024 * 1024):
                        file_hash = calculate_file_hash(full_path)
                    
                    all_files.append({
                        'full_path': full_path,
                        'filename': filename,
                        'size': size,
                        'mtime': mtime,
                        'file_hash': file_hash
                    })
            except Exception as e:
                log(f"處理文件錯誤 {full_path}: {e}")
                stats['error_files'] += 1
    
    if not all_files:
        console.print("[yellow]沒有找到任何文件[/yellow]")
        return stats
    
    # 第二階段：使用 Rich 進度條處理文件 - 單行顯示
    console.print(f"[bold green]📁 找到 {len(all_files)} 個文件，開始處理...[/bold green]")
    
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
                "掃描文件", 
                total=len(all_files)
            )
            
            # 用於收集詳細結果的列表
            detailed_results = []
            
            for file_info in all_files:
                full_path = file_info['full_path']
                filename = file_info['filename']
                size = file_info['size']
                mtime = file_info['mtime']
                file_hash = file_info['file_hash']
                
                # 只更新進度條的描述，不打印單獨的文件信息
                progress.update(
                    task, 
                    description=f"掃描: {filename[:40]}{'...' if len(filename) > 40 else ''}"
                )
                
                try:
                    # 檢查文件是否已存在
                    cur.execute("SELECT id, size, mtime, status FROM files WHERE path=?", (full_path,))
                    existing = cur.fetchone()
                    
                    if existing:
                        existing_id, existing_size, existing_mtime, existing_status = existing
                        
                        # 文件已更新？
                        if mtime > existing_mtime or size != existing_size:
                            cur.execute("""
                                UPDATE files SET size=?, mtime=?, file_hash=?, 
                                status='pending', updated_at=CURRENT_TIMESTAMP 
                                WHERE path=?
                            """, (size, mtime, file_hash, full_path))
                            stats['updated_files'] += 1
                            detailed_results.append(f"🔄 更新: {filename}")
                        else:
                            # 如果文件已完成，不重新處理
                            if existing_status != 'completed':
                                cur.execute("UPDATE files SET status='pending' WHERE path=?", (full_path,))
                                stats['updated_files'] += 1
                            else:
                                stats['duplicate_files'] += 1
                                detailed_results.append(f"⏭️ 跳過: {filename} (已完成)")
                    else:
                        # 檢查內容重複（如果有哈希）
                        if file_hash:
                            cur.execute("SELECT path FROM files WHERE file_hash=?", (file_hash,))
                            duplicate = cur.fetchone()
                            if duplicate:
                                stats['duplicate_files'] += 1
                                detailed_results.append(f"🔁 重複: {filename}")
                                progress.update(task, advance=1)
                                continue
                        
                        # 新文件
                        cur.execute("""
                            INSERT INTO files (path, size, mtime, file_hash, status) 
                            VALUES (?, ?, ?, ?, 'pending')
                        """, (full_path, size, mtime, file_hash))
                        stats['new_files'] += 1
                        detailed_results.append(f"➕ 新增: {filename}")
                    
                    # 更新進度
                    progress.update(task, advance=1)
                    
                except Exception as e:
                    detailed_results.append(f"❌ 錯誤: {filename}: {str(e)[:50]}")
                    stats['error_files'] += 1
                    progress.update(task, advance=1)
            
            # 完成後顯示摘要描述
            progress.update(
                task, 
                description=f"[green]✓ 文件掃描完成: {stats['new_files']} 新增, {stats['updated_files']} 更新[/green]"
            )
    
    else:
        # 無 Rich 的基本版本 - 也保持簡潔
        for i, file_info in enumerate(all_files):
            if i % 50 == 0 or i == len(all_files) - 1:  # 每50個文件或最後一個文件才顯示進度
                progress_percent = ((i + 1) / len(all_files)) * 100
                log(f"掃描進度: ({i+1}/{len(all_files)}, {progress_percent:.1f}%)")
            # ... 處理邏輯 ...
    
    conn.commit()
    
    # 簡潔的最終統計（只有需要的時候才顯示詳細信息）
    console.print(f"\n[bold green]📊 掃描完成:[/bold green] [green]{stats['new_files']} 新增[/green] [yellow]{stats['updated_files']} 更新[/yellow] [blue]{stats['duplicate_files']} 重複[/blue]")
    
    if stats['error_files'] > 0:
        console.print(f"[red]❌ 錯誤文件: {stats['error_files']}[/red]")
    
    return stats

def query_pending_files_count():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # 詳細查詢各種狀態的文件數
        cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
        status_counts = dict(cur.fetchall())
        
        pending_count = status_counts.get('pending', 0)
        
        #log(f"調試: 文件狀態統計: {status_counts}")
        #log(f"調試: 待處理文件數: {pending_count}")
        
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
        
        log(f"動態批次: 開始批次: {self.current_batch_id}")
        return self.current_batch_id
    
    def get_next_file_batch(self, max_files=None, max_size_gb=None):
        """獲取下一批待處理文件"""
        if max_files is None:
            max_files = params.get('batch_size', 1000)
        if max_size_gb is None:
            max_size_gb = params.get('batch_size_gb', 90)
        
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
                log(f"記錄錯誤: 無法記錄批次歷史: {e}")
        
        log(f"動態批次: 選擇 {len(selected_files)} 個文件，總大小 {current_size/1024/1024:.1f}MB")
        return selected_files
    
    def mark_file_pushed(self, file_path):
        """標記文件為已推送"""
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
            log(f"數據庫錯誤: 更新文件狀態失敗: {e}")
            return False
    
    def mark_file_failed(self, file_path, error_msg=None):
        """標記文件推送失敗"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE files SET status='failed', updated_at=CURRENT_TIMESTAMP 
                WHERE path=?
            """, (file_path,))
            self.conn.commit()
            # 錯誤信息由調用方處理
        except Exception as e:
            log(f"數據庫錯誤: 標記文件失敗狀態失敗: {e}")
    
    def complete_batch(self, batch_status='completed'):
        """完成當前批次"""
        if not self.current_batch_id:
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
            
            # 修復：使用實際的批次文件數量
            total_files = len(self.batch_files)
            success_rate = (self.successful_pushes / total_files) * 100 if total_files > 0 else 0
            
            log(f"批次完成: {self.current_batch_id}: {self.successful_pushes}/{total_files} ({success_rate:.1f}%)")
            
        except Exception as e:
            log(f"批次記錄錯誤: 無法更新批次狀態: {e}")
        finally:
            # 重置狀態
            self.current_batch_id = None
            self.batch_files = []
            self.successful_pushes = 0


# /////////////////////////////////////////////////////////////////////////////
# ADB 工具
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

def adb_push_file(local_path, remote_folder):
    filename = os.path.basename(local_path)
    remote_path = f"{remote_folder}/{filename}"
    log(f"ADB: 傳送: {local_path} -> {remote_path}")
    try:
        run_adb_command(["push", local_path, remote_path])
    except Exception as e:
        log(f"ADB: 傳送失敗: {e}")
        try:
            output = run_adb_command(["shell", "ls", remote_path])
            if filename in output:
                log(f"ADB: 檔案已存在，視為推送成功: {remote_path}")
                return
            else:
                raise e
        except Exception as check_e:
            log(f"ADB: 檔案存在檢查失敗: {check_e}")
            raise e

def adb_push_file_silent(local_path, remote_folder):
    """靜默版本的文件推送"""
    filename = os.path.basename(local_path)
    remote_path = f"{remote_folder}/{filename}"
    
    try:
        run_adb_command(["push", local_path, remote_path])
    except Exception as e:
        # 檢查文件是否實際存在
        try:
            output = run_adb_command(["shell", "ls", remote_path])
            if filename in output:
                return
            else:
                raise e
        except Exception:
            raise e

def adb_move_remote_folder(src, dst):
    log(f"ADB: 移動: {src} -> {dst}")
    run_adb_command(["shell", "mv", src, dst])

def adb_remove_remote_folder(folder):
    log(f"ADB: 刪除遠端目錄: {folder}")
    run_adb_command(["shell", "rm", "-rf", folder])

def adb_trigger_media_scan(path):
    uri_path = f"file://{path}"
    log(f"ADB: 觸發媒體掃描: {uri_path}")
    run_adb_command(["shell", "am", "broadcast", "-a", "android.intent.action.MEDIA_SCANNER_SCAN_FILE", "-d", uri_path])

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
        
        log(f"搬移成功: {src_folder} -> {dst_folder}")
        return True
        
    except Exception as e:
        log(f"搬移失敗: {src_folder} -> {dst_folder}: {e}")
        return False

def cleanup_camera_folder(camera_folder):
    """清理Camera目錄中的批次資料夾"""
    try:
        log(f"清理: 刪除Camera資料夾: {camera_folder}")
        adb_remove_remote_folder(camera_folder)
        
        # 觸發媒體掃描
        adb_trigger_media_scan(CAMERA_ROOT)
        log(f"清理成功: {camera_folder}")
        
    except Exception as e:
        log(f"清理失敗: {camera_folder}: {e}")


# /////////////////////////////////////////////////////////////////////////////
# Google Photos CPU 監控
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
        log(f"錯誤: 無法獲取當前批次大小: {e}")
        return 0


# /////////////////////////////////////////////////////////////////////////////
# 批次推送及管理流程
def clean_camera_batch():
    try:
        output = run_adb_command(['shell', 'ls', CAMERA_ROOT])
        batch_dirs = [line.strip() for line in output.splitlines() if line.startswith(BATCH_PREFIX)]
        for batch_dir in batch_dirs:
            full_path = f"{CAMERA_ROOT}/{batch_dir}"
            log(f"清理: 刪除資料夾：{full_path}")
            adb_remove_remote_folder(full_path)
        adb_trigger_media_scan(CAMERA_ROOT)
        log("清理: 完成")
    except Exception as e:
        log(f"清理: 失敗: {e}")

def push_files_individually(batch_manager, file_batch, remote_folder):
    """逐個推送文件 - 支持 rich 和普通模式"""
    try:
        adb_create_remote_folder(remote_folder)
    except Exception as e:
        log(f"推送: 建立遠端目錄失敗: {e}")
        return 0
    
    success_count = 0
    total_files = len(file_batch)
    
    if total_files == 0:
        return 0
    
    # 根據 rich 可用性選擇不同的顯示方式
    if RICH_AVAILABLE:
        return push_files_with_rich_progress(batch_manager, file_batch, remote_folder, total_files)
    else:
        return push_files_basic_progress(batch_manager, file_batch, remote_folder, total_files)

def push_files_with_rich_progress(batch_manager, file_batch, remote_folder, total_files):
    """使用 rich 進度條的推送"""
    success_count = 0
    
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
            f"推送批次文件 ({total_files} 個)", 
            total=total_files
        )
        
        for i, file_info in enumerate(file_batch):
            file_path = file_info['path']
            filename = os.path.basename(file_path)
            
            progress.update(
                task, 
                description=f"推送: {filename[:40]}{'...' if len(filename) > 40 else ''}"
            )
            
            try:
                adb_push_file_silent(file_path, remote_folder)
                
                if batch_manager.mark_file_pushed(file_path):
                    success_count += 1
                
                progress.update(task, advance=1)
                
                if (i + 1) % 5 == 0 or (i + 1) == total_files:
                    update_pending_count_text()
                
            except Exception as e:
                log(f"推送失敗: {filename}: {str(e)[:50]}")
                batch_manager.mark_file_failed(file_path, str(e))
                progress.update(task, advance=1)
        
        progress.update(
            task, 
            description=f"✓ 批次推送完成: {success_count}/{total_files} 成功"
        )
    
    return success_count

def push_files_basic_progress(batch_manager, file_batch, remote_folder, total_files):
    """基本進度顯示的推送（無 rich）"""
    success_count = 0
    
    log(f"推送: 開始推送 {total_files} 個文件...")
    
    for i, file_info in enumerate(file_batch):
        file_path = file_info['path']
        filename = os.path.basename(file_path)
        
        # 顯示進度
        progress_percent = ((i + 1) / total_files) * 100
        log(f"推送: ({i+1}/{total_files}, {progress_percent:.1f}%) {filename}")
        
        try:
            adb_push_file_silent(file_path, remote_folder)
            
            if batch_manager.mark_file_pushed(file_path):
                success_count += 1
            
            if (i + 1) % 5 == 0 or (i + 1) == total_files:
                update_pending_count_text()
            
        except Exception as e:
            log(f"推送失敗: {filename}: {e}")
            batch_manager.mark_file_failed(file_path, str(e))
    
    success_rate = (success_count / total_files) * 100
    log(f"推送完成: {success_count}/{total_files} ({success_rate:.1f}%)")
    
    return success_count

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
        log(f"狀態更新: {completed_count} 個已推送文件標記為完成")
        return completed_count
        
    except Exception as e:
        log(f"狀態更新錯誤: {e}")
        return 0

def check_all_files_processed(conn):
    """檢查是否所有文件都已處理"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE status='pending'")
    pending_count = cur.fetchone()[0]
    return pending_count == 0

def wait_for_backup_complete():
    """Rich 優化的備份完成檢測"""
    stable_seconds = 0
    required_stable = params.get('backup_stable_time', 60)
    
    # 動態調整等待時間
    current_batch_size = get_current_batch_size()
    if params.get('quick_backup_detection', True):
        if current_batch_size < 100:
            required_stable = min(30, required_stable)
            console.print(f"[cyan]⚡ 小批次 ({current_batch_size} 文件)，等待 {required_stable} 秒穩定期[/cyan]")
        elif current_batch_size < 500:
            required_stable = min(45, required_stable)
            console.print(f"[yellow]📦 中等批次 ({current_batch_size} 文件)，等待 {required_stable} 秒穩定期[/yellow]")
        else:
            console.print(f"[magenta]📚 大批次 ({current_batch_size} 文件)，等待 {required_stable} 秒穩定期[/magenta]")
    
    if RICH_AVAILABLE:
        return wait_with_rich_progress(required_stable)
    else:
        return wait_basic_display(required_stable)

def wait_with_rich_progress(required_stable):
    """使用 Rich 進度條的備份等待"""
    start_time = time.time()
    stable_seconds = 0
    reset_count = 0
    max_cpu_seen = 0
    
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=30),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        
        task = progress.add_task(
            "等待 Google Photos 備份", 
            total=required_stable
        )
        
        while stable_seconds < required_stable and batch_processing:
            cpu = get_cpu_usage()
            current_time = time.time()
            max_cpu_seen = max(max_cpu_seen, cpu)
            
            if cpu < params['cpu_threshold']:
                stable_seconds += params['monitor_interval']
                
                # 更新進度條
                progress.update(
                    task, 
                    completed=stable_seconds,
                    description=f"[green]🟢 CPU穩定 ({cpu:.1f}%) - 備份中"
                )
                
            else:
                if stable_seconds > 0:
                    reset_count += 1
                    
                    # 控制重置消息頻率
                    if reset_count <= 3:
                        progress.update(
                            task,
                            completed=0,
                            description=f"[red]🔴 CPU活躍 ({cpu:.1f}%) - 重置計時器"
                        )
                    elif reset_count % 10 == 0:
                        elapsed = int(current_time - start_time)
                        progress.update(
                            task,
                            completed=0,
                            description=f"[orange]🟠 持續活躍 ({cpu:.1f}%) - 已重置{reset_count}次"
                        )
                        console.print(f"[dim]已等待 {elapsed}s，最高CPU: {max_cpu_seen:.1f}%[/dim]")
                    else:
                        progress.update(
                            task,
                            completed=0,
                            description=f"[orange]🟠 處理中 ({cpu:.1f}%)"
                        )
                    
                    stable_seconds = 0
            
            time.sleep(params['monitor_interval'])
        
        if stable_seconds >= required_stable:
            progress.update(
                task,
                completed=required_stable,
                description=f"[bold green]✅ 備份完成 - CPU已穩定{required_stable}秒"
            )
            elapsed = int(time.time() - start_time)
            console.print(f"[bold green]🎉 備份完成！耗時 {elapsed}s，共重置 {reset_count} 次，最高CPU: {max_cpu_seen:.1f}%[/bold green]")
            return True
        else:
            progress.update(task, description="[yellow]⏹️ 備份等待被中斷")
            console.print("[yellow]⚠️ 備份等待被中斷[/yellow]")
            return False

def wait_basic_display(required_stable):
    """基本顯示模式的等待（無rich）"""
    stable_seconds = 0
    reset_count = 0
    start_time = time.time()
    
    log(f"備份等待: 開始等待 {required_stable} 秒穩定期...")
    
    while stable_seconds < required_stable and batch_processing:
        cpu = get_cpu_usage()
        current_time = time.time()
        
        if cpu < params['cpu_threshold']:
            stable_seconds += params['monitor_interval']
            
            if stable_seconds % 10 == 0:
                elapsed = int(current_time - start_time)
                log(f"備份等待: 已穩定 {stable_seconds}/{required_stable}s (CPU: {cpu:.1f}%, 總耗時: {elapsed}s)")
        else:
            if stable_seconds > 0:
                reset_count += 1
                
                if reset_count <= 5 or reset_count % 15 == 0:
                    elapsed = int(current_time - start_time)
                    log(f"備份等待: CPU活躍 ({cpu:.1f}%)，重置計時器 (第{reset_count}次，總耗時: {elapsed}s)")
                
                stable_seconds = 0
        
        time.sleep(params['monitor_interval'])
    
    elapsed = int(time.time() - start_time)
    if stable_seconds >= required_stable:
        log(f"備份完成: CPU已穩定 {required_stable} 秒，總耗時 {elapsed}s，共重置 {reset_count} 次")
        return True
    else:
        log(f"備份中斷: 等待被中斷，總耗時 {elapsed}s")
        return False


# /////////////////////////////////////////////////////////////////////////////
# CPU 監控線程
def cpu_monitor_thread():
    global cpu_monitoring, status_text, cpu_active_flag
    log("CPU監控: 線程啟動")
    
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
            log(f"CPU監控: 錯誤: {e}")
            time.sleep(5)
    
    log("CPU監控: 線程結束")
    
def immediate_completion_check(conn, processed_batches):
    """批次完成後的立即檢查"""
    try:
        # 強制刷新檔案計數
        update_pending_count_text()
        
        # 立即檢查是否完成
        pending_count = query_pending_files_count()
        if pending_count == 0:
            console.print(f"[bold green]⚡ 立即檢測到完成！總共處理 {processed_batches} 個批次[/bold green]")
            show_completion_notification(processed_batches)
            return True
        
        return False
    except Exception as e:
        log(f"立即完成檢查錯誤: {e}")
        return False

def check_all_files_processed_with_retry(conn, max_retries=3):
    """檢查是否所有文件都已處理 - 重試版本"""
    for attempt in range(max_retries):
        try:
            cur = conn.cursor()
            
            # 🔧 使用更直接的查詢
            cur.execute("SELECT COUNT(*) FROM files WHERE status IN ('pending', 'processing')")
            unfinished_count = cur.fetchone()[0]
            
            if unfinished_count == 0:
                return True
            
            # 短暫等待後重試
            if attempt < max_retries - 1:
                time.sleep(0.2)
                
        except Exception as e:
            log(f"檢查完成狀態失敗 (嘗試 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
    
    return False    

def dynamic_batch_process_thread():
    """動態批次處理線程 - 優化完成檢測"""
    global batch_in_process, batch_processing
    console.print("[bold green]🚀 動態批次處理啟動[/bold green]")
    
    try:
        conn = init_db()
        batch_manager = DynamicBatchManager(conn)
        total_processed_batches = 0
        
        while batch_processing:
            try:
                with cpu_status_lock:
                    active = cpu_active_flag
                
                if not active:
                    with batch_processing_lock:
                        if not batch_in_process:
                            file_batch = batch_manager.get_next_file_batch()
                            
                            if file_batch:
                                # ... 批次處理邏輯 ...
                                batch_manager.complete_batch('completed')
                                total_processed_batches += 1
                                
                                # 🔧 立即檢查完成狀態 - 新增
                                if immediate_completion_check(conn, total_processed_batches):
                                    break
                                    
                            else:
                                # 🔧 優化：減少檢查間隔和增加重試機制
                                if check_all_files_processed_with_retry(conn):
                                    update_pending_count_text()
                                    console.print(f"[bold green]🎉 所有文件處理完成！總共處理 {total_processed_batches} 個批次[/bold green]")
                                    show_completion_notification(total_processed_batches)
                                    break
                                else:
                                    time.sleep(0.5)  # 🔧 縮短到0.5秒
                        else:
                            time.sleep(0.5)  # 🔧 縮短檢查間隔
                else:
                    time.sleep(1)
                    
            except Exception as e:
                console.print(f"[red]線程錯誤: {e}[/red]")
                time.sleep(2)
        
        conn.close()
        console.print("[bold blue]📴 動態批次處理結束[/bold blue]")
    
    finally:
        batch_processing = False
        ui_state.set_state('idle')
        update_pending_count_text()



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
        cur.execute("SELECT status, COUNT(*) FROM batch_history GROUP BY status")
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
        log(f"統計錯誤: 無法獲取完成統計: {e}")
        return {
            'file_stats': {'pending': 0, 'completed': 0},
            'batch_stats': {'completed': 0}, 
            'time_stats': (None, None)
        }

def show_completion_notification(processed_batches):
    """顯示處理完成的通知窗口"""
    try:
        # 最終刷新一次文件數
        update_pending_count_text()
        
        # 獲取統計信息
        conn = sqlite3.connect(DB_PATH)
        stats = get_completion_statistics_dynamic(conn)
        conn.close()
        
        # 創建通知窗口
        root = tk.Tk()
        root.title("傳輸完成")
        root.geometry("500x450")
        root.resizable(False, False)
        
        # 設置窗口居中
        root.update_idletasks()
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x = (screen_width // 2) - (500 // 2)
        y = (screen_height // 2) - (450 // 2)
        root.geometry(f"500x450+{x}+{y}")
        
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
        pending_files = file_stats.get('pending', 0)
        
        tk.Label(file_frame,
                text=f"📁 總文件數: {total_files}",
                font=("Microsoft JhengHei", 10)).pack(anchor="w", padx=10)
        tk.Label(file_frame,
                text=f"✅ 成功傳輸: {completed_files}",
                font=("Microsoft JhengHei", 10)).pack(anchor="w", padx=10)
        
        if pending_files > 0:
            tk.Label(file_frame,
                    text=f"⏳ 剩餘待處理: {pending_files}",
                    font=("Microsoft JhengHei", 10), fg="orange").pack(anchor="w", padx=10)
        else:
            tk.Label(file_frame,
                    text=f"🎯 所有文件已處理完成！",
                    font=("Microsoft JhengHei", 10), fg="green").pack(anchor="w", padx=10)
        
        if file_stats.get('failed', 0) > 0:
            tk.Label(file_frame,
                    text=f"❌ 傳輸失敗: {file_stats.get('failed', 0)}",
                    font=("Microsoft JhengHei", 10), fg="red").pack(anchor="w", padx=10)
        
        # 按鈕區域
        button_frame = tk.Frame(root)
        button_frame.pack(pady=20)
        
        # 確定按鈕
        def on_close():
            update_pending_count_text()
            root.destroy()
        
        tk.Button(button_frame, text="確定", 
                 command=on_close,
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
        log(f"錯誤: 顯示完成通知失敗: {e}")
        update_pending_count_text()
        try:
            msgbox.showinfo("傳輸完成", f"動態批次傳輸已完成！\n處理了 {processed_batches} 個批次")
        except:
            log(f"通知: 動態批次傳輸已完成！處理了 {processed_batches} 個批次")


# /////////////////////////////////////////////////////////////////////////////
# 自動啟動功能
def auto_start_cpu_monitoring():
    """程序啟動時自動開始CPU監控"""
    global cpu_monitoring
    if not cpu_monitoring:
        cpu_monitoring = True
        log("自動啟動: CPU監控已開始")
        threading.Thread(target=cpu_monitor_thread, daemon=True).start()
    else:
        log("提示: CPU監控已在運行中")


# /////////////////////////////////////////////////////////////////////////////
# UI 回調函式
def select_folder_with_dynamic_batch():
    """動態批次版的資料夾選擇"""
    root = tk.Tk()
    root.withdraw()
    folder = filedialog.askdirectory()
    if folder:
        log(f"UI: 選擇資料夾: {folder}")
        conn = init_db()
        
        # 使用簡化的掃描（不創建批次）
        stats = scan_and_add_files(conn, folder)
        
        update_pending_count_text()
        conn.close()
        
        log("系統: 文件掃描完成，準備動態批次處理")
    else:
        log("UI: 未選擇資料夾")

def update_status_text():
    status_txt_obj.set_text(f"狀態: {status_text}")
    ax_status.figure.canvas.draw_idle()

def update_pending_count_text():
    try:
        count = query_pending_files_count()
        #log(f"待處理文件數: {count}")
        pending_count_text.set_text(f"待處理文件數: {count:,}")
        ax_pending_count.figure.canvas.draw_idle()
    except Exception as e:
        log(f"刷新失敗: 無法更新待處理文件數: {e}")

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
        ax_cpu.plot(list(range(len(cpu_data))), list(cpu_data), color='red', linewidth=1.5)
        
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
        log(f"防護: {message}")
        return
    
    global batch_processing
    
    if batch_processing:
        log("提示: 傳輸已在進行中")
        return
    
 #  log("DEBUG: 開始動態批次傳輸")
    
    # 確保CPU監控已啟動
    if not cpu_monitoring:
        log("警告: CPU監控未啟動，正在自動啟動...")
        auto_start_cpu_monitoring()
        time.sleep(1)
    
    # 檢查前置條件
    pending_count = query_pending_files_count()
    if pending_count == 0:
        log("提示: 沒有待處理文件，請先掃描資料夾")
        return
        
    # 檢查ADB連接
    try:
        run_adb_command(['devices'])
        log("檢查: ADB連接正常")
    except Exception as e:
        log(f"錯誤: ADB連接失敗: {e}")
        return
    
    # 設置處理狀態
    ui_state.set_state('processing')
    
    # 啟動動態批次處理
    batch_processing = True
    log("UI: 開始動態批次文件傳輸")
    threading.Thread(target=dynamic_batch_process_thread, daemon=True).start()
    log(f"成功: 動態批次處理已啟動，待處理文件數: {pending_count}")

def on_scan_folder_final(event):
    """動態批次版掃描資料夾"""
    can_scan, message = ui_state.can_perform_action('scan_folder', 5.0)
    if not can_scan:
        log(f"防護: {message}")
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
        update_pending_count_text()
        log("UI: 停止動態批次處理")
        log("成功: 動態批次處理已停止")
        
        # 顯示停止通知
        try:
            msgbox.showinfo("傳輸停止", "文件傳輸已手動停止\n待處理文件數已更新")
        except:
            log("通知: 文件傳輸已手動停止")
    else:
        log("提示: 批次處理未在運行")

def on_apply_params(event):
    global params
    try:
        batch_size_val = int(text_batch_size.text)
        batch_size_gb_val = float(text_batch_size_gb.text)
        cpu_threshold_val = float(text_cpu_threshold.text)
        monitor_interval_val = float(text_monitor_interval.text)
        params.update({
            'batch_size': batch_size_val,
            'batch_size_gb': batch_size_gb_val,
            'cpu_threshold': cpu_threshold_val,
            'monitor_interval': monitor_interval_val,
        })
        log(f"UI: 參數更新: batch_size={batch_size_val}, batch_size_gb={batch_size_gb_val}GB, cpu_threshold={cpu_threshold_val}, interval={monitor_interval_val}s")
    except Exception as e:
        log(f"UI: 參數更新錯誤: {e}")
    update_status_text()

def on_refresh_pending_count_final(event):
    """刷新計數"""
    can_refresh, message = ui_state.can_perform_action('refresh', 1.0)
    if not can_refresh:
        log(f"防護: {message}")
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
status_txt_obj = ax_status.text(1.3, 0.5, f"狀態: {status_text}", va='center', fontsize=14)

# 待處理文件數顯示（右對齊到屏幕右側）
ax_pending_count = fig.add_subplot(gs[3, 2:])
ax_pending_count.axis('off')
pending_count_text = ax_pending_count.text(0.95, 0.5, "待處理文件數: 0", fontsize=12, va='center', ha='right')

# 參數輸入區 - 兩行布局
ax_bs = plt.axes([0.15, 0.30, 0.10, 0.04])
text_batch_size = TextBox(ax_bs, 'Batch Size', initial=str(params['batch_size']))
text_batch_size.label.set_fontsize(9)
text_batch_size.text_disp.set_fontsize(9)

ax_bsgb = plt.axes([0.30, 0.30, 0.10, 0.04])
text_batch_size_gb = TextBox(ax_bsgb, 'Size (GB)', initial=str(params['batch_size_gb']))
text_batch_size_gb.label.set_fontsize(9)
text_batch_size_gb.text_disp.set_fontsize(9)

ax_cpu_th = plt.axes([0.15, 0.24, 0.10, 0.04])
text_cpu_threshold = TextBox(ax_cpu_th, 'CPU 門檻', initial=str(params['cpu_threshold']))
text_cpu_threshold.label.set_fontsize(9)
text_cpu_threshold.text_disp.set_fontsize(9)

ax_interval = plt.axes([0.30, 0.24, 0.10, 0.04])
text_monitor_interval = TextBox(ax_interval, '間隔(s)', initial=str(params['monitor_interval']))
text_monitor_interval.label.set_fontsize(9)
text_monitor_interval.text_disp.set_fontsize(9)

# 參數套用按鈕
ax_apply = plt.axes([0.45, 0.27, 0.08, 0.04])
button_apply = Button(ax_apply, '套用')
button_apply.label.set_fontsize(10)
button_apply.on_clicked(on_apply_params)

# 開始與停止按鈕 - 動態批次版本
ax_start = plt.axes([0.58, 0.28, 0.1, 0.06])
button_start = Button(ax_start, '開始傳輸')
button_start.label.set_fontsize(12)
button_start.on_clicked(on_start_dynamic)  # 使用動態批次版本

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
ax_refresh = plt.axes([0.44, 0.18, 0.1, 0.05])
button_refresh = Button(ax_refresh, '刷新數字')
button_refresh.label.set_fontsize(10)
button_refresh.on_clicked(on_refresh_pending_count_final)

# 啟動畫面動畫刷新
ani = FuncAnimation(fig, update, interval=1000)

# /////////////////////////////////////////////////////////////////////////////
# 程式啟動初始化
if __name__ == "__main__":
    log("系統啟動: 正在初始化...")
    
    # 修復現有數據庫結構
    log("系統啟動: 檢查並修復數據庫...")
    fix_existing_database()
    
    # 初始化數據庫
    conn = init_db()
    update_pending_count_text()
    conn.close()

    # 初始化UI狀態管理
    ui_state.set_state('idle')

    # 自動啟動CPU監控
    log("系統啟動: 正在啟動CPU監控...")
    auto_start_cpu_monitoring()
    log("系統啟動: CPU監控已啟動")
    log("系統提示: 點擊'開始傳輸'按鈕開始動態批次處理")
    log("系統提示: UI狀態管理已啟用 - 防止重複操作")
    log("系統說明: 動態批次管理 - 真正的斷點續傳功能")

    plt.tight_layout()
    plt.show()
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
        console.print(f"[green]✓ 推送完成: {success_count}/{total_files} ({success_rate:.1f}%)[/green]")
    else:
        console.print(f"[yellow]⚠ 推送完成: {success_count}/{total_files} ({success_rate:.1f}%) - 成功率偏低[/yellow]")
    
    return success_count