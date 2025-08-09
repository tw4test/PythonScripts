import subprocess
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque

PACKAGE_NAME = 'com.google.android.apps.photos'  # 目標應用包名

cpu_data = deque(maxlen=60)     # 近期60秒的CPU使用率數據
recent_cpu = deque(maxlen=10)   # 最近10次數據用於判斷狀態

def get_pid():
    """取得 Google Photos 的 PID"""
    try:
        pid_output = subprocess.check_output(
            ['adb', 'shell', 'pidof', 'com.google.android.apps.photos'],
            text=True
        ).strip()
        if pid_output:
            return pid_output.split()[0]  # 有時候會返回多個PID，取第一個
    except subprocess.CalledProcessError:
        return None
    return None


def get_cpu_usage():
    """用 PID 從 top 輸出中取得 CPU 使用率"""
    try:
        pid = get_pid()
        if not pid:
            print("Google Photos is not running.")
            return 0.0

        output = subprocess.check_output(['adb', 'shell', 'top', '-n', '1'], text=True)

        for line in output.splitlines():
            if pid in line and 'grep' not in line:
                parts = line.split()
                # CPU% 在第10欄（index 9）
                if len(parts) > 8:
                    cpu_str = parts[8]
                    try:
                        return float(cpu_str.strip('%'))
                    except ValueError:
                        pass
        print("PID found but CPU value parse failed.")
    except Exception as e:
        print(f"Error getting CPU usage: {e}")
    return 0.0



def estimate_activity():
    """根據最近CPU使用率平均判斷應用是否活躍"""
    if len(recent_cpu) < 5:
        return "Checking..."
    avg_cpu = sum(recent_cpu) / len(recent_cpu)
    cpu_threshold = 20.0  # 平均超過此閾值視為活躍
    if avg_cpu > cpu_threshold:
        return f"Active (Avg CPU: {avg_cpu:.1f}%)"
    else:
        return f"Idle (Avg CPU: {avg_cpu:.1f}%)"

# 建立 matplotlib 視窗與軸
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 6))

ax1.set_title('Google Photos CPU Usage (%)')
ax1.set_ylim(0, 150)
ax1.set_xlabel('Time (s)')
ax1.set_ylabel('CPU %')

ax2.axis('off')  # 第二軸用於顯示狀態文字，無座標軸  

def update(frame):
    cpu_percent = get_cpu_usage()
    cpu_data.append(cpu_percent)
    recent_cpu.append(cpu_percent)

    ax1.clear()
    ax1.set_title('Google Photos CPU Usage (%)')
    ax1.set_ylim(0, 150)
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('CPU %')
    ax1.plot(list(range(len(cpu_data))), list(cpu_data), 'r-')

    status = estimate_activity()
    ax2.clear()
    ax2.axis('off')
    ax2.text(0.5, 0.5, f'Status: {status}', fontsize=14, ha='center')

# 設定動畫，每1000毫秒更新一次
ani = animation.FuncAnimation(fig, update, interval=1000)

plt.tight_layout()
plt.show()
