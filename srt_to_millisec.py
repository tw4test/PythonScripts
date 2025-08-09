from datetime import datetime, timedelta
import re
import sys

def interpolate_srt_segment(start, end, text, base_index, count=60):
    t_start = datetime.strptime(start, "%H:%M:%S,%f")
    t_end = datetime.strptime(end, "%H:%M:%S,%f")
    delta = (t_end - t_start) / count

    entries = []
    for i in range(count):
        t1 = t_start + delta * i
        t2 = t_start + delta * (i + 1)
        # 修正時間格式化：
        time1 = t1.strftime("%H:%M:%S,") + f"{t1.microsecond // 1000:03d}"
        time2 = t2.strftime("%H:%M:%S,") + f"{t2.microsecond // 1000:03d}"
        label = f"{text}:{i+1:02d}"
        entry = f"{base_index + i}\n{time1} --> {time2}\n{label}\n\n"
        entries.append(entry)
    return ''.join(entries)


def batch_interpolate_srt(input_path, output_path, segment_count=60):
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 匹配 SRT 條目: 編號 + 時間軸 + 文字（至少一行）
    pattern = re.compile(r"(\d+)\s+(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\s+(.+?)(?=\n\n|\Z)", re.DOTALL)
    matches = pattern.findall(content)

    output = []
    idx = 1
    for seq_num, start_time, end_time, text in matches:
        text_line = text.strip().replace('\n', ' ')  # 多行合併為單行
        segment_srt = interpolate_srt_segment(start_time, end_time, text_line, idx, count=segment_count)
        output.append(segment_srt)
        idx += segment_count

    with open(output_path, 'w', encoding='utf-8') as f_out:
        f_out.write(''.join(output))

    print(f"完成批量插值輸出: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python script.py input.srt output.srt [segment_count]")
        print("參數:")
        print("  input.srt       原始SRT檔案路徑")
        print("  output.srt      輸出檔案路徑")
        print("  segment_count   (選填)每秒分幾份，預設60")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    segment = int(sys.argv[3]) if len(sys.argv) > 3 else 60

    batch_interpolate_srt(input_file, output_file, segment_count=segment)
