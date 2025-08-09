import os
import re
import subprocess
from pathlib import Path
os.environ['PATH'] += os.pathsep + 'D:\\Apps\\ffmpeg-hi'

def detect_encoding(file_path):
    """Attempt to detect file encoding with fallback."""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read()
            return encoding
        except UnicodeDecodeError:
            continue
    return 'utf-8'

def parse_cue_file(cue_path):
    """Parse CUE file and extract track information and metadata."""
    tracks = []
    audio_file = None
    album = None
    performer = None
    
    try:
        encoding = detect_encoding(cue_path)
        with open(cue_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
            
        current_track = {}
        for line in lines:
            line = line.strip()
            file_match = re.match(r'FILE\s+"(.+)"\s+WAVE', line)
            if file_match:
                audio_file = file_match.group(1)
                print(f"Parsed audio file from CUE: {audio_file}")
                
            album_match = re.match(r'TITLE\s+"(.+)"', line)
            if album_match and not current_track:  # Album title is outside TRACK blocks
                album = album_match.group(1)
                
            performer_match = re.match(r'PERFORMER\s+"(.+)"', line)
            if performer_match and not current_track:  # Performer is outside TRACK blocks
                performer = performer_match.group(1)
                
            track_match = re.match(r'TRACK\s+(\d+)\s+AUDIO', line)
            if track_match:
                if current_track:
                    tracks.append(current_track)
                current_track = {'number': track_match.group(1)}
                
            title_match = re.match(r'TITLE\s+"(.+)"', line)
            if title_match and current_track:
                current_track['title'] = title_match.group(1)
                
            index_match = re.match(r'INDEX\s+01\s+(\d+:\d+:\d+)', line)
            if index_match and current_track:
                current_track['index'] = index_match.group(1)
                
        if current_track:
            tracks.append(current_track)
        return audio_file, tracks, album, performer
    
    except Exception as e:
        print(f"Error parsing CUE file: {e}")
        return None, None, None, None

def to_seconds(timestamp):
    """Convert MM:SS:FF (frames) to seconds with milliseconds."""
    m, s, f = map(int, timestamp.split(':'))
    return m * 60 + s + f / 75

def calculate_duration(start_time, end_time):
    """Calculate duration between two timestamps in seconds."""
    start_secs = to_seconds(start_time)
    end_secs = to_seconds(end_time)
    return end_secs - start_secs

def sanitize_filename(title):
    """Remove invalid filename characters."""
    return re.sub(r'[<>:"/\\|?*]', '', title.strip())

def find_audio_file(cue_path, audio_file):
    """Find the actual audio file based on CUE reference and existing files."""
    cue_dir = cue_path.parent
    cue_stem = cue_path.stem
    
    # If audio_file has an extension, check if it exists
    if '.' in audio_file:
        full_path = cue_dir / audio_file
        if full_path.exists():
            return full_path
    
    # Otherwise, try common audio extensions with the cue filename stem
    extensions = ['.ape', '.wav', '.flac']
    for ext in extensions:
        candidate = cue_dir / (cue_stem + ext)
        if candidate.exists():
            print(f"Found matching audio file: {candidate}")
            return candidate
    
    print(f"No matching audio file found for {audio_file}")
    return None

def convert_to_flac(input_dir, ffmpeg_path=None):
    """Convert audio to FLAC tracks with metadata, recursively processing subfolders."""
    input_path = Path(input_dir)
    ffmpeg_cmd = ffmpeg_path if ffmpeg_path else 'ffmpeg'
    
    cue_files = list(input_path.rglob('*.cue'))
    if not cue_files:
        print("No CUE files found in the specified directory or its subfolders.")
        return
    
    for cue_file in cue_files:
        print(f"Processing: {cue_file}")
        audio_file_ref, tracks, album, performer = parse_cue_file(cue_file)
        if not audio_file_ref or not tracks:
            print(f"Skipping {cue_file} due to parsing errors.")
            continue
            
        audio_path = find_audio_file(cue_file, audio_file_ref)
        if not audio_path:
            print(f"Skipping {cue_file} due to missing audio file.")
            continue
            
        for i, track in enumerate(tracks):
            output_file = cue_file.parent / f"{track['number']}.{sanitize_filename(track['title'])}.flac"
            start_time = to_seconds(track['index'])
            
            # Base FFmpeg command
            cmd = [ffmpeg_cmd, '-i', str(audio_path), '-ss', str(start_time)]
            
            # Add duration for all but the last track
            if i < len(tracks) - 1:
                duration = calculate_duration(track['index'], tracks[i + 1]['index'])
                cmd.extend(['-t', str(duration)])
            
            # Add metadata
            cmd.extend([
                '-metadata', f"title={track['title']}",
                '-metadata', f"track={track['number']}",
            ])
            if album:
                cmd.extend(['-metadata', f"album={album}"])
            if performer:
                cmd.extend(['-metadata', f"artist={performer}"])
            
            # Complete the command with codec and output
            cmd.extend(['-c:a', 'flac', '-y', str(output_file)])
            
            try:
                result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print(f"Created: {output_file}")
            except subprocess.CalledProcessError as e:
                print(f"Error converting track {track['number']}: {e.stderr.decode()}")
            except FileNotFoundError:
                print("FFmpeg not found. Please install FFmpeg or specify its path.")
                return

def main():
    """Main function to run the converter."""
    input_dir = input("Please enter the directory path: ")
    
    if not os.path.isdir(input_dir):
        print("Invalid directory path.")
        return
        
    ffmpeg_path = None  # e.g., r"C:\ffmpeg\bin\ffmpeg.exe"
    convert_to_flac(input_dir, ffmpeg_path)
    print("Conversion process completed.")

if __name__ == "__main__":
    main()