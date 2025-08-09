import os
import re
from mutagen.flac import FLAC
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError

def extract_info_from_folder(folder_name):
    # Match pattern: yyyy.mm.dd followed by album name
    match = re.search(r'^((19|20)\d{2})\.(\d{2})\.(\d{2})\s+(.+)$', folder_name)
    if match:
        year = match.group(1)  # 4-digit year (e.g., 1980)
        month = match.group(3)  # 2-digit month (e.g., 12)
        day = match.group(4)    # 2-digit day (e.g., 25)
        full_date = f"{year}.{month}.{day}"  # Combine to yyyymmdd (e.g., 19801225)
        raw_album = match.group(5).strip()  # Raw album name
        # Remove anything in Chinese parentheses （） and trim
        album = re.sub(r'\s*（[^（）]*）', '', raw_album).strip()
        return full_date, album
    return None, None

def get_info_from_path(dirpath):
    # Split the path into components and check each parent folder for info
    parts = dirpath.split(os.sep)
    for part in reversed(parts):  # Check from innermost to outermost
        full_date, album = extract_info_from_folder(part)
        if full_date and album:
            return full_date, album
    return None, None

def update_music_tags(root_dir, dry_run=False):
    # Walk through all directories and files
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Get the full date and album from the current path or its parents
        full_date, album = get_info_from_path(dirpath)
        
        if full_date and album:
            for filename in filenames:
                # Check if file is FLAC or MP3
                if filename.lower().endswith(('.flac', '.mp3')):
                    file_path = os.path.join(dirpath, filename)
                    
                    try:
                        if filename.lower().endswith('.flac'):
                            # Handle FLAC files
                            audio = FLAC(file_path)
                            audio['date'] = full_date
                            audio['album'] = album
                            audio['genre'] = 'Pop'
                            if not dry_run:
                                audio.save()
                            print(f"{'[DRY RUN] ' if dry_run else ''}Updated date to {full_date}, album to '{album}', genre to Pop for FLAC: {file_path}")
                        
                        elif filename.lower().endswith('.mp3'):
                            # Handle MP3 files
                            try:
                                audio = EasyID3(file_path)
                            except ID3NoHeaderError:
                                # If no ID3 tags exist, create them (but only if not dry-run)
                                audio = EasyID3()
                                if not dry_run:
                                    audio.save(file_path)
                                    audio = EasyID3(file_path)
                            
                            audio['date'] = full_date
                            audio['album'] = album
                            audio['genre'] = 'Pop'
                            if not dry_run:
                                audio.save()
                            print(f"{'[DRY RUN] ' if dry_run else ''}Updated date to {full_date}, album to '{album}', genre to Pop for MP3: {file_path}")
                    
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
        else:
            print(f"No date or album found in path: {dirpath}")

if __name__ == "__main__":
    # Ask user for the root directory
    root_dir = input("Please enter the root directory to scan (e.g., Q:\\男歌手\\譚詠麟 詠麟調61CD [Flac]): ").strip()
    
    # Ask user for dry-run mode
    dry_run_input = input("Enable dry-run mode? (yes/no, default is no): ").strip().lower()
    dry_run = dry_run_input in ('yes', 'y')
    
    # Check if directory exists
    if not os.path.exists(root_dir):
        print(f"Directory not found: {root_dir}")
    else:
        print(f"Scanning directory: {root_dir} {'(dry-run mode)' if dry_run else ''}")
        update_music_tags(root_dir, dry_run)
        print("Tag update process completed.")