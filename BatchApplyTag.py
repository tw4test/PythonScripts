import os
import re
from mutagen.flac import FLAC
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError

def extract_year_from_folder(folder_name):
    # Use regex to find a 4-digit year (e.g., 1990, 1991)
    match = re.search(r'\b(19|20)\d{2}\b', folder_name)
    if match:
        return match.group(0)
    return None

def update_music_tags(root_dir, dry_run=False):
    # Walk through all directories and files
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Get the parent folder name
        parent_folder = os.path.basename(dirpath)
        year = extract_year_from_folder(parent_folder)
        
        if year:
            for filename in filenames:
                # Check if file is FLAC or MP3
                if filename.lower().endswith(('.flac', '.mp3')):
                    file_path = os.path.join(dirpath, filename)
                    
                    try:
                        if filename.lower().endswith('.flac'):
                            # Handle FLAC files
                            audio = FLAC(file_path)
                            audio['date'] = year
                            audio['genre'] = 'Pop'
                            if not dry_run:
                                audio.save()
                            print(f"{'[DRY RUN] ' if dry_run else ''}Updated year to {year} and genre to Pop for FLAC: {file_path}")
                        
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
                            
                            audio['date'] = year
                            audio['genre'] = 'Pop'
                            if not dry_run:
                                audio.save()
                            print(f"{'[DRY RUN] ' if dry_run else ''}Updated year to {year} and genre to Pop for MP3: {file_path}")
                    
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
        else:
            print(f"No year found in folder name: {parent_folder}")

if __name__ == "__main__":
    # Ask user for the root directory
    root_dir = input("Please enter the root directory to scan:").strip()
    
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