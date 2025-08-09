import os
import subprocess
from pathlib import Path

# Add FFmpeg path to environment if needed
os.environ['PATH'] += os.pathsep + 'D:\\Apps\\ffmpeg-hi'

def sanitize_filename(filename):
    """Remove invalid filename characters."""
    return ''.join(c for c in filename if c not in '<>:"/\\|?*')

def convert_ape_to_flac_in_folder(folder_path, ffmpeg_path=None):
    """Convert APE files to FLAC in the specified folder and subfolders."""
    input_path = Path(folder_path)
    ffmpeg_cmd = ffmpeg_path if ffmpeg_path else 'ffmpeg'
    
    # Find all .ape files recursively
    ape_files = list(input_path.rglob('*.ape'))
    if not ape_files:
        print(f"No APE files found in {folder_path} or its subfolders.")
        return 0
    
    total_files = len(ape_files)
    print(f"Found {total_files} APE file(s) in {folder_path}")
    
    converted_count = 0
    for i, ape_file in enumerate(ape_files, 1):
        print(f"\nProcessing file {i}/{total_files}: {ape_file}")
        
        # Create output filename (replace .ape with .flac)
        output_file = ape_file.with_suffix('.flac')
        
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # FFmpeg command
        cmd = [
            ffmpeg_cmd,
            '-i', str(ape_file),        # Input file
            '-c:a', 'flac',            # Audio codec: FLAC
            '-compression_level', '8', # Higher compression (optional, adjustable)
            '-y',                      # Overwrite output if exists
            str(output_file)           # Output file
        ]
        
        try:
            # Run FFmpeg conversion
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print(f"Successfully converted to: {output_file}")
            converted_count += 1
            
        except subprocess.CalledProcessError as e:
            print(f"Error converting {ape_file}: {e.stderr.decode().strip()}")
        except FileNotFoundError:
            print("FFmpeg not found. Please install FFmpeg or specify its path.")
            return -1
            
    return converted_count

def process_folders_from_file(input_file, ffmpeg_path=None):
    """Process folders listed in the input file."""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            folders = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Input file not found: {input_file}")
        return
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    if not folders:
        print("No valid folder paths found in the input file.")
        return
    
    total_converted = 0
    for i, folder in enumerate(folders, 1):
        print(f"\n[{i}/{len(folders)}] Processing folder: {folder}")
        if not os.path.isdir(folder):
            print(f"Skipping - Not a valid directory: {folder}")
            continue
        converted = convert_ape_to_flac_in_folder(folder, ffmpeg_path)
        if converted >= 0:
            total_converted += converted
    
    print(f"\nConversion process completed. Total files converted: {total_converted}")

def main():
    """Main function to run the APE to FLAC converter from an input file."""
    input_file = input("Please enter the path to the input file (containing folder paths): ")
    
    if not os.path.isfile(input_file):
        print("Invalid input file path.")
        return
        
    ffmpeg_path = None  # Optional: e.g., r"C:\ffmpeg\bin\ffmpeg.exe"
    process_folders_from_file(input_file, ffmpeg_path)

if __name__ == "__main__":
    main()