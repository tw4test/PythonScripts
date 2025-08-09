import os
import shutil
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sort_videos.log'),
        logging.StreamHandler()
    ]
)

input_folder = r"E:\TO_NAS_temp\iphone\video"
target_folder = r"E:\TO_NAS_SORTED"

def create_target_path(filename):
    match = re.match(r"(\d{4})_(\d{2})_(\d{2})", filename)
    if not match:
        logging.error(f"Invalid filename format: {filename}")
        return None
    year, month, day = match.groups()
    return os.path.join(target_folder, year, f"{year}_{month}", f"{year}_{month}_{day}")

def move_file(file_path, filename):
    try:
        target_path = create_target_path(filename)
        if not target_path:
            return
        os.makedirs(target_path, exist_ok=True)
        target_file = os.path.join(target_path, filename)
        shutil.move(file_path, target_file)
        logging.info(f"Moved {filename} to {target_file}")
    except PermissionError:
        logging.error(f"Permission denied moving {filename}")
    except Exception as e:
        logging.error(f"Failed to move {filename}: {str(e)}")

def main():
    if not os.path.exists(input_folder):
        logging.error(f"Input folder {input_folder} does not exist")
        return
    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        if os.path.isfile(file_path):
            logging.info(f"Processing {filename}")
            move_file(file_path, filename)

if __name__ == "__main__":
    main()