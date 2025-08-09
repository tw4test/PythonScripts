import os
import shutil
from loguru import logger

# logger.add("info.log", level="info")

def move_files(dest_path, src_path):
    for root, _, files in os.walk(src_path):
        for name in files:
            source = os.path.join(root, name)
            destination = os.path.join(dest_path, name)
            logger.info(f"Moving file: {source} to {destination}")
            shutil.move(source, destination)

def dry_run_move_files(dest_path, src_path):
    logger.info("Dry run mode: ")
    for root, _, files in os.walk(src_path):
        for name in files:
            source = os.path.join(root, name)
            destination = os.path.join(dest_path, name)
            logger.info(f"Would move file: {source} to {destination}")

def process_directories(src: str, dest: str, dry_run: bool = False):
    logger.info(f"Source directory: {src}")
    logger.info(f"Destination directory: {dest}")
    logger.info(f"Dry run mode: {dry_run}")

    for dir in os.listdir(src):
        src_dir_path = os.path.join(src, dir, dir)  # Second level directory
        dest_dir_path = os.path.join(dest, dir)
        logger.info(f"Processing directory: {src_dir_path}")

        if not os.path.isdir(dest_dir_path):
            logger.info(f"Would create directory: {dest_dir_path}")
            if not dry_run:
                os.makedirs(dest_dir_path)

        if os.path.isdir(src_dir_path):
            for item in os.listdir(src_dir_path):
                item_path = os.path.join(src_dir_path, item)
                if os.path.isfile(item_path):
                    if dry_run:
                        logger.info(f"Would move file: {item_path} to {dest_dir_path}")
                    else:
                        shutil.move(item_path, dest_dir_path)
                elif os.path.isdir(item_path):
                    dest_item_path = os.path.join(dest_dir_path, item)
                    if dry_run:
                        logger.info(f"Would move directory: {item_path} to {dest_item_path}")
                    else:
                        shutil.move(item_path, dest_item_path)

# Call the function with your specific parameters
# process_directories('E:/BaiduNetdiskDownload/王菲 无损全集 (29CD)', 'E:/BaiduNetdiskDownload/王菲', True)

def relocate_songs(src: str, dry_run: bool = False):
    for son in os.listdir(src):
        son_path = os.path.join(src, son)
        if os.path.isdir(son_path):
            for root, dirs, files in os.walk(son_path):
                for file in files:
                    if file.endswith(('.ape', '.cue', '.wav')):
                        src_file_path = os.path.join(root, file)
                        dest_file_path = os.path.join(son_path, file)
                        if dry_run:
                            logger.info(f"Would move file: {src_file_path} to {dest_file_path}")
                        else:
                            shutil.move(src_file_path, dest_file_path)
                            logger.info(f"Moved file: {src_file_path} to {dest_file_path}")

relocate_songs('E:\\BaiduNetdiskDownload\\王菲 无损全集 (29CD)', dry_run=False)