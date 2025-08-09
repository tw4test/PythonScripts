import os
from PIL import Image

def convert_images_to_jpg(directory):
    # Valid extensions (case-insensitive handled in the logic)
    valid_extensions = ('.webp', '.bmp')
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Check if file ends with any valid extension (case-insensitive)
            if file.lower().endswith(valid_extensions):
                file_path = os.path.join(root, file)
                # Create new filename by replacing extension with .jpg
                new_file_path = os.path.splitext(file_path)[0] + '.jpg'
                
                try:
                    img = Image.open(file_path)
                    # Convert to RGB if needed (WEBP might have alpha channel)
                    if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                        img = img.convert('RGB')
                    img.save(new_file_path, 'JPEG', quality=95)
                    print(f"Converted {file_path} to {new_file_path}")
                except Exception as e:
                    print(f"Failed to convert {file_path}: {e}")

# Specify the directory to start the search from
directory_path = input("Please enter the directory path: ")

convert_images_to_jpg(directory_path)