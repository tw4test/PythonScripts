import os
from pydub import AudioSegment

os.environ['PATH'] += os.pathsep + 'D:\\Apps\\ffmpeg-hi'

def convert_to_flac(directory, output_format='flac'):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.wav'):
                audio = AudioSegment.from_file(os.path.join(root, file), format='wav')
                file_name, _ = os.path.splitext(file)
                flac_file = os.path.join(root, f'{file_name}.{output_format}')
                audio.export(flac_file, format=output_format)
                print(f'Converted {file} to FLAC format.')

# Confirm the directory path
directory_path = input("Please enter the directory path: ")
print(f"Process: {directory_path}")
confirmation = input("Is this correct? (yes/no): ")

while confirmation.lower() not in ['yes', 'y']:
    directory_path = input("Please enter the directory path: ")
    print(f"Process: {directory_path}")
    confirmation = input("Is this correct? (yes/no): ")
    
convert_to_flac(directory_path)