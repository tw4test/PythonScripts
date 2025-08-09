import os
import subprocess

# Folder containing the mp3 files
input_folder = r"E:\Temp\foruvr"
output_folder = r"E:\Temp\foruvr"

# Whisper command parameters
device = "cuda"
model = "large-v2"
output_format = "srt"

# Iterate over all files in the input folder
for filename in os.listdir(input_folder):
    if filename.lower().endswith(".mp3"):
        input_path = os.path.join(input_folder, filename)
        
        # Build the whisper command
        command = [
            "whisper",
            "--device", device,
            "--model", model,
            "--output_dir", output_folder,
            "--output_format", output_format,
            input_path
        ]
        
        print(f"Processing {filename}...")
        # Run the command
        subprocess.run(command, check=True)

print("All files processed.")
