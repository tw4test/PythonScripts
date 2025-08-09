import os
import glob
import codecs

def print_txt_files(directory):
    for filename in glob.glob(os.path.join(directory, '**', '*.cue'), recursive=True):
        try:
            with codecs.open(filename, 'r', 'utf-8') as file:
                print(f"Contents of {filename}:")
                print(file.read())
                print("-------------------------------")
        except UnicodeDecodeError:
            try:
                with codecs.open(filename, 'r', 'cp950') as file:
                    print(f"Contents of {filename}:")
                    print(file.read())
                    print("-------------------------------")
            except Exception as exc:
                print(f'Error reading file {filename}: {str(exc)}')





# Confirm the directory path
directory_path = input("Please enter the directory path: ")
print(f"Process: {directory_path}")
confirmation = input("Is this correct? (yes/no): ")

while confirmation.lower() not in ['yes', 'y']:
    directory_path = input("Please enter the directory path: ")
    print(f"Process: {directory_path}")
    confirmation = input("Is this correct? (yes/no): ")


print_txt_files(directory_path)