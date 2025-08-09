import os

def calculate_size_and_files_from_yyyy_sorted(base_dir):
    result = {}
    print(f"Scanning base directory: {base_dir}")
    if os.path.isdir(base_dir):
        # Collect only directories and sort them (MM folders)
        months = [month for month in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, month))]
        months.sort()
        for month in months:
            month_path = os.path.join(base_dir, month)
            total_size = 0
            total_files = 0
            for root, dirs, files in os.walk(month_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    total_files += 1
                    total_size += os.path.getsize(file_path)
            size_gb = total_size / (1024 ** 3)  # Convert bytes to GB
            print(f"MM folder: {month_path}, Files: {total_files}, Size: {size_gb:.2f} GB")
            result[month_path] = {
                'total_size_gb': round(size_gb, 2),
                'total_files': total_files
            }
    else:
        print(f"Base directory does not exist: {base_dir}")
    return result

# Example usage:
result = calculate_size_and_files_from_yyyy_sorted(r'N:\2025')
#print(result)
