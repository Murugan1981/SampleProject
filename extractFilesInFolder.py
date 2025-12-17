import os

def get_all_files_from_folder(folder_path):
    """
    Recursively extracts all file paths from the given folder.
    """
    all_files = []
    
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    
    return all_files


# === USAGE EXAMPLE ===
folder_path = r"C:\Your\Folder\Path"  # Replace with your actual path
files = get_all_files_from_folder(folder_path)

print(f"Total {len(files)} files found:")
for f in files:
    print(f)
