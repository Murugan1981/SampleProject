To clear old response files before saving new ones, you should delete all `.json` files in the following two folders:

```
shared/reports/{System}/
  ├── Source_json/
  └── Target_json/
```

---

### ✅ Code to clear old `.json` files:

```python
import glob

def clear_old_responses(system):
    """Delete all .json files in the Source_json and Target_json folders for the given system."""
    base_path = os.path.join("shared", "reports", system)
    source_folder = os.path.join(base_path, "Source_json")
    target_folder = os.path.join(base_path, "Target_json")

    for folder in [source_folder, target_folder]:
        if os.path.exists(folder):
            for file in glob.glob(os.path.join(folder, "*.json")):
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"Failed to delete {file}: {e}")
```

---

### 🧩 Where to place this code:

You should call this function **before the loop begins** that processes test cases and writes new JSONs.

---

### ✅ Add this call in your `main()` function — **after loading the System name**:

```python
SYSTEM = api_testdata.get("System", "UNKNOWN_SYSTEM")
clear_old_responses(SYSTEM)  # ← Insert here
```

Let me know if you want to also:

* Print how many files were deleted
* Add a confirmation prompt before deletion
* Support `.bak` or other formats too

Ready to patch this in.
