# core/orchestrator.py

import json
from datetime import datetime
from .task_registry import TASK_REGISTRY

def run_suite(env_name: str, suite_config_path: str):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"🚀 Running suite: {suite_config_path} | Environment: {env_name} | Run ID: {run_id}")

    with open(suite_config_path, "r") as f:
        suite_cfg = json.load(f)

    tasks = suite_cfg.get("tasks", [])
    for task in tasks:
        name = task["name"]
        enabled = task.get("enabled", True)
        params = task.get("params", {})

        if not enabled:
            print(f"⏩ Skipping disabled task: {name}")
            continue

        task_fn = TASK_REGISTRY.get(name)
        if not task_fn:
            print(f"❌ Task '{name}' not found in registry")
            continue

        print(f"\n▶️ Executing task: {name}")
        result = task_fn(env_cfg={}, params=params, run_id=run_id)
        print(f"✅ Task finished: {name} | Result: {result['status']}\n")
