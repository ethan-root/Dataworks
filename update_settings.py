import json
from pathlib import Path

features_dir = Path("features")
for f in features_dir.glob("*/setting-*.json"):
    feature_name = f.parent.name
    try:
        data = json.loads(f.read_text("utf8"))
        if "task" in data:
            if "reader_prefix" not in data["task"]:
                # user-feature -> user_feature
                safe_name = feature_name.replace("-", "_")
                data["task"]["reader_prefix"] = f"camos/{safe_name}/"
            if "mc_partition_retention" not in data["task"]:
                data["task"]["mc_partition_retention"] = 30
            f.write_text(json.dumps(data, indent=4), "utf8")
            print(f"Updated {f}")
    except Exception as e:
        print(f"Error on {f}: {e}")
