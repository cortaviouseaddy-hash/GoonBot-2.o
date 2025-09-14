import json
from pathlib import Path
from typing import Dict, List

def load_presets(path: str = "activities.json") -> Dict[str, List[str]]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for key in ("raids", "dungeons", "exotic_activities"):
        if key not in data or not isinstance(data[key], list):
            raise ValueError(f"activities.json missing key: {key}")
    return data