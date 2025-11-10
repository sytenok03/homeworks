"""Module for reading JSON files from disk."""

import json
from pathlib import Path


def read_json_from_disk(raw_dir: str) -> list:
    """
    Read all JSON files from raw directory.
    
    Args:
        raw_dir: Path to directory with JSON files
        
    Returns:
        List of all records from JSON files
    """
    raw_path = Path(raw_dir)
    
    if not raw_path.exists():
        raise FileNotFoundError(f"Directory {raw_dir} does not exist")
    
    all_data = []
    
    # Читаємо всі JSON файли
    for json_file in raw_path.glob("*.json"):
        print(f"\tReading {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_data.extend(data)
    
    if not all_data:
        raise ValueError(f"No data found in {raw_dir}")
    
    print(f"\tTotal records read: {len(all_data)}")
    return all_data