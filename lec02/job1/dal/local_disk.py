"""Module for saving data to local disk."""

import json
from pathlib import Path


def save_to_disk(data: list, date: str, raw_dir: str) -> None:
    """
    Save data to local disk as JSON file.
    
    Args:
        data: List of records to save
        date: Date in format YYYY-MM-DD
        raw_dir: Full path to save directory
    """
    # Створити папку якщо не існує
    save_path = Path(raw_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    
    # Очистити папку (ідемпотентність)
    for file in save_path.glob("*"):
        if file.is_file():
            file.unlink()
    
    # Зберегти дані в JSON
    output_file = save_path / f"sales_{date}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved {len(data)} records to {output_file}")