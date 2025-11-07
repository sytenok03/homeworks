"""Module for extracting sales data from API."""

import json
from pathlib import Path

import requests


def run_sales_job(date: str, raw_dir: str, auth_token: str) -> dict:
    """
    Extract sales data from API and save to raw directory.

    Args:
        date: Date in format YYYY-MM-DD
        raw_dir: Directory path (може бути або базова папка, або повний шлях)
        auth_token: Authentication token for API

    Returns:
        Dictionary with job execution results
    """
    # ЗМІНЕНО: Перевіряємо, чи raw_dir вже містить повну структуру
    raw_dir_path = Path(raw_dir)
    
    # Якщо raw_dir вже закінчується на дату (наприклад: /raw/sales/2022-08-09)
    # то використовуємо його напряму
    if raw_dir_path.name == date:
        save_path = raw_dir_path
    else:
        # Інакше додаємо структуру /raw/sales/date
        save_path = raw_dir_path / "raw" / "sales" / date
    
    save_path.mkdir(parents=True, exist_ok=True)

    # Ensure idempotency: clear directory before writing
    for file in save_path.glob("*"):
        if file.is_file():
            file.unlink()

    api_url = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"
    headers = {"Authorization": auth_token}

    all_data = []
    page = 1

    # Fetch all pages from API
    while True:
        params = {"date": date, "page": page}
        response = requests.get(
            api_url,
            headers=headers,
            params=params
        )

        # Stop if no more data available
        if response.status_code == 404:
            break

        response.raise_for_status()
        data = response.json()

        if not data:
            break

        all_data.extend(data)
        page += 1

    # Save all data to single JSON file
    output_file = save_path / f"sales_{date}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    return {
        'date': date,
        'total_records': len(all_data),
        'output_file': str(output_file)
    }