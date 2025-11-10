"""Module for fetching sales data from external API."""

import requests


def get_sales(date: str, api_key: str) -> list:
    """
    Fetch sales data from API for a specific date.
    
    Args:
        date: Date in format YYYY-MM-DD
        api_key: Authentication token for API
        
    Returns:
        List of sales records
    """
    api_url = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"
    headers = {"Authorization": api_key}
    
    all_data = []
    page = 1
    
    # Завантажити всі сторінки
    while True:
        params = {"date": date, "page": page}
        response = requests.get(
            api_url,
            headers=headers,
            params=params
        )
        
        # Зупинитися якщо немає даних
        if response.status_code == 404:
            break
            
        response.raise_for_status()
        data = response.json()
        
        if not data:
            break
            
        all_data.extend(data)
        page += 1
    
    return all_data