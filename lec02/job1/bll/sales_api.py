"""Business logic layer for sales data processing."""

from dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str, auth_token: str) -> None:
    """
    Get sales data from API and save to local disk.
    
    Args:
        date: Date in format YYYY-MM-DD
        raw_dir: Full path to save directory
        auth_token: Authentication token for API
    """
    print("\tI'm in save_sales_to_local_disk(...) function!")
    
    # 1. Get data from the API
    print(f"\tFetching data for {date}...")
    data = sales_api.get_sales(date=date, api_key=auth_token)
    print(f"\tFetched {len(data)} records")
    
    # 2. Save data to disk
    print(f"\tSaving data to {raw_dir}...")
    local_disk.save_to_disk(data=data, date=date, raw_dir=raw_dir)
    print("\tDone!")