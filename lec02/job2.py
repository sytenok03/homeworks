from fastavro import writer, parse_schema
import json
from pathlib import Path


# Avro схема
AVRO_SCHEMA = {
    "type": "record",
    "name": "Sale",
    "namespace": "sales",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "int"}
    ]
}

def read_json_files(raw_dir):
    """Читає всі JSON файли з raw директорії"""
    raw_path = Path(raw_dir)
    
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw directory не існує: {raw_dir}")
    
    json_data = []
    json_files = list(raw_path.glob("*.json"))
    
    if not json_files:
        raise FileNotFoundError(f"Не знайдено JSON файлів в {raw_dir}")
    
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Якщо це список записів
            if isinstance(data, list):
                json_data.extend(data)
            else:
                json_data.append(data)
    
    return json_data

def write_avro_file(data, stg_dir):
    """Записує дані у форматі Avro"""
    stg_path = Path(stg_dir)
    
    # Створюємо папку якщо не існує
    stg_path.mkdir(parents=True, exist_ok=True)
    
    # Витягуємо дату з шляху (останній елемент)
    # Наприклад: /path/to/stg/sales/2022-08-09 → 2022-08-09
    date = stg_path.name
    
    # Формуємо ім'я файлу з датою з директорії
    avro_file = stg_path / f"sales_{date}.avro"
    
    # Парсимо схему
    parsed_schema = parse_schema(AVRO_SCHEMA)
    
    # Записуємо дані в Avro формат
    with open(avro_file, 'wb') as out:
        writer(out, parsed_schema, data)
    
    return str(avro_file)

def run_job(raw_dir, stg_dir):
    """
    Основна функція джоби
    Конвертує JSON файли з raw_dir в Avro формат і зберігає в stg_dir
    """
    # Читаємо JSON
    json_data = read_json_files(raw_dir)
    
    # Записуємо Avro
    avro_file_path = write_avro_file(json_data, stg_dir)
    
    return {
        "avro_file": avro_file_path,
        "records_processed": len(json_data)
    }