"""Module for writing data to Avro format."""

from pathlib import Path
from fastavro import writer, parse_schema


def save_to_avro(data: list, stg_dir: str) -> None:
    """
    Save data to Avro file.
    
    Args:
        data: List of records to save
        stg_dir: Path to directory where Avro file will be saved
    """
    stg_path = Path(stg_dir)
    stg_path.mkdir(parents=True, exist_ok=True)
    
    # Очистити папку (ідемпотентність)
    for file in stg_path.glob("*.avro"):
        if file.is_file():
            file.unlink()
    
    # Визначити схему на основі першого запису
    # Можна адаптувати під конкретну структуру даних
    schema = {
        "type": "record",
        "name": "Sales",
        "fields": []
    }
    
    # Автоматично визначаємо поля з першого запису
    if data:
        first_record = data[0]
        for key, value in first_record.items():
            field_type = get_avro_type(value)
            schema["fields"].append({
                "name": key,
                "type": ["null", field_type]  # Nullable поля
            })
    
    parsed_schema = parse_schema(schema)
    
    # Зберегти в Avro
    output_file = stg_path / "sales.avro"
    with open(output_file, 'wb') as out:
        writer(out, parsed_schema, data)
    
    print(f"\tSaved {len(data)} records to {output_file}")


def get_avro_type(value) -> str:
    """
    Determine Avro type based on Python value type.
    
    Args:
        value: Python value
        
    Returns:
        Avro type as string
    """
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "long"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return {"type": "array", "items": "string"}
    elif isinstance(value, dict):
        return "string"  # Конвертуємо dict в JSON string
    else:
        return "string"