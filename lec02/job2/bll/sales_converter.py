"""Business logic layer for converting sales data from JSON to Avro."""

from dal import json_reader, avro_writer


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
    """
    Read JSON from raw directory and convert to Avro in stg directory.
    
    Args:
        raw_dir: Path to directory with JSON files
        stg_dir: Path to directory where Avro file will be saved
    """
    print("\tI'm in convert_json_to_avro(...) function!")
    
    # 1. Read JSON data
    print(f"\tReading JSON from {raw_dir}...")
    data = json_reader.read_json_from_disk(raw_dir=raw_dir)
    
    # 2. Save to Avro
    print(f"\tSaving to Avro in {stg_dir}...")
    avro_writer.save_to_avro(data=data, stg_dir=stg_dir)
    
    print("\tDone!")