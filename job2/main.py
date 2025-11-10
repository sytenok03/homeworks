"""
Flask server for converting JSON sales data to Avro format
"""
from flask import Flask, request
from flask import typing as flask_typing

from bll.sales_converter import convert_json_to_avro

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and triggers conversion
    
    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')
    
    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400
    
    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400
    
    # Викликаємо конвертацію
    convert_json_to_avro(raw_dir=raw_dir, stg_dir=stg_dir)
    
    return {
        "message": "Data converted successfully to Avro format",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)