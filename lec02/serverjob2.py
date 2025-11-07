from flask import Flask, request, jsonify
from job2 import run_job

app = Flask(__name__)

@app.route('/', methods=['POST'])
def trigger_job():
    """API endpoint для запуску job2"""
    try:
        # Отримуємо параметри з JSON
        params = request.get_json()
        
        if not params:
            return jsonify({
                "status": "error",
                "message": "Не передано JSON параметри"
            }), 400
        
        raw_dir = params.get('raw_dir')
        stg_dir = params.get('stg_dir')
        
        # Валідація параметрів
        if not raw_dir or not stg_dir:
            return jsonify({
                "status": "error",
                "message": "Параметри raw_dir та stg_dir обов'язкові"
            }), 400
        
        # Запускаємо джобу
        result = run_job(raw_dir, stg_dir)
        
        return jsonify({
            "status": "success",
            "message": "JSON успішно конвертовано в Avro",
            "raw_dir": raw_dir,
            "stg_dir": stg_dir,
            "avro_file": result["avro_file"],
            "records_processed": result["records_processed"]
        }), 201  # <- ВАЖЛИВО: статус 201!
        
    except FileNotFoundError as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 404
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Помилка: {str(e)}"
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Перевірка стану сервісу"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, debug=True)