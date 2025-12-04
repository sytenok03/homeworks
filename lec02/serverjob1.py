"""Flask server for running sales data extraction job."""

import os

from flask import Flask, request, jsonify

from job import run_sales_job


app = Flask(__name__)


@app.route('/', methods=['POST'])  # ← ЗМІНЕНО: був '/run_job'
def run_job():
    """
    Endpoint to trigger the sales data extraction job.

    Expects JSON with 'date' and 'raw_dir' parameters.
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON data'
            }), 400

        date = data.get('date')
        raw_dir = data.get('raw_dir')

        if not date:
            return jsonify({
                'status': 'error',
                'message': 'Missing: date'
            }), 400
        if not raw_dir:
            return jsonify({
                'status': 'error',
                'message': 'Missing: raw_dir'
            }), 400

        auth_token = os.environ.get('AUTH_TOKEN')
        if not auth_token:
            return jsonify({
                'status': 'error',
                'message': 'AUTH_TOKEN not set'
            }), 500

        result = run_sales_job(
            date=date,
            raw_dir=raw_dir,
            auth_token=auth_token
        )

        return jsonify({
            'status': 'success',
            'message': 'Job completed',
            'details': result
        }), 201  # ← ЗМІНЕНО: був 200

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=True)