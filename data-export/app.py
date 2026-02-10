"""
Data Export Service – Flask API
Exposes a REST endpoint so the scheduler can trigger data export.
"""

import logging
from flask import Flask, jsonify
from export import export

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "data-export"}), 200


@app.route("/export", methods=["POST"])
def trigger_export():
    """Trigger Oracle → TSV export."""
    try:
        result = export()
        status_code = 200 if result["status"] == "success" else 500
        return jsonify(result), status_code
    except Exception as exc:
        logging.exception("Export endpoint error")
        return jsonify({"status": "error", "message": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
