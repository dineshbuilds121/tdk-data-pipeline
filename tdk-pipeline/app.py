"""
Unified TDK Data Pipeline Service
Combines:
  - Flask API for /ingest, /export, /health endpoints
  - APScheduler for nightly orchestration
"""

import os
import sys
import logging
import threading
import time

from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Add parent dir so we can import shared and service modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.db_config import get_connection
from ingest import ingest
from export import export

# ============================================================================
# Flask App Setup
# ============================================================================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").lower() in ("true", "1", "yes")
SCHEDULER_HOUR = int(os.getenv("SCHEDULER_HOUR", "0"))
SCHEDULER_MINUTE = int(os.getenv("SCHEDULER_MINUTE", "0"))

# ============================================================================
# Flask Routes
# ============================================================================

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    try:
        conn = get_connection()
        conn.close()
        return jsonify({
            "status": "healthy",
            "service": "tdk-pipeline",
            "database": "connected"
        }), 200
    except Exception as exc:
        logger.error("Health check failed: %s", exc)
        return jsonify({
            "status": "unhealthy",
            "service": "tdk-pipeline",
            "error": str(exc)
        }), 503


@app.route("/ingest", methods=["POST"])
def trigger_ingest():
    """
    Trigger full DSV → Oracle ingestion.
    Endpoint: POST /ingest
    """
    logger.info("=" * 60)
    logger.info("INGEST ENDPOINT TRIGGERED")
    logger.info("=" * 60)
    try:
        result = ingest()
        status_code = 200 if result["status"] == "success" else 500
        logger.info("Ingest result: %s", result)
        return jsonify(result), status_code
    except Exception as exc:
        logger.exception("Ingest endpoint error")
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/export", methods=["POST"])
def trigger_export():
    """
    Trigger Oracle → TSV export.
    Endpoint: POST /export
    """
    logger.info("=" * 60)
    logger.info("EXPORT ENDPOINT TRIGGERED")
    logger.info("=" * 60)
    try:
        result = export()
        status_code = 200 if result["status"] == "success" else 500
        logger.info("Export result: %s", result)
        return jsonify(result), status_code
    except Exception as exc:
        logger.exception("Export endpoint error")
        return jsonify({"status": "error", "message": str(exc)}), 500


# ============================================================================
# Scheduler & Orchestration
# ============================================================================

def run_full_pipeline():
    """
    Execute the full pipeline: ingest → export.
    Called by scheduler at midnight (or on startup).
    """
    logger.info("=" * 60)
    logger.info("FULL PIPELINE RUN STARTED")
    logger.info("=" * 60)

    # Step 1: Ingest DSV → Oracle
    logger.info("Step 1/2: Running data ingestion...")
    try:
        ingest_result = ingest()
        if ingest_result["status"] != "success":
            logger.error("Ingest failed: %s", ingest_result)
            return
        logger.info("Ingest succeeded: %s", ingest_result)
    except Exception as exc:
        logger.exception("Ingest failed with exception")
        return

    # Step 2: Export Oracle → TSV
    logger.info("Step 2/2: Running data export...")
    try:
        export_result = export()
        logger.info("Export result: %s", export_result)
    except Exception as exc:
        logger.exception("Export failed with exception")
        return

    logger.info("=" * 60)
    logger.info("FULL PIPELINE RUN COMPLETED")
    logger.info("=" * 60)


def init_scheduler():
    """
    Initialize APScheduler to run pipeline on cron schedule.
    Runs in background thread (non-blocking).
    """
    scheduler = BackgroundScheduler()
    
    # Schedule nightly run
    scheduler.add_job(
        run_full_pipeline,
        trigger=CronTrigger(hour=SCHEDULER_HOUR, minute=SCHEDULER_MINUTE),
        id="nightly_pipeline",
        name="Nightly Data Pipeline",
        replace_existing=True,
    )
    
    scheduler.start()
    logger.info(
        "Scheduler initialized – pipeline scheduled for %02d:%02d daily",
        SCHEDULER_HOUR,
        SCHEDULER_MINUTE
    )
    
    return scheduler


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    logger.info("TDK Data Pipeline Service starting...")
    logger.info("RUN_ON_STARTUP: %s", RUN_ON_STARTUP)
    
    # Initialize scheduler (background thread)
    scheduler = init_scheduler()
    
    # Run on startup if configured
    if RUN_ON_STARTUP:
        logger.info("RUN_ON_STARTUP=true – running pipeline immediately...")
        # Run in separate thread to avoid blocking Flask startup
        pipeline_thread = threading.Thread(target=run_full_pipeline, daemon=True)
        pipeline_thread.start()
    
    # Start Flask app on 0.0.0.0:5000
    logger.info("Starting Flask app on 0.0.0.0:5000...")
    app.run(host="0.0.0.0", port=5000, debug=False)
