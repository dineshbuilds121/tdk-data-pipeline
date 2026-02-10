"""
Scheduler Service – Orchestrator
Runs a cron job at midnight (00:00) to trigger:
  1. Data Ingestion (POST to data-ingestion service)
  2. Data Export  (POST to data-export service)
"""

import os
import logging
import time

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INGESTION_URL = os.getenv("INGESTION_URL", "http://data-ingestion:5001/ingest")
EXPORT_URL = os.getenv("EXPORT_URL", "http://data-export:5002/export")
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").lower() in ("true", "1", "yes")


def run_pipeline():
    """Execute the full pipeline: ingest then export."""
    logger.info("=" * 60)
    logger.info("PIPELINE RUN STARTED")
    logger.info("=" * 60)

    # Step 1: Ingest DSV → Oracle
    logger.info("Step 1/2: Triggering data ingestion...")
    try:
        resp = requests.post(INGESTION_URL, timeout=300)
        result = resp.json()
        logger.info("Ingestion result: %s (HTTP %d)", result.get("message", ""), resp.status_code)
        if resp.status_code != 200:
            logger.error("Ingestion failed – skipping export")
            return
    except Exception as exc:
        logger.error("Ingestion request failed: %s", exc)
        return

    # Step 2: Export Oracle → TSV
    logger.info("Step 2/2: Triggering data export...")
    try:
        resp = requests.post(EXPORT_URL, timeout=300)
        result = resp.json()
        logger.info("Export result: %s (HTTP %d)", result.get("message", ""), resp.status_code)
    except Exception as exc:
        logger.error("Export request failed: %s", exc)
        return

    logger.info("=" * 60)
    logger.info("PIPELINE RUN COMPLETED")
    logger.info("=" * 60)


def wait_for_services(max_retries=30, delay=2):
    """Wait until both downstream services are healthy."""
    services = {
        "data-ingestion": INGESTION_URL.replace("/ingest", "/health"),
        "data-export": EXPORT_URL.replace("/export", "/health"),
    }

    for name, url in services.items():
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    logger.info("✓ %s is healthy", name)
                    break
            except requests.ConnectionError:
                pass
            logger.info("Waiting for %s (attempt %d/%d)...", name, attempt, max_retries)
            time.sleep(delay)
        else:
            logger.warning("⚠ %s did not become healthy after %d attempts", name, max_retries)


def main():
    logger.info("Scheduler service starting...")
    logger.info("Ingestion URL: %s", INGESTION_URL)
    logger.info("Export URL:    %s", EXPORT_URL)

    # Wait for downstream services to be ready
    wait_for_services()

    # Run immediately on startup if configured
    if RUN_ON_STARTUP:
        logger.info("RUN_ON_STARTUP=true – running pipeline now...")
        run_pipeline()

    # Schedule nightly run at midnight
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger=CronTrigger(hour=0, minute=0),
        id="nightly_pipeline",
        name="Nightly Data Pipeline",
        replace_existing=True,
    )
    logger.info("Scheduled nightly pipeline run at 00:00")
    logger.info("Scheduler is running. Press Ctrl+C to exit.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down...")


if __name__ == "__main__":
    main()
