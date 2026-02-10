"""
Shared Oracle Database Configuration Module.
Provides connection helper used by both data-ingestion and data-export services.
"""

import os
import logging

import oracledb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------
ORACLE_HOST = os.getenv("ORACLE_HOST", "oracle-db")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SID = os.getenv("ORACLE_SID", "XE")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "")
ORACLE_USER = os.getenv("ORACLE_USER", "pipeline_user")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "oracle123")
ORACLE_CLIENT_DIR = os.getenv("ORACLE_CLIENT_DIR", "")

# Use thick mode only when a client dir is explicitly supplied
_thick_initialised = False


def _ensure_thick_mode():
    """Initialise Oracle thick client once, if ORACLE_CLIENT_DIR is set."""
    global _thick_initialised
    if ORACLE_CLIENT_DIR and not _thick_initialised:
        try:
            oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_DIR)
            _thick_initialised = True
            logger.info("Oracle thick-mode initialised from %s", ORACLE_CLIENT_DIR)
        except Exception as exc:
            logger.warning("Could not init thick mode: %s – falling back to thin mode", exc)


def get_connection():
    """Return a new Oracle DB connection."""
    _ensure_thick_mode()

    # Prefer service_name if provided, otherwise use SID
    if ORACLE_SERVICE_NAME:
        dsn = oracledb.makedsn(host=ORACLE_HOST, port=ORACLE_PORT, service_name=ORACLE_SERVICE_NAME)
    else:
        dsn = oracledb.makedsn(host=ORACLE_HOST, port=ORACLE_PORT, service_name=ORACLE_SID)

    connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    logger.info("Connected to Oracle DB at %s:%s/%s", ORACLE_HOST, ORACLE_PORT, ORACLE_SID or ORACLE_SERVICE_NAME)
    return connection
