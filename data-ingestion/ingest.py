"""
Data Ingestion Service – Core Logic
Parses the RAW DATA.dsv (pipe-delimited) file and inserts rows into Oracle DB.
"""

import os
import sys
import csv
import logging

# Add parent dir so we can import the shared package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.db_config import get_connection

logger = logging.getLogger(__name__)

INPUT_DIR = os.getenv("INPUT_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "input"))
DSV_FILENAME = os.getenv("DSV_FILENAME", "RAW DATA.dsv")
TABLE_NAME = "C_DUNS_V"


def parse_dsv(filepath=None):
    """
    Parse the pipe-delimited DSV file.
    Returns (rows, column_names).
    """
    if filepath is None:
        filepath = os.path.join(INPUT_DIR, DSV_FILENAME)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"DSV file not found: {filepath}")

    rows = []
    column_names = []

    with open(filepath, "r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="|", quotechar='"')
        for i, row in enumerate(reader):
            # Strip whitespace from each cell
            cleaned = [cell.strip() for cell in row]
            if i == 0:
                column_names = cleaned
            else:
                if any(cell != "" for cell in cleaned):  # skip completely empty rows
                    rows.append(cleaned)

    logger.info("Parsed %d data rows and %d columns from %s", len(rows), len(column_names), filepath)
    return rows, column_names


def _sanitize_col_name(name):
    """Make a column name safe for Oracle (alphanumeric + underscore, max 128 chars)."""
    sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if sanitized and sanitized[0].isdigit():
        sanitized = "C_" + sanitized
    return sanitized[:128].upper()


def create_table_if_not_exists(cursor, columns):
    """Create the target table if it does not already exist (all VARCHAR2 columns)."""
    safe_columns = [_sanitize_col_name(c) for c in columns]

    # Check if table exists
    cursor.execute(
        "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
        [TABLE_NAME.upper()],
    )
    (count,) = cursor.fetchone()

    if count == 0:
        col_defs = ", ".join(f'"{col}" VARCHAR2(4000)' for col in safe_columns)
        ddl = f'CREATE TABLE {TABLE_NAME} ({col_defs})'
        cursor.execute(ddl)
        logger.info("Created table %s with %d columns", TABLE_NAME, len(safe_columns))
    else:
        logger.info("Table %s already exists – skipping creation", TABLE_NAME)

    return safe_columns


def ingest(filepath=None):
    """
    Full ingestion pipeline:
    1. Parse DSV
    2. Connect to Oracle
    3. Create table if needed
    4. Truncate + bulk insert
    """
    rows, columns = parse_dsv(filepath)

    if not rows:
        logger.warning("No data rows found – nothing to ingest")
        return {"status": "warning", "message": "No data rows found", "rows": 0}

    conn = get_connection()
    cursor = conn.cursor()

    try:
        safe_columns = create_table_if_not_exists(cursor, columns)

        # Truncate existing data
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        logger.info("Truncated table %s", TABLE_NAME)

        # Prepare insert
        placeholders = ", ".join(f":{i+1}" for i in range(len(safe_columns)))
        col_list = ", ".join(f'"{c}"' for c in safe_columns)
        insert_sql = f'INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders})'

        # Pad or trim rows to match column count
        col_count = len(safe_columns)
        normalised = []
        for row in rows:
            if len(row) < col_count:
                row = row + [""] * (col_count - len(row))
            elif len(row) > col_count:
                row = row[:col_count]
            normalised.append(row)

        # Bulk insert
        cursor.executemany(insert_sql, normalised)
        conn.commit()

        msg = f"Successfully ingested {len(normalised)} rows into {TABLE_NAME}"
        logger.info(msg)
        return {"status": "success", "message": msg, "rows": len(normalised)}

    except Exception as exc:
        conn.rollback()
        logger.exception("Ingestion failed")
        return {"status": "error", "message": str(exc), "rows": 0}
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = ingest()
    print(result)
