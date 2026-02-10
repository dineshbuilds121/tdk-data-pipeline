"""
Data Export Service – Core Logic
Queries Oracle DB and writes the result to a TSV file.
"""

import os
import sys
import logging
from datetime import datetime

import pandas as pd

# Add parent dir so we can import the shared package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.db_config import get_connection

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "output"))
TABLE_NAME = "C_DUNS_V"


def export():
    """
    Query all data from C_DUNS_V and write to a TSV file.
    Returns dict with status and output file path.
    """
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    output_filename = f"{timestamp}_testOutput.txt"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    conn = get_connection()

    try:
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql(query, con=conn)

        row_count = len(df)
        if row_count == 0:
            logger.warning("Query returned 0 rows from %s", TABLE_NAME)
            return {
                "status": "warning",
                "message": f"No data found in {TABLE_NAME}",
                "rows": 0,
                "file": None,
            }

        # Write as TSV (tab-separated)
        df.to_csv(output_path, sep="\t", index=False)

        msg = f"Exported {row_count} rows to {output_path}"
        logger.info(msg)
        return {
            "status": "success",
            "message": msg,
            "rows": row_count,
            "file": output_filename,
        }

    except Exception as exc:
        logger.exception("Export failed")
        return {"status": "error", "message": str(exc), "rows": 0, "file": None}
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = export()
    print(result)
