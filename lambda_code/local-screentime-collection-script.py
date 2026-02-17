"""Script for sending macbook screentime data to Amazon S3."""

import json
import logging
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

# --- Configuration ---
LAMBDA_URL = os.getenv("SCREENTIME_LAMBDA_ENDPOINT")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
STATE_FILE = Path("~/screen_time_job/.screen_time_state.json").expanduser()
DB_PATH = Path("~/Library/Application Support/Knowledge/knowledgeC.db").expanduser()
DEVICE_NAME = "Doug's MacBook Pro"

# --- Partitioned Logging Setup ---
SCRIPT_DIR = Path(__file__).parent
TODAY_STR = datetime.now(UTC).date().isoformat()
LOG_DIR = SCRIPT_DIR.parent / "logs" / TODAY_STR
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "screen_time_scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_hardware_uuid() -> str:
    """
    Generate a consistent unique ID for this specific Mac.

    Returns
    -------
    str
        The hardware UUID formatted as a colon-separated string.
    """
    node = uuid.getnode()
    return "-".join(f"{(node >> i) & 0xFF:02X}" for i in range(0, 48, 8)[::-1])


def get_processed_dates() -> set[str]:
    """
    Load the manifest of dates already pushed to S3.

    Returns
    -------
    set[str]
        A set of date strings (ISO format) that have been successfully processed.
    """
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                return set(json.load(f).get("processed_dates", []))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("⚠️ State file corrupt, starting fresh: %s", e)
    return set()


def save_processed_date(date_str: str) -> None:
    """
    Log a successful transmission to the local state file.

    Parameters
    ----------
    date_str : str
        The ISO format date string to mark as processed.
    """
    processed = list(get_processed_dates())
    if date_str not in processed:
        processed.append(date_str)
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump({"processed_dates": sorted(processed)}, f)


def query_database_for_date(target_date_str: str) -> list[dict[str, Any]] | None:
    """
    Query the forensics database for a specific local calendar day.

    Parameters
    ----------
    target_date_str : str
        The target date in 'YYYY-MM-DD' format.

    Returns
    -------
    list[dict[str, Any]] | None
        A list of dictionaries containing usage data, or None if an error occurs.
    """
    temp_dir = tempfile.gettempdir()
    temp_db_path = Path(temp_dir) / "knowledgeC_temp.db"

    try:
        shutil.copy2(DB_PATH, temp_db_path)
        with sqlite3.connect(temp_db_path) as con:
            cur = con.cursor()
            query = """
            SELECT
                ZVALUESTRING AS "bundle_id",
                COALESCE(ZSYNCPEER.ZMODEL, 'Mac') AS "device_model",
                DATE(ZOBJECT.ZSTARTDATE + 978307200, 'unixepoch', 'localtime') AS "usage_date_local",
                SUM(ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "total_usage_seconds",
                ROUND(SUM(ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) / 60.0, 2) AS "total_usage_minutes"
            FROM
                ZOBJECT
            LEFT JOIN ZSOURCE ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK
            LEFT JOIN ZSYNCPEER ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
            WHERE
                ZSTREAMNAME = "/app/usage"
                AND usage_date_local = ?
            GROUP BY 1, 2, 3
            HAVING total_usage_seconds > 0;
            """
            cur.execute(query, (target_date_str,))
            columns = [column[0] for column in cur.description]
            return [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]
    except (sqlite3.Error, OSError):
        logger.exception("❌ SQL Error for %s", target_date_str)
        return None
    finally:
        if temp_db_path.exists():
            temp_db_path.unlink()


def send_to_lambda(usage_data: list[dict[str, Any]], target_date_str: str) -> bool:
    """
    Send aggregated data to the AWS Lambda endpoint.

    Parameters
    ----------
    usage_data : list[dict[str, Any]]
        The list of app usage records.
    target_date_str : str
        The date the data belongs to.

    Returns
    -------
    bool
        True if the request was successful, False otherwise.
    """
    payload = {
        "device_type": "mac",
        "device_name": DEVICE_NAME,
        "device_id": get_hardware_uuid(),
        "updated_at": datetime.now(UTC).isoformat(),
        "usage_date": target_date_str,
        "data": usage_data,
    }

    headers = {"Authorization": f"Bearer {AUTH_TOKEN}", "Content-Type": "application/json"}

    try:
        response = requests.post(LAMBDA_URL, json=payload, headers=headers, timeout=30)
        logger.info("📡 Lambda Response (%s) [%s]: %s", target_date_str, response.status_code, response.text)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException:
        logger.exception("❌ Lambda push failed for %s", target_date_str)
        return False


def main() -> None:
    """Execute the primary ETL logic for the last 30 days."""
    logger.info("🚀 Starting Screen Time ETL (Partitioned Logging)")

    today = datetime.now(UTC).date()
    lookback_window = [(today - timedelta(days=i)).isoformat() for i in range(1, 31)]

    processed = get_processed_dates()
    missing_dates = [d for d in lookback_window if d not in processed]

    if not missing_dates:
        logger.info("✅ All dates within the 30-day window are already processed.")
        return

    for target_date in sorted(missing_dates):
        logger.info("🔄 Processing: %s", target_date)
        data = query_database_for_date(target_date)

        if data is not None:
            if data:
                if send_to_lambda(data, target_date):
                    save_processed_date(target_date)
                    logger.info("   Successfully pushed %s records for %s.", len(data), target_date)
            else:
                logger.info("   No usage found for %s. Marking as processed.", target_date)
                save_processed_date(target_date)


if __name__ == "__main__":
    main()
