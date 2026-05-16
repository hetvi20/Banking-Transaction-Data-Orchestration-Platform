"""
ingestion/transaction_ingestor.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pulls real-time transaction data from the Core Banking API,
validates records, and uploads to Azure Data Lake Bronze zone.

In production: replace mock_fetch_transactions() with real
API calls to your core banking system (Temenos, Finacle, etc.)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from config.settings import CORE_BANKING_API_URL, CORE_BANKING_API_KEY, BATCH_SIZE
from utils.azure_storage import lake
from utils.validators import validate_transactions, validate_row_count
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Simulated API response (replace with real API client) ────────────────────
def mock_fetch_transactions(
    from_timestamp: str,
    to_timestamp: str,
    batch_size: int = BATCH_SIZE,
) -> list[dict]:
    """
    MOCK: Simulates Core Banking API response.
    Production: Use requests.get(CORE_BANKING_API_URL, headers={"X-API-Key": CORE_BANKING_API_KEY})
    """
    transaction_types = ["debit", "credit", "transfer", "withdrawal", "deposit"]
    currencies        = ["USD", "EUR", "GBP", "USD", "USD"]  # USD weighted higher
    merchants         = [f"MERCH_{i:04d}" for i in range(1, 200)]
    accounts          = [f"ACC_{i:06d}" for i in range(1, 5000)]
    statuses          = ["completed"] * 90 + ["pending"] * 7 + ["failed"] * 3  # realistic distribution

    records = []
    base_time = datetime.fromisoformat(from_timestamp)

    for i in range(batch_size):
        amount = round(random.lognormvariate(4, 2), 2)  # Log-normal: realistic transaction amounts
        records.append({
            "transaction_id":   str(uuid.uuid4()),
            "account_id":       random.choice(accounts),
            "amount":           amount,
            "currency":         random.choice(currencies),
            "transaction_type": random.choice(transaction_types),
            "merchant_id":      random.choice(merchants),
            "timestamp":        (base_time + timedelta(seconds=i * 2)).isoformat(),
            "status":           random.choice(statuses),
            "region":           random.choice(["US-EAST", "US-WEST", "EU", "APAC", "LATAM"]),
            "channel":          random.choice(["mobile", "web", "atm", "branch", "api"]),
            "ip_address":       f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
            "device_id":        f"DEV_{random.randint(10000, 99999)}",
            "ingested_at":      datetime.utcnow().isoformat(),
        })

    return records


def fetch_transactions(
    from_timestamp: Optional[str] = None,
    to_timestamp: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch transactions from Core Banking API."""
    now = datetime.utcnow()
    to_ts   = to_timestamp   or now.isoformat()
    from_ts = from_timestamp or (now - timedelta(minutes=15)).isoformat()

    logger.info(f"Fetching transactions: {from_ts} → {to_ts}")

    try:
        # In production: replace with actual API call
        # response = requests.get(
        #     f"{CORE_BANKING_API_URL}/v1/transactions",
        #     headers={"X-API-Key": CORE_BANKING_API_KEY},
        #     params={"from": from_ts, "to": to_ts, "limit": BATCH_SIZE},
        #     timeout=30,
        # )
        # response.raise_for_status()
        # records = response.json()["data"]

        records = mock_fetch_transactions(from_ts, to_ts)
        df = pd.DataFrame(records)
        logger.info(f"Fetched {len(df)} transactions from API")
        return df

    except Exception as e:
        logger.error(f"Transaction fetch failed: {e}")
        raise


def ingest_transactions(partition_date: Optional[str] = None) -> dict:
    """
    Full ingestion step:
    1. Fetch from API
    2. Validate
    3. Upload to Azure Bronze zone
    Returns: ingestion summary
    """
    partition_date = partition_date or datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"Starting transaction ingestion for {partition_date}")

    # Step 1: Fetch
    df = fetch_transactions()

    # Step 2: Validate
    if not validate_row_count(df, min_rows=1):
        raise ValueError("Empty dataset returned from API")

    validation = validate_transactions(df)
    if not validation.passed:
        logger.error(f"Validation failed — {validation.error_count} bad rows")
        # Filter to valid rows only
        df = df.head(validation.valid_rows)

    # Step 3: Upload to Azure Data Lake (Bronze zone)
    path = lake.upload_dataframe(
        df,
        entity="transactions",
        zone="raw",
        partition_date=partition_date,
        file_format="parquet",
    )

    summary = {
        "entity":         "transactions",
        "partition_date": partition_date,
        "rows_fetched":   validation.total_rows,
        "rows_valid":     validation.valid_rows,
        "rows_rejected":  validation.error_count,
        "pass_rate":      f"{validation.pass_rate:.1%}",
        "azure_path":     path,
        "warnings":       validation.warnings,
    }

    logger.info(f"Ingestion complete: {summary}")
    return summary


if __name__ == "__main__":
    result = ingest_transactions()
    print(f"\n✅ Ingestion Result:")
    for k, v in result.items():
        print(f"   {k}: {v}")
