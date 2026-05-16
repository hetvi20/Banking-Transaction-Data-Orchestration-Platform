"""
ingestion/fraud_signal_ingestor.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pulls fraud signals from the bank's Fraud Alert API.
Each record contains a transaction_id and ML fraud score.
These are joined with transaction records in the dbt layer.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import uuid
import random
import pandas as pd
from datetime import datetime
from typing import Optional

from config.settings import FRAUD_API_URL, FRAUD_API_KEY, FRAUD_SCORE_THRESHOLD
from utils.azure_storage import lake
from utils.logger import get_logger

logger = get_logger(__name__)


def mock_fetch_fraud_signals(transaction_ids: list[str]) -> list[dict]:
    """MOCK: Fraud Alert API response with ML scores per transaction."""
    signals = []
    for txn_id in transaction_ids:
        score = random.betavariate(1, 8)  # Most scores near 0, few near 1
        signals.append({
            "transaction_id":     txn_id,
            "fraud_score":        round(score, 4),
            "is_flagged":         score >= FRAUD_SCORE_THRESHOLD,
            "fraud_reasons":      _get_fraud_reasons(score),
            "model_version":      "fraud-v2.3.1",
            "scored_at":          datetime.utcnow().isoformat(),
        })
    return signals


def _get_fraud_reasons(score: float) -> list[str]:
    """Return mock fraud reason codes based on score."""
    if score < 0.3:
        return []
    reasons = []
    if score > 0.5:
        reasons.append("velocity_check_failed")
    if score > 0.6:
        reasons.append("unusual_merchant_category")
    if score > 0.7:
        reasons.append("geo_anomaly")
    if score > 0.8:
        reasons.append("device_fingerprint_mismatch")
    if score > 0.9:
        reasons.append("known_fraud_network")
    return reasons


def ingest_fraud_signals(
    transaction_ids: Optional[list[str]] = None,
    partition_date: Optional[str] = None,
) -> dict:
    """Fetch fraud scores and upload to Azure Bronze zone."""
    partition_date = partition_date or datetime.utcnow().strftime("%Y-%m-%d")

    # Generate sample transaction IDs if none provided
    if not transaction_ids:
        transaction_ids = [str(uuid.uuid4()) for _ in range(500)]

    logger.info(f"Scoring {len(transaction_ids)} transactions for fraud signals")

    records = mock_fetch_fraud_signals(transaction_ids)
    df = pd.DataFrame(records)

    flagged_count = df["is_flagged"].sum()
    logger.info(f"Fraud scoring complete: {flagged_count}/{len(df)} flagged (score ≥ {FRAUD_SCORE_THRESHOLD})")

    path = lake.upload_dataframe(
        df,
        entity="fraud_signals",
        zone="raw",
        partition_date=partition_date,
    )

    return {
        "total_scored":    len(df),
        "flagged":         int(flagged_count),
        "flag_rate":       f"{flagged_count / len(df):.1%}",
        "azure_path":      path,
    }


if __name__ == "__main__":
    result = ingest_fraud_signals()
    print("\n✅ Fraud Signal Ingestion Result:")
    for k, v in result.items():
        print(f"   {k}: {v}")
