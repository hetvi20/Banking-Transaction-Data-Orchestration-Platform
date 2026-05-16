"""
utils/validators.py — Data validation for ingested records
Catches schema drift, nulls, and anomalies before they hit the warehouse.
"""
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    total_rows: int
    valid_rows: int
    error_count: int
    errors: list[str]
    warnings: list[str]

    @property
    def pass_rate(self) -> float:
        return self.valid_rows / self.total_rows if self.total_rows > 0 else 0.0


TRANSACTION_SCHEMA = {
    "transaction_id":   str,
    "account_id":       str,
    "amount":           float,
    "currency":         str,
    "transaction_type": str,
    "merchant_id":      str,
    "timestamp":        str,
    "status":           str,
}

REQUIRED_TRANSACTION_COLS = [
    "transaction_id", "account_id", "amount", "currency", "timestamp"
]

VALID_TRANSACTION_TYPES = ["debit", "credit", "transfer", "withdrawal", "deposit"]
VALID_CURRENCIES        = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]
VALID_STATUSES          = ["completed", "pending", "failed", "reversed"]


def validate_transactions(df: pd.DataFrame) -> ValidationResult:
    """Full validation suite for transaction records."""
    errors   = []
    warnings = []
    invalid_rows = set()

    # 1. Required columns presence
    missing_cols = [c for c in REQUIRED_TRANSACTION_COLS if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return ValidationResult(False, len(df), 0, len(df), errors, warnings)

    # 2. Null checks on required fields
    for col in REQUIRED_TRANSACTION_COLS:
        null_mask = df[col].isnull()
        if null_mask.any():
            count = null_mask.sum()
            errors.append(f"Column '{col}' has {count} null values")
            invalid_rows.update(df[null_mask].index.tolist())

    # 3. Duplicate transaction IDs
    dupes = df[df.duplicated("transaction_id", keep=False)]
    if not dupes.empty:
        errors.append(f"Found {len(dupes)} duplicate transaction_ids")
        invalid_rows.update(dupes.index.tolist())

    # 4. Amount validation
    if "amount" in df.columns:
        negative = df[df["amount"] < 0]
        if not negative.empty:
            errors.append(f"{len(negative)} transactions have negative amounts")
            invalid_rows.update(negative.index.tolist())

        zero_amounts = df[df["amount"] == 0]
        if not zero_amounts.empty:
            warnings.append(f"{len(zero_amounts)} transactions have zero amount")

        # Anomaly: unusually large transactions (>$1M)
        large = df[df["amount"] > 1_000_000]
        if not large.empty:
            warnings.append(f"{len(large)} transactions exceed $1M — verify legitimacy")

    # 5. Accepted values
    if "transaction_type" in df.columns:
        invalid_types = df[~df["transaction_type"].isin(VALID_TRANSACTION_TYPES)]
        if not invalid_types.empty:
            errors.append(f"{len(invalid_types)} rows have invalid transaction_type")
            invalid_rows.update(invalid_types.index.tolist())

    if "currency" in df.columns:
        invalid_curr = df[~df["currency"].isin(VALID_CURRENCIES)]
        if not invalid_curr.empty:
            warnings.append(f"{len(invalid_curr)} rows have unsupported currency codes")

    valid_rows = len(df) - len(invalid_rows)
    passed     = len(errors) == 0

    result = ValidationResult(
        passed=passed,
        total_rows=len(df),
        valid_rows=valid_rows,
        error_count=len(invalid_rows),
        errors=errors,
        warnings=warnings,
    )

    if passed:
        logger.info(f"Validation passed: {valid_rows}/{len(df)} rows valid")
    else:
        logger.error(f"Validation failed: {len(errors)} errors, {valid_rows}/{len(df)} rows valid")
        for e in errors:
            logger.error(f"  ✗ {e}")
    for w in warnings:
        logger.warning(f"  ⚠ {w}")

    return result


def validate_row_count(df: pd.DataFrame, min_rows: int = 1) -> bool:
    """Ensure minimum row count to detect empty/failed pulls."""
    if len(df) < min_rows:
        logger.error(f"Row count {len(df)} below minimum {min_rows} — possible API failure")
        return False
    return True
