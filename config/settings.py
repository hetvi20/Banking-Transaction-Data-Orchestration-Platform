"""
config/settings.py — Central configuration for the banking ETL platform
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Azure ────────────────────────────────────────────────────────────────────
AZURE_STORAGE_ACCOUNT   = os.getenv("AZURE_STORAGE_ACCOUNT", "bankingdatalake")
AZURE_STORAGE_KEY        = os.getenv("AZURE_STORAGE_KEY", "")
AZURE_CONTAINER_NAME     = os.getenv("AZURE_CONTAINER_NAME", "transactions")
AZURE_TENANT_ID          = os.getenv("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID          = os.getenv("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET      = os.getenv("AZURE_CLIENT_SECRET", "")

# ── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mssql+pyodbc://user:pass@server/BankingDW?driver=ODBC+Driver+18+for+SQL+Server"
)

# ── Banking APIs ──────────────────────────────────────────────────────────────
CORE_BANKING_API_URL  = os.getenv("CORE_BANKING_API_URL", "https://api.corebanking.internal")
CORE_BANKING_API_KEY  = os.getenv("CORE_BANKING_API_KEY", "")
FRAUD_API_URL         = os.getenv("FRAUD_API_URL", "https://api.fraud-alerts.internal")
FRAUD_API_KEY         = os.getenv("FRAUD_API_KEY", "")
FX_RATE_API_URL       = os.getenv("FX_RATE_API_URL", "https://api.exchangeratesapi.io/v1")
FX_RATE_API_KEY       = os.getenv("FX_RATE_API_KEY", "")

# ── Prefect ───────────────────────────────────────────────────────────────────
PREFECT_API_URL       = os.getenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
SLACK_WEBHOOK_URL     = os.getenv("SLACK_WEBHOOK_URL", "")

# ── Pipeline ─────────────────────────────────────────────────────────────────
PIPELINE_SCHEDULE_MINUTES = 15        # Run every 15 minutes
BATCH_SIZE                = 5000      # Records per API call
FRAUD_SCORE_THRESHOLD     = 0.75      # Flag transactions above this score
LARGE_TXN_THRESHOLD       = 10_000   # USD — triggers compliance flag

# ── Data Lake Zones ───────────────────────────────────────────────────────────
BRONZE_ZONE = "raw"
SILVER_ZONE = "processed"
GOLD_ZONE   = "serving"
