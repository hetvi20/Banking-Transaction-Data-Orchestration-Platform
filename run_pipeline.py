"""
run_pipeline.py — Manual pipeline trigger for testing
Usage:
    python run_pipeline.py                    # Full pipeline
    python run_pipeline.py --ingest-only      # Ingestion only
    python run_pipeline.py --dbt-only         # dbt transforms only
    python run_pipeline.py --date 2025-05-16  # Specific partition date
"""
import sys
import argparse
from datetime import datetime

sys.path.insert(0, ".")

from ingestion.transaction_ingestor import ingest_transactions
from ingestion.fraud_signal_ingestor import ingest_fraud_signals


def run_ingestion(partition_date: str) -> dict:
    print(f"\n📥 Running ingestion for {partition_date}...")
    txn    = ingest_transactions(partition_date)
    fraud  = ingest_fraud_signals(partition_date=partition_date)
    return {"transactions": txn, "fraud_signals": fraud}


def run_dbt(select: str = None) -> None:
    import subprocess
    cmd = ["dbt", "run"]
    if select:
        cmd += ["--select", select]
    print(f"\n⚙️  Running: {' '.join(cmd)}")
    # subprocess.run(cmd, cwd="transformation/", check=True)
    print("   [dbt would run here in production]")


def main():
    parser = argparse.ArgumentParser(description="Banking ETL Pipeline Runner")
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--dbt-only",    action="store_true")
    parser.add_argument("--date",        type=str, default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    print(f"\n🏦 Banking ETL Pipeline")
    print(f"   Partition date: {args.date}")
    print("=" * 50)

    if args.dbt_only:
        run_dbt()
        return

    results = run_ingestion(args.date)

    print(f"\n📊 Ingestion Summary:")
    t = results["transactions"]
    f = results["fraud_signals"]
    print(f"   Transactions : {t.get('rows_valid', 0):,} valid rows")
    print(f"   Fraud signals: {f.get('total_scored', 0):,} scored, {f.get('flagged', 0)} flagged")
    print(f"   Azure path   : {t.get('azure_path', 'N/A')}")

    if not args.ingest_only:
        run_dbt()
        print("\n✅ Full pipeline complete!")


if __name__ == "__main__":
    main()
