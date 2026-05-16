"""
utils/azure_storage.py — Azure Data Lake Storage Gen2 helper
Handles Bronze / Silver / Gold zone uploads and downloads.
"""
import json
import io
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET,
    BRONZE_ZONE, SILVER_ZONE, GOLD_ZONE,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class AzureDataLakeClient:
    """Client for Azure Data Lake Storage Gen2 operations."""

    def __init__(self):
        if not AZURE_AVAILABLE:
            logger.warning("Azure SDK not installed. Using mock mode.")
            self._client = None
            return

        if AZURE_TENANT_ID:
            credential = ClientSecretCredential(
                tenant_id=AZURE_TENANT_ID,
                client_id=AZURE_CLIENT_ID,
                client_secret=AZURE_CLIENT_SECRET,
            )
        else:
            credential = AZURE_STORAGE_KEY

        self._client = DataLakeServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
            credential=credential,
        )
        logger.info("Azure Data Lake client initialized")

    def _get_zone_path(self, zone: str, entity: str, partition_date: str) -> str:
        """Build partitioned path: zone/entity/year=YYYY/month=MM/day=DD/"""
        dt = datetime.strptime(partition_date, "%Y-%m-%d")
        return f"{zone}/{entity}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        entity: str,
        zone: str = BRONZE_ZONE,
        partition_date: str = None,
        file_format: str = "parquet",
    ) -> str:
        """Upload a DataFrame to Azure Data Lake in the specified zone."""
        partition_date = partition_date or datetime.now().strftime("%Y-%m-%d")
        folder_path = self._get_zone_path(zone, entity, partition_date)
        timestamp = datetime.now().strftime("%H%M%S")
        file_name = f"{entity}_{timestamp}.{file_format}"
        full_path = f"{folder_path}/{file_name}"

        if self._client is None:
            logger.info(f"[MOCK] Would upload {len(df)} rows to: {full_path}")
            return full_path

        # Convert to bytes
        if file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            data = buffer.getvalue()
        else:
            data = df.to_csv(index=False).encode()

        # Upload to ADLS
        file_system = self._client.get_file_system_client(AZURE_STORAGE_ACCOUNT)
        directory = file_system.get_directory_client(folder_path)
        directory.create_directory()
        file_client = directory.create_file(file_name)
        file_client.upload_data(data, overwrite=True)

        logger.info(f"Uploaded {len(df)} rows → {full_path}")
        return full_path

    def read_dataframe(self, path: str) -> pd.DataFrame:
        """Read a parquet file from Azure Data Lake."""
        if self._client is None:
            logger.info(f"[MOCK] Would read from: {path}")
            return pd.DataFrame()

        parts = path.split("/", 1)
        file_system_name, file_path = parts[0], parts[1]
        file_system = self._client.get_file_system_client(file_system_name)
        file_client = file_system.get_file_client(file_path)
        download = file_client.download_file()
        buffer = io.BytesIO(download.readall())
        return pd.read_parquet(buffer)


# Singleton instance
lake = AzureDataLakeClient()
