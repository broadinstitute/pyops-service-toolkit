"""Module for interacting with Azure."""
import os
import logging
import base64
import re
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import unquote


class AzureBlobDetails:
    """Class to interact with with Azure Blobs."""

    def __init__(self, account_url: str, sas_token: str, container_name: str):
        """Initialize the AzureBlobDetails class.

        **Args:**
        - account_url (str): The Azure account URL
        - sas_token (str): The SAS token
        - container_name (str): The container name
        """
        from azure.storage.blob import BlobServiceClient
        self.account_url = account_url
        """@private"""
        self.sas_token = sas_token
        """@private"""
        self.container_name = container_name
        """@private"""
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url, credential=self.sas_token)
        """@private"""

    def get_blob_details(self, max_per_page: int = 500) -> list[dict]:
        """
        Get details about all Azure blobs within a container.

        **Args**:
        - max_per_page (int): The maximum number of blobs to return per page
        """
        container_client = self.blob_service_client.get_container_client(
            self.container_name)
        details = []

        blob_list = container_client.list_blobs(results_per_page=max_per_page)
        page = blob_list.by_page()

        page_count = 0
        for blob_page in page:
            page_count += 1
            logging.info(
                f"Getting page {page_count} of max {max_per_page} blobs")
            for blob in blob_page:
                blob_client = container_client.get_blob_client(blob)  # type: ignore[arg-type]
                props = blob_client.get_blob_properties()
                if not blob.name.endswith('/'):
                    md5_hash = base64.b64encode(props.content_settings.content_md5).decode(
                        'utf-8') if props.content_settings.content_md5 else ""
                    full_path = blob_client.url.replace(
                        f'?{self.sas_token}', '')
                    rel_path = full_path.replace(f"{self.account_url}/{self.container_name}/", '')
                    details.append(
                        {
                            'file_name': blob.name,
                            'file_path': full_path,
                            'relative_path': rel_path,
                            'content_type': props.content_settings.content_type,
                            'file_extension': os.path.splitext(blob.name)[1],
                            'size_in_bytes': props.size,
                            'md5_hash': md5_hash
                        }
                    )
        return details

    def download_blob(self, blob_name: str, dl_path: Path) -> None:
        """
        Download an Azure blob object.

        **Args:**
        - blob_name (str): The name of the blob to download
        - dl_path (Path): The path to download the blob to
        """
        blob_client = self.blob_service_client.get_blob_client(blob=blob_name, container=self.container_name)
        dl_path.parent.mkdir(parents=True, exist_ok=True)
        with dl_path.open(mode='wb') as file:
            blob_data = blob_client.download_blob()
            file.write(blob_data.readall())


class SasTokenUtil:
    """
    Class to obtain and manage Az SAS tokens.

    @private
    """

    def __init__(self, token: str):
        """Initialize the SasTokenUtil class."""
        self.token = token
        self.expiry_datetime = self._set_token_expiry()

    def _set_token_expiry(self) -> datetime:
        sas_expiry_time_pattern = re.compile(r"se.+?(?=\&sp)")
        expiry_time_str = sas_expiry_time_pattern.search(self.token)
        time_str = unquote(expiry_time_str.group()).replace("se=", "").replace("&sr=c", "")  # type: ignore[union-attr]
        return datetime.fromisoformat(time_str)

    def seconds_until_token_expires(self) -> int:
        """Get time until token expires."""
        current_time = datetime.now(timezone.utc)
        time_delta = self.expiry_datetime - current_time
        return time_delta.seconds
