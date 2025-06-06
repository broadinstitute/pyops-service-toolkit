"""Module for handling TDR interactions with BigQuery."""

from ..bq_utils import BigQueryUtil
from .tdr_api_utils import TDR

import logging
from typing import Optional, Any


class GetTdrAssetInfo:
    """Class to obtain TDR asset metadata (from dataset or snapshot) from BigQuery."""

    def __init__(self, tdr: TDR, dataset_id: Optional[str] = None, snapshot_id: Optional[str] = None):
        """
        Initialize the GetTdrAssetInfo class.

        **Args:**
        - tdr (`ops_utils.tdr_utils.tdr_api_utils.TDR`): TDR instance for interacting with the TDR API.
        - dataset_id (str, optional): ID of the dataset.
        - snapshot_id (str, optional): ID of the snapshot.
        """
        if not dataset_id and not snapshot_id:
            raise ValueError("Either dataset_id or snapshot_id must be provided.")
        self.tdr = tdr
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.snapshot_id = snapshot_id
        """@private"""

    def _get_dataset_info(self) -> dict:
        """
        Retrieve dataset information from TDR.

        **Returns:**
        - dict: A dictionary containing BigQuery project ID, schema, tables, and relationships.
        """
        dataset_info = self.tdr.get_dataset_info(
            dataset_id=self.dataset_id,  # type: ignore[arg-type]
            info_to_include=["SCHEMA", "ACCESS_INFORMATION"]
        ).json()
        return {
            "bq_project": dataset_info["accessInformation"]["bigQuery"]["projectId"],
            "bq_schema": dataset_info["accessInformation"]["bigQuery"]["datasetName"],
            "tables": dataset_info["schema"]["tables"],
            "relationships": dataset_info["schema"]["relationships"]
        }

    def _get_snapshot_info(self) -> Optional[dict]:
        """
        Retrieve snapshot information from TDR.

        **Returns:**
        - dict: A dictionary containing BigQuery project ID, schema, tables, and relationships.
        Returns None if the snapshot is not found or access is denied.
        """
        response = self.tdr.get_snapshot_info(
            snapshot_id=self.snapshot_id,  # type: ignore[arg-type]
            info_to_include=["TABLES", "RELATIONSHIPS", "ACCESS_INFORMATION"]
        )
        if response:
            snapshot_info = response.json()
            return {
                "bq_project": snapshot_info["accessInformation"]["bigQuery"]["projectId"],
                "bq_schema": snapshot_info["accessInformation"]["bigQuery"]["datasetName"],
                "tables": snapshot_info["tables"],
                "relationships": snapshot_info["relationships"]
            }
        return None

    def run(self) -> Optional[dict]:
        """
        Execute the process to retrieve either dataset or snapshot information.

        **Returns:**
        - dict (optional): A dictionary containing the relevant information based on whether `dataset_id` or
            `snapshot_id` is provided. Returns None if the snapshot is not found or access is denied.
        """
        if self.dataset_id:
            return self._get_dataset_info()
        return self._get_snapshot_info()


class TdrBq:
    """Class to interact with TDR BigQuery tables."""

    def __init__(self, project_id: str, bq_schema: str):
        """
        Initialize the TdrBq class.

        **Args:**
        - project_id (str): The Google Cloud project ID.
        - bq_schema (str): The BigQuery schema name.
        """
        self.project_id = project_id
        """@private"""
        self.bq_schema = bq_schema
        """@private"""
        self.bq_util = BigQueryUtil(project_id)
        """@private"""

    def check_permissions_for_dataset(self, raise_on_other_failure: bool) -> bool:
        """
        Check the permissions for accessing BigQuery for specific dataset.

        **Args:**
        - raise_on_other_failure (bool): Whether to raise an exception on other failures.

        **Returns:**
        - bool: `True` if permissions are sufficient, `False` otherwise.
        """
        query = f"""SELECT 1 FROM `{self.project_id}.{self.bq_schema}.INFORMATION_SCHEMA.TABLES`"""
        return self.bq_util.check_permissions_for_query(
            query=query,
            raise_on_other_failure=raise_on_other_failure
        )

    def get_tdr_table_contents(self, exclude_datarepo_id: bool, table_name: str, to_dataframe: bool) -> Any:
        """
        Retrieve the contents of a TDR table from BigQuery.

        **Args:**
        - exclude_datarepo_id (bool): Whether to exclude the datarepo_row_id column.
        - to_dataframe (bool): Whether to return the results as a DataFrame.

        **Returns:**
        - list[dict]: The contents of the table
        """
        if exclude_datarepo_id:
            exclude_str = "EXCEPT (datarepo_row_id)"
        else:
            exclude_str = ""
        query = f"""SELECT * {exclude_str} FROM `{self.project_id}.{self.bq_schema}.{table_name}`"""
        logging.info(f"Getting contents of table {table_name} from BQ")
        return self.bq_util.query_table(query=query, to_dataframe=to_dataframe)
