"""Classes and functions for ingesting data into TDR."""

import json
import logging
import sys
import pytz
from datetime import datetime
import math
from typing import Optional, Any
from dateutil import parser

from ..vars import ARG_DEFAULTS

from .tdr_api_utils import TDR, FilterOutSampleIdsAlreadyInDataset
from .tdr_job_utils import MonitorTDRJob
from ..terra_util import TerraWorkspace


class BatchIngest:
    """A class to handle batch ingestion of metadata into TDR (Terra Data Repository)."""

    def __init__(
            self,
            ingest_metadata: list[dict],
            tdr: TDR,
            target_table_name: str,
            dataset_id: str,
            batch_size: int,
            bulk_mode: bool,
            update_strategy: str = "replace",
            waiting_time_to_poll: int = ARG_DEFAULTS["waiting_time_to_poll"],  # type: ignore[assignment]
            test_ingest: bool = False,
            load_tag: Optional[str] = None,
            file_to_uuid_dict: Optional[dict] = None,
            schema_info: Optional[dict] = None,
            skip_reformat: bool = False
    ):
        """
        Initialize the BatchIngest class.

        **Args:**
        - ingest_metadata (list[dict]): The metadata to be ingested.
        - tdr (`ops_utils.tdr_utils.tdr_api_utils.TDR`): An instance of the TDR class.
        - target_table_name (str): The name of the target table.
        - dataset_id (str): The ID of the dataset.
        - batch_size (int): The size of each batch for ingestion.
        - bulk_mode (bool): Flag indicating if bulk mode should be used.
        - update_strategy (str, optional): The strategy for updating existing records. Defaults to `replace`.
        - waiting_time_to_poll (int, optional): The time to wait between polling for job status. Defaults to `90`.
        - test_ingest (bool, optional): Flag indicating if only the first batch should be
                ingested for testing. Defaults to `False`.
        - load_tag (str, optional): A tag to identify the load. Used so future ingests
                can pick up where left off. Defaults to None.
        - file_to_uuid_dict (dict, optional): Only useful for self-hosted dataset. Can get from
            create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset. A dictionary mapping
            source file paths to UUIDs. If used will make ingest much quicker since no ingest
            or look up of file needed. Defaults to None.
        - schema_info (dict, optional): Schema information for the tables. Validates ingest data matches up
                with schema info. Defaults to None.
        - skip_reformat (bool, optional): Flag indicating if reformatting should be skipped. Defaults to `False`.
        """
        self.ingest_metadata = self._reformat_for_type_consistency(ingest_metadata)
        """@private"""
        self.tdr = tdr
        """@private"""
        self.target_table_name = target_table_name
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.batch_size = int(batch_size)
        """@private"""
        self.update_strategy = update_strategy
        """@private"""
        self.bulk_mode = bulk_mode
        """@private"""
        self.waiting_time_to_poll = waiting_time_to_poll
        """@private"""
        # Used if you want to run first batch and then exit after success
        self.test_ingest = test_ingest
        """@private"""
        self.load_tag = load_tag
        """@private"""
        self.file_to_uuid_dict = file_to_uuid_dict
        """@private"""
        # Used if you want to provide schema info for tables to make sure values match.
        # Should be dict with key being column name and value being dict with datatype
        self.schema_info = schema_info
        """@private"""
        # Use if input is already formatted correctly for ingest
        self.skip_reformat = skip_reformat
        """@private"""

    @staticmethod
    def _reformat_for_type_consistency(ingest_metadata: list[dict]) -> list[dict]:
        """
        Reformats ingest metadata and finds headers where values are a mix of lists and non-lists.

        If there is mix of these types of values, it converts the non-array to a one-item list. The updated metadata
        is then returned to be used for everything downstream
        """
        unique_headers = sorted({key for item in ingest_metadata for key in item.keys()})

        headers_containing_mismatch = []
        for header in unique_headers:
            all_values_for_header = [r.get(header) for r in ingest_metadata]
            # Find headers where some values are lists and some are not (while filtering out None values)
            if any(isinstance(value, list) for value in all_values_for_header if value is not None) and not all(
                    isinstance(value, list) for value in all_values_for_header if value is not None):
                logging.info(
                    f"Header {header} contains lists and non-list items. Will convert the non-list items into a list"
                )
                headers_containing_mismatch.append(header)

        updated_metadata = []
        for record in ingest_metadata:
            new_record = {}
            for header, value in record.items():
                if header in headers_containing_mismatch:
                    updated_value = [value] if not isinstance(value, list) else value
                else:
                    updated_value = value
                new_record[header] = updated_value
            updated_metadata.append(new_record)

        return updated_metadata

    def run(self) -> None:
        """Run the batch ingestion process."""
        logging.info(
            f"Batching {len(self.ingest_metadata)} total rows into batches of {self.batch_size} for ingest")
        total_batches = math.ceil(len(self.ingest_metadata) / self.batch_size)
        for i in range(0, len(self.ingest_metadata), self.batch_size):
            batch_number = i // self.batch_size + 1
            logging.info(f"Starting ingest batch {batch_number} of {total_batches} into table {self.target_table_name}")
            metrics_batch = self.ingest_metadata[i:i + self.batch_size]
            if self.skip_reformat:
                reformatted_batch = metrics_batch
            else:
                reformatted_batch = ReformatMetricsForIngest(
                    ingest_metadata=metrics_batch,
                    file_to_uuid_dict=self.file_to_uuid_dict,
                    schema_info=self.schema_info
                ).run()

            if self.load_tag:
                load_tag = self.load_tag
            else:
                load_tag = f"{self.dataset_id}.{self.target_table_name}"
            # Start actual ingest
            if reformatted_batch:
                StartAndMonitorIngest(
                    tdr=self.tdr,
                    ingest_records=reformatted_batch,
                    target_table_name=self.target_table_name,
                    dataset_id=self.dataset_id,
                    load_tag=load_tag,
                    bulk_mode=self.bulk_mode,
                    update_strategy=self.update_strategy,
                    waiting_time_to_poll=self.waiting_time_to_poll
                ).run()
                logging.info(f"Completed batch ingest of {len(reformatted_batch)} rows")
                if self.test_ingest:
                    logging.info("First batch completed, exiting since test_ingest was used")
                    sys.exit(0)
            else:
                logging.info("No rows to ingest in this batch after reformatting")
        logging.info("Whole Ingest completed")


class StartAndMonitorIngest:
    """Class to start and monitor the ingestion of recordsinto a TDR dataset."""

    def __init__(
            self, tdr: TDR,
            ingest_records: list[dict],
            target_table_name: str,
            dataset_id: str,
            load_tag: str,
            bulk_mode: bool,
            update_strategy: str,
            waiting_time_to_poll: int
    ):
        """
        Initialize the StartAndMonitorIngest.

        **Args:**
        - tdr (`ops_utils.tdr_utils.tdr_api_utils.TDR`): An instance of the TDR class.
        - ingest_records (list[dict]): The records to be ingested.
        - target_table_name (str): The name of the target table.
        - dataset_id (str): The ID of the dataset.
        - load_tag (str): A tag to identify the load.
        - bulk_mode (bool): Flag indicating if bulk mode should be used.
        - update_strategy (str): The strategy for updating existing records.
        - waiting_time_to_poll (int): The time to wait between polling for job status.
        """
        self.tdr = tdr
        """@private"""
        self.ingest_records = ingest_records
        """@private"""
        self.target_table_name = target_table_name
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.load_tag = load_tag
        """@private"""
        self.bulk_mode = bulk_mode
        """@private"""
        self.update_strategy = update_strategy
        """@private"""
        self.waiting_time_to_poll = waiting_time_to_poll
        """@private"""

    def _create_ingest_dataset_request(self) -> Any:
        """
        Create the ingestDataset request body.

        Returns:
            Any: The request body for ingesting the dataset.
        """
        # https://support.terra.bio/hc/en-us/articles/23460453585819-How-to-ingest-and-update-TDR-data-with-APIs
        load_dict = {
            "format": "array",
            "records": self.ingest_records,
            "table": self.target_table_name,
            "resolve_existing_files": "true",
            "updateStrategy": self.update_strategy,
            "load_tag": self.load_tag,
            "bulkMode": "true" if self.bulk_mode else "false"
        }
        return json.dumps(load_dict)  # dict -> json

    def run(self) -> None:
        """Run the ingestion process and monitor the job until completion."""
        ingest_request = self._create_ingest_dataset_request()
        logging.info(f"Starting ingest to {self.dataset_id}")
        ingest_response = self.tdr.ingest_to_dataset(dataset_id=self.dataset_id, data=ingest_request).json()
        MonitorTDRJob(
            tdr=self.tdr,
            job_id=ingest_response["id"],
            check_interval=self.waiting_time_to_poll,
            return_json=False
        ).run()


class ReformatMetricsForIngest:
    """A class to reformat metrics for ingestion into TDR (Terra Data Repository)."""

    def __init__(
            self,
            ingest_metadata: list[dict],
            file_to_uuid_dict: Optional[dict] = None,
            schema_info: Optional[dict] = None
    ):
        """
        Initialize the ReformatMetricsForIngest class.

        This class is used to reformat metrics for ingest.
        Assumes input JSON will be in the following format for GCP:
        ```
        {
            "file_name": blob.name,
            "file_path": f"gs://{self.bucket_name}/{blob.name}",
            "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
            "file_extension": os.path.splitext(blob.name)[1],
            "size_in_bytes": blob.size,
            "md5_hash": blob.md5_hash
        }
        ```

        **Args:**
        - ingest_metadata (list[dict]): The metadata to be ingested.
        - file_to_uuid_dict (dict, optional): Only useful for self-hosted dataset. Can get from
            create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset. A dictionary mapping
            source file paths to UUIDs. If used will make ingest much quicker since no ingest
            or look up of file needed. Defaults to None.
        - schema_info (dict, optional): Schema information for the tables. Defaults to None.
        """
        self.ingest_metadata = ingest_metadata
        """@private"""
        self.file_prefix = "gs://"
        """@private"""
        self.file_to_uuid_dict = file_to_uuid_dict
        """@private"""
        self.schema_info = schema_info
        """@private"""

    def _add_file_ref(self, file_details: dict) -> None:
        """
        Create file ref for ingest.

        Args:
            file_details (dict): The details of the file to be ingested.
        """
        file_details["file_ref"] = {
            "sourcePath": file_details["path"],
            "targetPath": self._format_relative_tdr_path(file_details["path"]),
            "description": f"Ingest of {file_details['path']}",
            "mimeType": file_details["content_type"]
        }

    @staticmethod
    def _format_relative_tdr_path(cloud_path: str) -> str:
        """
        Format cloud path to TDR path.

        Args:
            cloud_path (str): The cloud path to be formatted.

        Returns:
            str: The formatted TDR path.
        """
        relative_path = "/".join(cloud_path.split("/")[3:])
        return f"/{relative_path}"

    def _check_and_format_file_path(self, column_value: str) -> tuple[Any, bool]:
        """
        Check if column value is a gs:// path and reformat to dict with ingest information.

        If file_to_uuid_dict is
        provided then it will add existing UUID. If file_to_uuid_dict provided and file not found then will warn and
        return None.

        Args:
            column_value (str): The column value to be checked and formatted.

        Returns:
            tuple[Any, bool]: The formatted column value and a validity flag.
        """
        valid = True
        if isinstance(column_value, str):
            if column_value.startswith(self.file_prefix):
                if self.file_to_uuid_dict:
                    uuid = self.file_to_uuid_dict.get(column_value)
                    if uuid:
                        column_value = uuid
                        return column_value, valid
                    else:
                        logging.warning(
                            f"File {column_value} not found in file_to_uuid_dict, will attempt "
                            "to ingest as regular file and not use UUID"
                        )
                source_dest_mapping = {
                    "sourcePath": column_value,
                    "targetPath": self._format_relative_tdr_path(column_value)
                }
                return source_dest_mapping, valid
        return column_value, valid

    def _validate_and_update_column_for_schema(self, column_name: str, column_value: Any) -> tuple[str, bool]:
        """
        Check if column matches what schema expects and attempt to update if not. Changes to string at the end.

        Args:
            column_name (str): The name of the column.
            column_value (Any): The value of the column.

        Returns:
            tuple[str, bool]: The validated and updated column value and a validity flag.
        """
        valid = True
        if self.schema_info:
            if column_name in self.schema_info.keys():
                expected_data_type = self.schema_info[column_name]["datatype"]
                if expected_data_type == "string" and not isinstance(column_value, str):
                    try:
                        column_value = str(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a string")
                        valid = False
                if expected_data_type in ["int64", "integer"] and not isinstance(column_value, int):
                    try:
                        column_value = int(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not an integer")
                        valid = False
                if expected_data_type == "float64" and not isinstance(column_value, float):
                    try:
                        column_value = float(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a float")
                        valid = False
                if expected_data_type == "boolean" and not isinstance(column_value, bool):
                    try:
                        column_value = bool(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a boolean")
                        valid = False
                if expected_data_type in ["datetime", "date", "time"] and not isinstance(column_value, datetime):
                    try:
                        column_value = parser.parse(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a datetime")
                        valid = False
                if expected_data_type == "array" and not isinstance(column_value, list):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not a list")
                if expected_data_type == "bytes" and not isinstance(column_value, bytes):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not bytes")
                if expected_data_type == "fileref" and not column_value.startswith(self.file_prefix):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not a file path")
        return str(column_value), valid

    def _reformat_metric(self, row_dict: dict) -> Optional[dict]:
        """
        Reformat metric for ingest.

        Args:
            row_dict (dict): The row dictionary to be reformatted.

        Returns:
            Optional[dict]: The reformatted row dictionary or None if invalid.
        """
        reformatted_dict = {}
        row_valid = True
        for key, value in row_dict.items():
            if value or value == 0:
                if self.schema_info:
                    value, valid = self._validate_and_update_column_for_schema(key, value)
                    if not valid:
                        row_valid = False
                if isinstance(value, list):
                    updated_value_list = []
                    for item in value:
                        update_value, valid = self._check_and_format_file_path(item)
                        if not valid:
                            row_valid = False
                        updated_value_list.append(update_value)
                    reformatted_dict[key] = updated_value_list
                else:
                    update_value, valid = self._check_and_format_file_path(value)
                    reformatted_dict[key] = update_value
                    if not valid:
                        row_valid = False
        reformatted_dict["last_modified_date"] = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")  # type: ignore[assignment] # noqa: E501
        if row_valid:
            return reformatted_dict
        else:
            logging.info(f"Row {json.dumps(row_dict, indent=4)} not valid and will not be included in ingest")
            return None

    def run(self) -> list[dict]:
        """
        Run the reformatting process for all metrics.

        **Returns:**
        - list[dict]: A list of reformatted metrics.
        """
        reformatted_metrics = []
        for row_dict in self.ingest_metadata:
            reformatted_row = self._reformat_metric(row_dict)
            if reformatted_row:
                reformatted_metrics.append(reformatted_row)
        return reformatted_metrics


class ConvertTerraTableInfoForIngest:
    """Converts each row of table metadata into a dictionary that can be ingested into TDR."""

    def __init__(
            self,
            table_metadata: list[dict],
            columns_to_ignore: list[str] = [],
            tdr_row_id: Optional[str] = None
    ):
        """
        Initialize the ConvertTerraTableInfoForIngest class.

        Input will look like this:
        ```
            [{
              "attributes": {
                "some_metric": 99.99,
                "some_file_path": "gs://path/to/file",
                "something_to_exclude": "exclude_me"
              },
              "entityType": "sample",
              "name": "SM-MVVVV"
            }]
        ```
        And be converted to this:
        ```
            [{
              "sample_id": "SM-MVVVV",
              "some_metric": 99.99,
              "some_file_path": "gs://path/to/file"
            }]
        ```
        **Args:**
        - table_metadata (list[dict]): The metadata of the table to be converted.
        - tdr_row_id (str): The row ID to be used in the TDR. Defaults to {entityType}_id.
        - columns_to_ignore (list[str]): List of columns to ignore during conversion. Defaults to an empty list.
        """
        self.table_metadata = table_metadata
        """@private"""
        if table_metadata:
            self.tdr_row_id = tdr_row_id if tdr_row_id else f'{table_metadata[0]["entityType"]}_id'
            """@private"""
        else:
            # Won't be used if table_metadata is empty but will be set to empty string
            self.tdr_row_id = ""
            """@private"""
        self.columns_to_ignore = columns_to_ignore
        """@private"""

    def run(self) -> list[dict]:
        """
        Convert the table metadata into a format suitable for TDR ingestion.

        **Returns:**
        - list[dict]: A list of dictionaries containing the converted table metadata.
        """
        return [
            {
                self.tdr_row_id: row["name"],
                **{k: v for k, v in row["attributes"].items()
                   # if columns_to_ignore is not provided or if the column is not in the columns_to_ignore list
                   if k not in self.columns_to_ignore}
            }
            for row in self.table_metadata
        ]


class FilterAndBatchIngest:
    """Filter and batch ingest process into TDR."""

    def __init__(
            self,
            tdr: TDR,
            filter_existing_ids: bool,
            unique_id_field: str,
            table_name: str,
            ingest_metadata: list[dict],
            dataset_id: str,
            ingest_waiting_time_poll: int,
            ingest_batch_size: int,
            bulk_mode: bool,
            update_strategy: str,
            load_tag: str,
            test_ingest: bool = False,
            file_to_uuid_dict: Optional[dict] = None,
            schema_info: Optional[dict] = None,
            skip_reformat: bool = False
    ):
        """
        Initialize the FilterAndBatchIngest class.

        **Args:**
        - tdr (`ops_utils.tdr_utils.tdr_api_utils.TDR`): An instance of the TDR class.
        - filter_existing_ids (bool): Whether to filter out sample IDs that already exist in the dataset.
        - unique_id_field (str): The unique ID field to filter on.
        - table_name (str): The name of the table to ingest data into.
        - ingest_metadata (list[dict]): The metadata to ingest.
        - dataset_id (str): The ID of the dataset.
        - ingest_waiting_time_poll (int): The waiting time to poll for ingest status.
        - ingest_batch_size (int): The batch size for ingest.
        - bulk_mode (bool): Whether to use bulk mode for ingest.
        - update_strategy (str): The update strategy to use.
        - load_tag (str): The load tag for ingest. Used to make future ingests of the same files go faster.
        - test_ingest (bool, optional): Whether to run a test ingest. Defaults to False.
        - file_to_uuid_dict (dict, optional): Only useful for self-hosted dataset. Can get from
            create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset. A dictionary mapping
            source file paths to UUIDs. If used will make ingest much quicker since no ingest
            or look up of file needed. Defaults to None.
        - schema_info (dict, optional): Schema information for the tables.
                Used to validate ingest metrics match. Defaults to None.
        - skip_reformat (bool, optional): Whether to skip reformatting of metrics. Defaults to False.
        """
        self.tdr = tdr
        """@private"""
        self.filter_existing_ids = filter_existing_ids
        """@private"""
        self.unique_id_field = unique_id_field
        """@private"""
        self.table_name = table_name
        """@private"""
        self.ingest_metadata = ingest_metadata
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.ingest_waiting_time_poll = ingest_waiting_time_poll
        """@private"""
        self.ingest_batch_size = ingest_batch_size
        """@private"""
        self.bulk_mode = bulk_mode
        """@private"""
        self.update_strategy = update_strategy
        """@private"""
        self.load_tag = load_tag
        """@private"""
        self.test_ingest = test_ingest
        """@private"""
        self.file_to_uuid_dict = file_to_uuid_dict
        """@private"""
        self.schema_info = schema_info
        """@private"""
        self.skip_reformat = skip_reformat
        """@private"""

    def run(self) -> None:
        """
        Run the filter and batch ingest process.

        This method filters out sample IDs that already exist in the dataset (if specified),
        and then performs a batch ingest of the remaining metadata into the specified table.
        """
        if self.filter_existing_ids:
            # Filter out sample ids that are already in the dataset
            filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
                ingest_metrics=self.ingest_metadata,
                dataset_id=self.dataset_id,
                tdr=self.tdr,
                target_table_name=self.table_name,
                filter_entity_id=self.unique_id_field
            ).run()
        else:
            filtered_metrics = self.ingest_metadata
        # If there are metrics to ingest then ingest them
        if filtered_metrics:
            # Batch ingest of table to table within dataset
            logging.info(f"Starting ingest into {self.table_name} in dataset {self.dataset_id}")
            BatchIngest(
                ingest_metadata=filtered_metrics,
                tdr=self.tdr,
                target_table_name=self.table_name,
                dataset_id=self.dataset_id,
                batch_size=self.ingest_batch_size,
                bulk_mode=self.bulk_mode,
                update_strategy=self.update_strategy,
                waiting_time_to_poll=self.ingest_waiting_time_poll,
                test_ingest=self.test_ingest,
                load_tag=self.load_tag,
                file_to_uuid_dict=self.file_to_uuid_dict,
                schema_info=self.schema_info,
                skip_reformat=self.skip_reformat
            ).run()


class GetPermissionsForWorkspaceIngest:
    """Obtain permissions necessary for workspace ingest."""

    def __init__(self, terra_workspace: TerraWorkspace, dataset_info: dict, added_to_auth_domain: bool = False):
        """
        Initialize the GetPermissionsForWorkspaceIngest class.

        **Args:**
        - terra_workspace (`ops_utils.terra_util.TerraWorkspace`): Instance of the TerraWorkspace class.
        - dataset_info (dict): Information about the dataset.
        - added_to_auth_domain (bool, optional): Flag indicating if the SA account
                has been added to the auth domain. Defaults to `False`.
        """
        self.terra_workspace = terra_workspace
        """@private"""
        self.dataset_info = dataset_info
        """@private"""
        self.added_to_auth_domain = added_to_auth_domain
        """@private"""

    def run(self) -> None:
        """
        Ensure the dataset SA account has the necessary permissions on the Terra workspace.

        This method updates the user ACL to make the SA account a reader on the Terra workspace.
        It also checks if the workspace has an authorization domain, and logs the
        necessary steps to add the SA account to the auth domain.
        """
        # Ensure dataset SA account is reader on Terra workspace.
        tdr_sa_account = self.dataset_info["ingestServiceAccount"]
        self.terra_workspace.update_user_acl(email=tdr_sa_account, access_level="READER")

        # Check if workspace has auth domain
        workspace_info = self.terra_workspace.get_workspace_info().json()
        auth_domain_list = workspace_info["workspace"]["authorizationDomain"]
        # Attempt to add tdr_sa_account to auth domain
        if auth_domain_list:
            for auth_domain_dict in auth_domain_list:
                auth_domain = auth_domain_dict["membersGroupName"]
                logging.info(f"TDR SA account {tdr_sa_account} needs to be added to auth domain group {auth_domain}")
            if self.added_to_auth_domain:
                logging.info("added_to_auth_domain has been set to true so assuming account has already been added")
            else:
                logging.info(
                    f"Please add TDR SA account {tdr_sa_account} to auth domain group(s) to allow  "
                    "access to workspace and then rerun with added_to_auth_domain=True"
                )
                sys.exit(0)
