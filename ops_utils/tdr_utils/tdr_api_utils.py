"""Utility classes for interacting with TDR API."""

import json
import logging
import requests
from typing import Any, Optional, Union
from pydantic import ValidationError

from ..request_util import GET, POST, DELETE, PUT, RunRequest
from ..tdr_api_schema.create_dataset_schema import CreateDatasetSchema
from ..tdr_api_schema.update_dataset_schema import UpdateSchema
from .tdr_job_utils import MonitorTDRJob, SubmitAndMonitorMultipleJobs
from ..vars import ARG_DEFAULTS, GCP, APPLICATION_JSON


class TDR:
    """Class to interact with the Terra Data Repository (TDR) API."""

    PROD_LINK = "https://data.terra.bio/api/repository/v1"
    DEV_LINK = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1"
    """(str): The base URL for the TDR API."""

    def __init__(self, request_util: RunRequest, env: str = 'prod', dry_run: bool = False):
        """
        Initialize the TDR class (A class to interact with the Terra Data Repository (TDR) API).

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): Utility for making HTTP requests.
        """
        self.request_util = request_util
        # NOTE: dry_run is not fully implemented in this class, only in delete_files_and_snapshots
        self.dry_run = dry_run
        if env.lower() == 'prod':
            self.tdr_link = self.PROD_LINK
        elif env.lower() == 'dev':
            self.tdr_link = self.DEV_LINK
        else:
            raise RuntimeError(f"Unsupported environment: {env}. Must be 'prod' or 'dev'.")
        """@private"""

    @staticmethod
    def _check_policy(policy: str) -> None:
        """
        Check if the policy is valid.

        **Args:**
        - policy (str): The role to check.

        **Raises:**
        - ValueError: If the policy is not one of the allowed options.
        """
        if policy not in ["steward", "custodian", "snapshot_creator"]:
            raise ValueError(f"Policy {policy} is not valid. Must be steward, custodian, or snapshot_creator")

    def get_dataset_files(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> list[dict]:
        """
        Get all files in a dataset.

        Returns json like below

            {
                "fileId": "68ba8bfc-1d84-4ef3-99b8-cf1754d5rrrr",
                "collectionId": "b20b6024-5943-4c23-82e7-9c24f545fuy7",
                "path": "/path/set/in/ingest.csv",
                "size": 1722,
                "checksums": [
                    {
                        "checksum": "82f7e79v",
                        "type": "crc32c"
                    },
                    {
                        "checksum": "fff973507e30b74fa47a3d6830b84a90",
                        "type": "md5"
                    }
                ],
                "created": "2024-13-11T15:01:00.256Z",
                "description": null,
                "fileType": "file",
                "fileDetail": {
                    "datasetId": "b20b6024-5943-4c23-82e7-9c24f5456444",
                    "mimeType": null,
                    "accessUrl": "gs://datarepo-bucket/path/to/actual/file.csv",
                    "loadTag": "RP_3333-RP_3333"
                },
                "directoryDetail": null
            }

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `20000`.

        **Returns:**
        - list[dict]: A list of dictionaries containing the metadata of the files in the dataset.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}/files"
        logging.info(f"Getting all files in dataset {dataset_id}")
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)

    def create_file_dict(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> dict:
        """
        Create a dictionary of all files in a dataset where the key is the file UUID.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `20000`.

        **Returns:**
        - dict: A dictionary where the key is the file UUID and the value is the file metadata.
        """
        return {
            file_dict["fileId"]: file_dict
            for file_dict in self.get_dataset_files(dataset_id=dataset_id, limit=limit)
        }

    def create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> dict:
        """
        Create a dictionary of all files in a dataset where the key is the file 'path' and the value is the file UUID.

        This assumes that the TDR 'path' is original path of the file in the cloud storage with `gs://` stripped out.

        This will ONLY work if dataset was created with `experimentalSelfHosted = True`

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `20000`.

        **Returns:**
        - dict: A dictionary where the key is the file UUID and the value is the file path.
        """
        return {
            file_dict['fileDetail']['accessUrl']: file_dict['fileId']
            for file_dict in self.get_dataset_files(dataset_id=dataset_id, limit=limit)
        }

    def delete_file(self, file_id: str, dataset_id: str) -> requests.Response:
        """
        Delete a file from a dataset.

        **Args:**
        - file_id (str): The ID of the file to be deleted.
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}/files/{file_id}"
        logging.info(f"Submitting delete job for file {file_id}")
        return self.request_util.run_request(uri=uri, method=DELETE)

    def delete_files(
            self,
            file_ids: list[str],
            dataset_id: str,
            batch_size_to_delete_files: int = ARG_DEFAULTS["batch_size_to_delete_files"],  # type: ignore[assignment]
            check_interval: int = 15) -> None:
        """
        Delete multiple files from a dataset in batches and monitor delete jobs until completion for each batch.

        **Args:**
        - file_ids (list[str]): A list of file IDs to be deleted.
        - dataset_id (str): The ID of the dataset.
        - batch_size_to_delete_files (int, optional): The number of files to delete per batch. Defaults to `200`.
        - check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to `15`.
        """
        SubmitAndMonitorMultipleJobs(
            tdr=self,
            job_function=self.delete_file,
            job_args_list=[(file_id, dataset_id) for file_id in file_ids],
            batch_size=batch_size_to_delete_files,
            check_interval=check_interval
        ).run()

    def _delete_snapshots_for_files(self, dataset_id: str, file_ids: set[str]) -> None:
        """Delete snapshots that reference any of the provided file IDs."""
        snapshots_resp = self.get_dataset_snapshots(dataset_id=dataset_id)
        snapshot_items = snapshots_resp.json().get('items', [])
        snapshots_to_delete = []
        logging.info(
            "Checking %d snapshots for references",
            len(snapshot_items),
        )
        for snap in snapshot_items:
            snap_id = snap.get('id')
            if not snap_id:
                continue
            snap_files = self.get_files_from_snapshot(snapshot_id=snap_id)
            snap_file_ids = {
                fd.get('fileId') for fd in snap_files if fd.get('fileId')
            }
            # Use set intersection to check for any matching file IDs
            if snap_file_ids & file_ids:
                snapshots_to_delete.append(snap_id)
        if snapshots_to_delete:
            self.delete_snapshots(snapshot_ids=snapshots_to_delete)
        else:
            logging.info("No snapshots reference the provided file ids")

    def dry_run_msg(self) -> str:
        return '[Dry run] ' if self.dry_run else ''

    def delete_files_and_snapshots(self, dataset_id: str, file_ids: set[str]) -> None:
        """Delete files from a dataset by their IDs, handling snapshots."""
        self._delete_snapshots_for_files(dataset_id=dataset_id, file_ids=file_ids)

        logging.info(
            f"{self.dry_run_msg()}Submitting delete request for {len(file_ids)} files in "
            f"dataset {dataset_id}")
        if not self.dry_run:
            self.delete_files(
                file_ids=list(file_ids),
                dataset_id=dataset_id
            )

    def add_user_to_dataset(self, dataset_id: str, user: str, policy: str) -> requests.Response:
        """
        Add a user to a dataset with a specified policy.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - user (str): The email of the user to be added.
        - policy (str): The policy to be assigned to the user.
                Must be one of `steward`, `custodian`, or `snapshot_creator`.

        **Returns:**
        - requests.Response: The response from the request.

        **Raises:**
        - ValueError: If the policy is not valid.
        """
        self._check_policy(policy)
        uri = f"{self.tdr_link}/datasets/{dataset_id}/policies/{policy}/members"
        member_dict = {"email": user}
        logging.info(f"Adding user {user} to dataset {dataset_id} with policy {policy}")
        return self.request_util.run_request(
            uri=uri,
            method=POST,
            data=json.dumps(member_dict),
            content_type=APPLICATION_JSON
        )

    def remove_user_from_dataset(self, dataset_id: str, user: str, policy: str) -> requests.Response:
        """
        Remove a user from a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - user (str): The email of the user to be removed.
        - policy (str): The policy to be removed from the user.
                Must be one of `steward`, `custodian`, or `snapshot_creator`.

        **Returns:**
        - requests.Response: The response from the request.

        **Raises:**
        - ValueError: If the policy is not valid.
        """
        self._check_policy(policy)
        uri = f"{self.tdr_link}/datasets/{dataset_id}/policies/{policy}/members/{user}"
        logging.info(f"Removing user {user} from dataset {dataset_id} with policy {policy}")
        return self.request_util.run_request(uri=uri, method=DELETE)

    def delete_dataset(self, dataset_id: str) -> None:
        """
        Delete a dataset and monitors the job until completion.

        **Args:**
            dataset_id (str): The ID of the dataset to be deleted.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}"
        logging.info(f"Deleting dataset {dataset_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=False).run()

    def make_snapshot_public(self, snapshot_id: str) -> requests.Response:
        """
        Make a snapshot public.

        **Args:**
        - snapshot_id (str): The ID of the snapshot to be made public.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/snapshots/{snapshot_id}/public"
        logging.info(f"Making snapshot {snapshot_id} public")
        return self.request_util.run_request(uri=uri, method=PUT, content_type=APPLICATION_JSON, data="true")

    def get_snapshot_info(
            self,
            snapshot_id: str,
            continue_not_found: bool = False,
            info_to_include: Optional[list[str]] = None
    ) -> Optional[requests.Response]:
        """
        Get information about a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - continue_not_found (bool, optional): Whether to accept a `404` response. Defaults to `False`.
        - info_to_include (list[str], optional): A list of additional information to include. Defaults to None.
                Options are: `SOURCES`, `TABLES`, `RELATIONSHIPS`, `ACCESS_INFORMATION`, `PROFILE`, `PROPERTIES`,
                `DATA_PROJECT`,`CREATION_INFORMATION`, `DUOS`

        **Returns:**
        - requests.Response (optional): The response from the request (returns None if the snapshot is not
         found or access is denied).
        """
        acceptable_return_code = [404, 403] if continue_not_found else []
        acceptable_include_info = [
            "SOURCES",
            "TABLES",
            "RELATIONSHIPS",
            "ACCESS_INFORMATION",
            "PROFILE",
            "PROPERTIES",
            "DATA_PROJECT",
            "CREATION_INFORMATION",
            "DUOS"
        ]
        if info_to_include:
            if not all(info in acceptable_include_info for info in info_to_include):
                raise ValueError(f"info_to_include must be a subset of {acceptable_include_info}")
            include_string = '&include='.join(info_to_include)
        else:
            include_string = ""
        uri = f"{self.tdr_link}/snapshots/{snapshot_id}?include={include_string}"
        response = self.request_util.run_request(
            uri=uri,
            method=GET,
            accept_return_codes=acceptable_return_code
        )
        if response.status_code == 404:
            logging.warning(f"Snapshot {snapshot_id} not found")
            return None
        if response.status_code == 403:
            logging.warning(f"Access denied for snapshot {snapshot_id}")
            return None
        return response

    def delete_snapshots(
            self,
            snapshot_ids: list[str],
            batch_size: int = 25,
            check_interval: int = 10,
            verbose: bool = False) -> None:
        """
        Delete multiple snapshots from a dataset in batches and monitor delete jobs until completion for each batch.

        **Args:**
        - snapshot_ids (list[str]): A list of snapshot IDs to be deleted.
        - batch_size (int, optional): The number of snapshots to delete per batch. Defaults to `25`.
        - check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to `10`.
        - verbose (bool, optional): Whether to log detailed information about each job. Defaults to `False`.
        """
        logging.info(f"{self.dry_run_msg()}Deleting {len(snapshot_ids)} snapshots")
        if not self.dry_run:
            SubmitAndMonitorMultipleJobs(
                tdr=self,
                job_function=self.delete_snapshot,
                job_args_list=[(snapshot_id,) for snapshot_id in snapshot_ids],
                batch_size=batch_size,
                check_interval=check_interval,
                verbose=verbose
            ).run()

    def delete_snapshot(self, snapshot_id: str) -> requests.Response:
        """
        Delete a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot to be deleted.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/snapshots/{snapshot_id}"
        logging.info(f"Deleting snapshot {snapshot_id}")
        return self.request_util.run_request(uri=uri, method=DELETE)

    def _yield_existing_datasets(
            self, filter: Optional[str] = None, batch_size: int = 100, direction: str = "asc"
    ) -> Any:
        """
        Get all datasets in TDR, optionally filtered by dataset name.

        **Args:**
            filter (Optional[str]): A filter string to match dataset names. Defaults to None.
            batch_size (int): The number of datasets to retrieve per batch. Defaults to 100.
            direction (str): The direction to sort the datasets by creation date. Defaults to "asc".

        Yields:
            Any: A generator yielding datasets.
        """
        offset = 0
        if filter:
            filter_str = f"&filter={filter}"
            log_message = f"Searching for datasets with filter {filter} in batches of {batch_size}"
        else:
            filter_str = ""
            log_message = f"Searching for all datasets in batches of {batch_size}"
        logging.info(log_message)
        while True:
            uri = f"{self.tdr_link}/datasets?offset={offset}&limit={batch_size}&sort=created_date&direction={direction}{filter_str}"  # noqa: E501
            response = self.request_util.run_request(uri=uri, method=GET)
            datasets = response.json()["items"]
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
            offset += batch_size
            break

    def check_if_dataset_exists(self, dataset_name: str, billing_profile: Optional[str] = None) -> list[dict]:
        """
        Check if a dataset exists by name and optionally by billing profile.

        **Args:**
        - dataset_name (str): The name of the dataset to check.
        - billing_profile (str, optional): The billing profile ID to match. Defaults to None.

        **Returns:**
        - list[dict]: A list of matching datasets.
        """
        matching_datasets = []
        for dataset in self._yield_existing_datasets(filter=dataset_name):
            # Search uses wildcard so could grab more datasets where dataset_name is substring
            if dataset_name == dataset["name"]:
                if billing_profile:
                    if dataset["defaultProfileId"] == billing_profile:
                        logging.info(
                            f"Dataset {dataset['name']} already exists under billing profile {billing_profile}")
                        dataset_id = dataset["id"]
                        logging.info(f"Dataset ID: {dataset_id}")
                        matching_datasets.append(dataset)
                    else:
                        logging.warning(
                            f"Dataset {dataset['name']} exists but is under {dataset['defaultProfileId']} " +
                            f"and not under billing profile {billing_profile}"
                        )
                        # Datasets names need to be unique regardless of billing profile, so raise an error if
                        # a dataset with the same name is found but is not under the requested billing profile
                        raise ValueError(
                            f"Dataset {dataset_name} already exists but is not under billing profile {billing_profile}")
                else:
                    matching_datasets.append(dataset)
        return matching_datasets

    def get_dataset_info(self, dataset_id: str, info_to_include: Optional[list[str]] = None) -> requests.Response:
        """
        Get information about a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - info_to_include (list[str], optional): A list of additional information to include. Valid options include:
        `SCHEMA`, `ACCESS_INFORMATION`, `PROFILE`, `PROPERTIES`, `DATA_PROJECT`, `STORAGE`, `SNAPSHOT_BUILDER_SETTING`.
        Defaults to None.

        **Returns:**
        - requests.Response: The response from the request.

        **Raises:**
        - ValueError: If `info_to_include` contains invalid information types.
        """
        acceptable_include_info = [
            "SCHEMA",
            "ACCESS_INFORMATION",
            "PROFILE",
            "PROPERTIES",
            "DATA_PROJECT",
            "STORAGE",
            "SNAPSHOT_BUILDER_SETTING"
        ]
        if info_to_include:
            if not all(info in acceptable_include_info for info in info_to_include):
                raise ValueError(f"info_to_include must be a subset of {acceptable_include_info}")
            include_string = '&include='.join(info_to_include)
        else:
            include_string = ""
        uri = f"{self.tdr_link}/datasets/{dataset_id}?include={include_string}"
        return self.request_util.run_request(uri=uri, method=GET)

    def get_table_schema_info(
            self,
            dataset_id: str,
            table_name: str,
            dataset_info: Optional[dict] = None
    ) -> Union[dict, None]:
        """
        Get schema information for a specific table within a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - table_name (str): The name of the table.
        - dataset_info (dict, optional): The dataset information if already retrieved. Defaults to None.

        **Returns:**
        - Union[dict, None]: A dictionary containing the table schema information, or None if the table is not found.
        """
        if not dataset_info:
            dataset_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"]).json()
        for table in dataset_info["schema"]["tables"]:  # type: ignore[index]
            if table["name"] == table_name:
                return table
        return None

    def get_job_result(self, job_id: str, expect_failure: bool = False) -> requests.Response:
        """
        Retrieve the result of a job.

        **Args:**
        - job_id (str): The ID of the job.
        - expect_failure (bool, optional): Whether the job is expected to fail. Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/jobs/{job_id}/result"
        # If job is expected to fail, accept any return code
        acceptable_return_code = list(range(100, 600)) if expect_failure else []
        return self.request_util.run_request(uri=uri, method=GET, accept_return_codes=acceptable_return_code)

    def ingest_to_dataset(self, dataset_id: str, data: dict) -> requests.Response:
        """
        Load data into a TDR dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - data (dict): The data to be ingested.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}/ingest"
        logging.info(
            "If recently added TDR SA to source bucket/dataset/workspace and you receive a 400/403 error, " +
            "it can sometimes take up to 12/24 hours for permissions to propagate. Try rerunning the script later.")
        return self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type=APPLICATION_JSON,
            data=data
        )

    def file_ingest_to_dataset(
            self,
            dataset_id: str,
            profile_id: str,
            file_list: list[dict],
            load_tag: str = "file_ingest_load_tag"
    ) -> dict:
        """
        Load files into a TDR dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - profile_id (str): The billing profile ID.
        - file_list (list[dict]): The list of files to be ingested.
        - load_tag (str): The tag to be used in the ingest job. Defaults to `file_ingest_load_tag`.

        **Returns:**
        - dict: A dictionary containing the response from the ingest operation job monitoring.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}/files/bulk/array"
        data = {
            "profileId": profile_id,
            "loadTag": f"{load_tag}",
            "maxFailedFileLoads": 0,
            "loadArray": file_list
        }

        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(data)
        )
        job_id = response.json()['id']
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        return job_results  # type: ignore[return-value]

    def get_dataset_table_metrics(
            self, dataset_id: str, target_table_name: str, query_limit: int = 1000
    ) -> list[dict]:
        """
        Retrieve all metrics for a specific table within a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - target_table_name (str): The name of the target table.
        - query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `1000`.

        **Returns:**
        - list[dict]: A list of dictionaries containing the metrics for the specified table.
        """
        return [
            metric
            for metric in self._yield_dataset_metrics(
                dataset_id=dataset_id,
                target_table_name=target_table_name,
                query_limit=query_limit
            )
        ]

    def _yield_dataset_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> Any:
        """
        Yield all entity metrics from a dataset.

        **Args:**
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Yields:
            Any: A generator yielding dictionaries containing the metrics for the specified table.
        """
        search_request = {
            "offset": 0,
            "limit": query_limit,
            "sort": "datarepo_row_id"
        }
        uri = f"{self.tdr_link}/datasets/{dataset_id}/data/{target_table_name}"
        while True:
            batch_number = int((search_request["offset"] / query_limit)) + 1  # type: ignore[operator]
            response = self.request_util.run_request(
                uri=uri,
                method=POST,
                content_type=APPLICATION_JSON,
                data=json.dumps(search_request)
            )
            if not response or not response.json()["result"]:
                break
            logging.info(
                f"Downloading batch {batch_number} of max {query_limit} records from {target_table_name} table " +
                f"dataset {dataset_id}"
            )
            for record in response.json()["result"]:
                yield record
            search_request["offset"] += query_limit  # type: ignore[operator]

    def get_dataset_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) -> list[str]:
        """
        Get existing IDs from a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - target_table_name (str): The name of the target table.
        - entity_id (str): The entity ID to retrieve.

        **Returns:**
        - list[str]: A list of entity IDs from the specified table.
        """
        dataset_metadata = self._yield_dataset_metrics(dataset_id=dataset_id, target_table_name=target_table_name)
        return [str(sample_dict[entity_id]) for sample_dict in dataset_metadata]

    def get_job_status(self, job_id: str) -> requests.Response:
        """
        Retrieve the status of a job.

        **Args:**
        - job_id (str): The ID of the job.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/jobs/{job_id}"
        return self.request_util.run_request(uri=uri, method=GET)

    def get_dataset_file_uuids_from_metadata(self, dataset_id: str) -> list[str]:
        """
        Get all file UUIDs from the metadata of a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - list[str]: A list of file UUIDs from the dataset metadata.
        """
        dataset_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"]).json()
        all_metadata_file_uuids = []
        tables = 0
        for table in dataset_info["schema"]["tables"]:
            tables += 1
            table_name = table["name"]
            logging.info(f"Getting all file information for {table_name}")
            # Get just columns where datatype is fileref
            file_columns = [column["name"] for column in table["columns"] if column["datatype"] == "fileref"]
            dataset_metrics = self.get_dataset_table_metrics(dataset_id=dataset_id, target_table_name=table_name)
            # Get unique list of file uuids
            file_uuids = list(
                set(
                    [
                        value for metric in dataset_metrics for key, value in metric.items() if key in file_columns
                    ]
                )
            )
            logging.info(f"Got {len(file_uuids)} file uuids from table '{table_name}'")
            all_metadata_file_uuids.extend(file_uuids)
            # Make full list unique
            all_metadata_file_uuids = list(set(all_metadata_file_uuids))
        logging.info(f"Got {len(all_metadata_file_uuids)} file uuids from {tables} total table(s)")
        return all_metadata_file_uuids

    def soft_delete_entries(
            self,
            dataset_id: str,
            table_name: str,
            datarepo_row_ids: list[str],
            check_intervals: int = 15
    ) -> Optional[dict]:
        """
        Soft delete specific records from a table.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - table_name (str): The name of the target table.
        - datarepo_row_ids (list[str]): A list of row IDs to be deleted.
        - check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to `15`.

        **Returns:**
        - dict (optional): A dictionary containing the response from the soft delete operation job
        monitoring. Returns None if no row IDs are provided.
        """
        if not datarepo_row_ids:
            logging.info(f"No records found to soft delete in table {table_name}")
            return None
        logging.info(f"Soft deleting {len(datarepo_row_ids)} records from table {table_name}")
        uri = f"{self.tdr_link}/datasets/{dataset_id}/deletes"
        payload = {
            "deleteType": "soft",
            "specType": "jsonArray",
            "tables": [
                {
                    "tableName": table_name,
                    "jsonArraySpec": {
                        "rowIds": datarepo_row_ids
                    }
                }
            ]
        }
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            data=json.dumps(payload),
            content_type=APPLICATION_JSON
        )
        job_id = response.json()["id"]
        return MonitorTDRJob(tdr=self, job_id=job_id, check_interval=check_intervals, return_json=False).run()

    def soft_delete_all_table_entries(
            self,
            dataset_id: str,
            table_name: str,
            query_limit: int = 1000,
            check_intervals: int = 15
    ) -> Optional[dict]:
        """
        Soft deletes all records in a table.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - table_name (str): The name of the target table.
        - query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `1000`.
        - check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to `15`.

        **Returns:**
        - dict (optional): A dictionary containing the response from the soft delete operation job monitoring. Returns
        None if no row IDs are provided.
        """
        dataset_metrics = self.get_dataset_table_metrics(
            dataset_id=dataset_id, target_table_name=table_name, query_limit=query_limit
        )
        row_ids = [metric["datarepo_row_id"] for metric in dataset_metrics]
        return self.soft_delete_entries(
            dataset_id=dataset_id,
            table_name=table_name,
            datarepo_row_ids=row_ids,
            check_intervals=check_intervals
        )

    def get_or_create_dataset(
            self,
            dataset_name: str,
            billing_profile: str,
            schema: dict,
            description: str,
            relationships: Optional[list[dict]] = None,
            delete_existing: bool = False,
            continue_if_exists: bool = False,
            additional_properties_dict: Optional[dict] = None
    ) -> str:
        """
        Get or create a dataset.

        **Args:**
        - dataset_name (str): The name of the dataset.
        - billing_profile (str): The billing profile ID.
        - schema (dict): The schema of the dataset.
        - description (str): The description of the dataset.
        - relationships (Optional[list[dict]], optional): A list of relationships to add to the dataset schema.
                Defaults to None.
        - additional_properties_dict (Optional[dict], optional): Additional properties
                for the dataset. Defaults to None.
        - delete_existing (bool, optional): Whether to delete the existing dataset if found.
                Defaults to `False`.
        - continue_if_exists (bool, optional): Whether to continue if the dataset already exists.
                Defaults to `False`.

        **Returns:**
        - str: The ID of the dataset.

        **Raises:**
        - ValueError: If multiple datasets with the same name are found under the billing profile.
        """
        existing_datasets = self.check_if_dataset_exists(dataset_name, billing_profile)
        if existing_datasets:
            if not continue_if_exists:
                raise ValueError(
                    f"Run with continue_if_exists=True to use the existing dataset {dataset_name}"
                )
            # If delete_existing is True, delete the existing dataset and set existing_datasets to an empty list
            if delete_existing:
                logging.info(f"Deleting existing dataset {dataset_name}")
                self.delete_dataset(existing_datasets[0]["id"])
                existing_datasets = []
            # If not delete_existing and continue_if_exists then grab existing datasets id
            else:
                dataset_id = existing_datasets[0]["id"]
        if not existing_datasets:
            logging.info("Did not find existing dataset")
            # Create dataset
            dataset_id = self.create_dataset(
                schema=schema,
                dataset_name=dataset_name,
                description=description,
                profile_id=billing_profile,
                additional_dataset_properties=additional_properties_dict
            )
        return dataset_id

    def create_dataset(  # type: ignore[return]
            self,
            schema: dict,
            dataset_name: str,
            description: str,
            profile_id: str,
            additional_dataset_properties: Optional[dict] = None
    ) -> Optional[str]:
        """
        Create a new dataset.

        **Args:**
        - schema (dict): The schema of the dataset.
        - dataset_name (str): The name of the dataset.
        - description (str): The description of the dataset.
        - profile_id (str): The billing profile ID.
        - additional_dataset_properties (Optional[dict], optional): Additional
                properties for the dataset. Defaults to None.

        **Returns:**
        - Optional[str]: The ID of the created dataset, or None if creation failed.

        Raises:
        - ValueError: If the schema validation fails.
        """
        dataset_properties = {
            "name": dataset_name,
            "description": description,
            "defaultProfileId": profile_id,
            "region": "us-central1",
            "cloudPlatform": GCP,
            "schema": schema
        }

        if additional_dataset_properties:
            dataset_properties.update(additional_dataset_properties)
        try:
            CreateDatasetSchema(**dataset_properties)  # type: ignore[arg-type]
        except ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")
        uri = f"{self.tdr_link}/datasets"
        logging.info(f"Creating dataset {dataset_name} under billing profile {profile_id}")
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            data=json.dumps(dataset_properties),
            content_type=APPLICATION_JSON
        )
        job_id = response.json()["id"]
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        dataset_id = job_results["id"]  # type: ignore[index]
        logging.info(f"Successfully created dataset {dataset_name}: {dataset_id}")
        return dataset_id

    def update_dataset_schema(  # type: ignore[return]
            self,
            dataset_id: str,
            update_note: str,
            tables_to_add: Optional[list[dict]] = None,
            relationships_to_add: Optional[list[dict]] = None,
            columns_to_add: Optional[list[dict]] = None
    ) -> Optional[str]:
        """
        Update the schema of a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - update_note (str): A note describing the update.
        - tables_to_add (list[dict], optional): A list of tables to add. Defaults to None.
        - relationships_to_add (list[dict], optional): A list of relationships to add. Defaults to None.
        - columns_to_add (list[dict], optional): A list of columns to add. Defaults to None.

        **Returns:**
        - Optional[str]: The ID of the updated dataset, or None if the update failed.

        **Raises:**
        - ValueError: If the schema validation fails.
        """
        uri = f"{self.tdr_link}/datasets/{dataset_id}/updateSchema"
        request_body: dict = {"description": f"{update_note}", "changes": {}}
        if tables_to_add:
            request_body["changes"]["addTables"] = tables_to_add
        if relationships_to_add:
            request_body["changes"]["addRelationships"] = relationships_to_add
        if columns_to_add:
            request_body["changes"]["addColumns"] = columns_to_add
        try:
            UpdateSchema(**request_body)
        except ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")

        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(request_body)
        )
        job_id = response.json()["id"]
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        dataset_id = job_results["id"]  # type: ignore[index]
        logging.info(f"Successfully ran schema updates in dataset {dataset_id}")
        return dataset_id

    def _get_response_from_batched_endpoint(self, uri: str, limit: int = 1000) -> list[dict]:
        """
        Get response from a batched endpoint.

        Helper method for all GET endpoints that require batching.

        Given the URI and the limit (optional), will
        loop through batches until all metadata is retrieved.

        **Args:**
        - uri (str): The base URI for the endpoint (without query params for offset or limit).
        - limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `1000`.

        **Returns:**
        - list[dict]: A list of dictionaries containing the metadata retrieved from the endpoint.
        """
        batch = 1
        offset = 0
        metadata: list = []
        while True:
            logging.info(f"Retrieving {(batch - 1) * limit} to {batch * limit} records in metadata")
            response_json = self.request_util.run_request(uri=f"{uri}?offset={offset}&limit={limit}", method=GET).json()

            # If no more files, break the loop
            if not response_json:
                logging.info(f"No more results to retrieve, found {len(metadata)} total records")
                break

            metadata.extend(response_json)
            if len(response_json) < limit:
                logging.info(f"Retrieved final batch of results, found {len(metadata)} total records")
                break

            # Increment the offset by limit for the next page
            offset += limit
            batch += 1
        return metadata

    def get_files_from_snapshot(self, snapshot_id: str, limit: int = 1000) -> list[dict]:
        """
        Return all the metadata about files in a given snapshot.

        Not all files can be returned at once, so the API
        is used repeatedly until all "batches" have been returned.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - limit (int, optional): The maximum number of records to retrieve per batch. Defaults to `1000`.

        **Returns:**
        - list[dict]: A list of dictionaries containing the metadata of the files in the snapshot.
        """
        uri = f"{self.tdr_link}/snapshots/{snapshot_id}/files"
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)

    def get_dataset_snapshots(self, dataset_id: str) -> requests.Response:
        """
        Return snapshots belonging to specified dataset.

        **Args:**
        - dataset_id: uuid of dataset to query.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/snapshots?datasetIds={dataset_id}"
        return self.request_util.run_request(
            uri=uri,
            method=GET
        )

    def create_snapshot(
            self,
            snapshot_name: str,
            description: str,
            dataset_name: str,
            snapshot_mode: str,  # byFullView is entire dataset
            profile_id: str,
            stewards: Optional[list[str]] = [],
            readers: Optional[list[str]] = [],
            consent_code: Optional[str] = None,
            duos_id: Optional[str] = None,
            data_access_control_groups: Optional[list[str]] = None,
    ) -> None:
        """
        Create a snapshot in TDR.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.tdr_link}/snapshots"
        payload = {
            "name": snapshot_name,
            "description": description,
            "contents": [
                {
                    "datasetName": dataset_name,
                    "mode": snapshot_mode,
                }
            ],
            "policies": {
                "stewards": stewards,
                "readers": readers,
            },
            "profileId": profile_id,
            "globalFileIds": True,
        }
        if consent_code:
            payload["consentCode"] = consent_code
        if duos_id:
            payload["duosId"] = duos_id
        if data_access_control_groups:
            payload["dataAccessControlGroups"] = data_access_control_groups
        logging.info(f"Creating snapshot {snapshot_name} in dataset {dataset_name}")
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )
        job_id = response.json()["id"]
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        snapshot_id = job_results["id"]  # type: ignore[index]
        logging.info(f"Successfully created snapshot {snapshot_name} - {snapshot_id}")


class FilterOutSampleIdsAlreadyInDataset:
    """Class to filter ingest metrics to remove sample IDs that already exist in the dataset."""

    def __init__(
            self,
            ingest_metrics: list[dict],
            dataset_id: str,
            tdr: TDR,
            target_table_name: str,
            filter_entity_id: str
    ):
        """
        Initialize the FilterOutSampleIdsAlreadyInDataset class.

        **Args:**
        - ingest_metrics (list[dict]): The metrics to be ingested.
        - dataset_id (str): The ID of the dataset.
        - tdr (`ops_utils.tdr_utils.tdr_utils.TDR`): The TDR instance
        - target_table_name (str): The name of the target table.
        - filter_entity_id (str): The entity ID to filter on.
        """
        self.ingest_metrics = ingest_metrics
        """@private"""
        self.tdr = tdr
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.target_table_name = target_table_name
        """@private"""
        self.filter_entity_id = filter_entity_id
        """@private"""

    def run(self) -> list[dict]:
        """
        Run the filter process to remove sample IDs that already exist in the dataset.

        **Returns:**
        - list[dict]: The filtered ingest metrics.
        """
        # Get all sample ids that already exist in dataset
        logging.info(
            f"Getting all {self.filter_entity_id} that already exist in table {self.target_table_name} in "
            f"dataset {self.dataset_id}"
        )

        dataset_sample_ids = self.tdr.get_dataset_sample_ids(
            dataset_id=self.dataset_id,
            target_table_name=self.target_table_name,
            entity_id=self.filter_entity_id
        )
        # Filter out rows that already exist in dataset
        filtered_ingest_metrics = [
            row
            for row in self.ingest_metrics
            if str(row[self.filter_entity_id]) not in dataset_sample_ids
        ]
        if len(filtered_ingest_metrics) < len(self.ingest_metrics):
            logging.info(
                f"Filtered out {len(self.ingest_metrics) - len(filtered_ingest_metrics)} rows that already exist in "
                f"dataset. There is {len(filtered_ingest_metrics)} rows left to ingest"
            )

            if filtered_ingest_metrics:
                return filtered_ingest_metrics
            else:
                logging.info("All rows filtered out as they all exist in dataset, nothing to ingest")
                return []
        else:
            logging.info("No rows were filtered out as they all do not exist in dataset")
            return filtered_ingest_metrics
