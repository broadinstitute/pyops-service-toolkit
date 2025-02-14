Module ops_utils.tdr_utils
==========================

Sub-modules
-----------
* ops_utils.tdr_utils.renaming_util
* ops_utils.tdr_utils.tdr_api_utils
* ops_utils.tdr_utils.tdr_bq_utils
* ops_utils.tdr_utils.tdr_ingest_utils
* ops_utils.tdr_utils.tdr_job_utils
* ops_utils.tdr_utils.tdr_schema_utils
* ops_utils.tdr_utils.tdr_table_utils

Classes
-------

`MonitorTDRJob(tdr: Any, job_id: str, check_interval: int, return_json: bool)`
:   A class to monitor the status of a TDR job until completion.
    
    Attributes:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
    
    Initialize the MonitorTDRJob class.
    
    Args:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
        return_json (bool): Whether to get and return the result of the job as json.

    ### Methods

    `run(self) ‑> dict | None`
    :   Monitor the job until completion.
        
        Returns:
            dict: The result of the job.

`SubmitAndMonitorMultipleJobs(tdr: Any, job_function: Callable, job_args_list: list[tuple], batch_size: int = 100, check_interval: int = 5, verbose: bool = False)`
:   Initialize the SubmitAndMonitorMultipleJobs class.
    
    Args:
        tdr (Any): An instance of the TDR class.
        job_function (Callable): The function to submit a job.
        job_args_list (list[tuple]): A list of tuples containing the arguments for each job.
        batch_size (int, optional): The number of jobs to process in each batch. Defaults to 100.
        check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 5.
        verbose (bool, optional): Whether to log detailed information about each job. Defaults to False.

    ### Methods

    `run(self) ‑> None`
    :   Run the process to submit and monitor multiple jobs in batches.
        
        Logs the progress and status of each batch and job.
        
        Returns:
            None

`TDR(request_util: Any)`
:   A class to interact with the Terra Data Repository (TDR) API.
    
    Attributes:
        TDR_LINK (str): The base URL for the TDR API.
        request_util (Any): Utility for making HTTP requests.
    
    Initialize the TDR class.
    
    Args:
        request_util (Any): Utility for making HTTP requests.

    ### Class variables

    `TDR_LINK`
    :

    ### Methods

    `add_user_to_dataset(self, dataset_id: str, user: str, policy: str) ‑> None`
    :   Add a user to a dataset with a specified policy.
        
        Args:
            dataset_id (str): The ID of the dataset.
            user (str): The email of the user to be added.
            policy (str): The policy to be assigned to the user.
                Must be one of "steward", "custodian", or "snapshot_creator".
        
        Raises:
            ValueError: If the policy is not valid.

    `check_if_dataset_exists(self, dataset_name: str, billing_profile: str | None = None) ‑> list[dict]`
    :   Check if a dataset exists by name and optionally by billing profile.
        
        Args:
            dataset_name (str): The name of the dataset to check.
            billing_profile (Optional[str]): The billing profile ID to match. Defaults to None.
        
        Returns:
            list[dict]: A list of matching datasets.

    `create_dataset(self, schema: dict, cloud_platform: str, dataset_name: str, description: str, profile_id: str, additional_dataset_properties: dict | None = None) ‑> str | None`
    :   Create a new dataset.
        
        Args:
            schema (dict): The schema of the dataset.
            cloud_platform (str): The cloud platform for the dataset.
            dataset_name (str): The name of the dataset.
            description (str): The description of the dataset.
            profile_id (str): The billing profile ID.
            additional_dataset_properties (Optional[dict], optional): Additional
                properties for the dataset. Defaults to None.
        
        Returns:
            Optional[str]: The ID of the created dataset, or None if creation failed.
        
        Raises:
            ValueError: If the schema validation fails.

    `create_file_dict(self, dataset_id: str, limit: int = 20000) ‑> dict`
    :   Create a dictionary of all files in a dataset where the key is the file UUID.
        
        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 20000.
        
        Returns:
            dict: A dictionary where the key is the file UUID and the value is the file metadata.

    `create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset(self, dataset_id: str, limit: int = 20000) ‑> dict`
    :   Create a dictionary of all files in a dataset where the key is the file 'path' and the value is the file UUID.
        This assumes that the tdr 'path' is original path of the file in the cloud storage with gs:// stripped out
        
        This will ONLY work if dataset was created with experimentalSelfHosted = True
        
        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 20000.
        
        Returns:
            dict: A dictionary where the key is the file UUID and the value is the file path.

    `delete_dataset(self, dataset_id: str) ‑> None`
    :   Delete a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset to be deleted.

    `delete_file(self, file_id: str, dataset_id: str) ‑> str`
    :   Delete a file from a dataset.
        
        Args:
            file_id (str): The ID of the file to be deleted.
            dataset_id (str): The ID of the dataset.
        
        Returns:
            str: The job ID of the delete operation.

    `delete_files(self, file_ids: list[str], dataset_id: str, batch_size_to_delete_files: int = 250, check_interval: int = 15) ‑> None`
    :   Delete multiple files from a dataset in batches and monitor delete jobs until completion for each batch.
        
        Args:
            file_ids (list[str]): A list of file IDs to be deleted.
            dataset_id (str): The ID of the dataset.
            batch_size_to_delete_files (int, optional): The number of files to delete per batch. Defaults to 100.
            check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 15.

    `delete_snapshot(self, snapshot_id: str) ‑> str`
    :   Delete a snapshot.
        
        Args:
            snapshot_id (str): The ID of the snapshot to be deleted.

    `delete_snapshots(self, snapshot_ids: list[str], batch_size: int = 25, check_interval: int = 10, verbose: bool = False) ‑> None`
    :   Delete multiple snapshots.
        
        Args:
            snapshot_ids (list[str]): A list of snapshot IDs to be deleted.
            batch_size (int, optional): The number of snapshots to delete per batch. Defaults to 25.
            check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 10.
            verbose (bool, optional): Whether to log detailed information about each job. Defaults to False.

    `file_ingest_to_dataset(self, dataset_id: str, profile_id: str, file_list: list[dict], load_tag: str = 'file_ingest_load_tag') ‑> dict`
    :   Load files into a TDR dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            data (dict): list of cloud file paths to be ingested.
            {
                "sourcePath":"gs:{bucket_name}/{file_path}",
                "targetPath":"/{path}/{file_name}"
            }
        
        Returns:
            dict: A dictionary containing the response from the ingest operation.

    `get_data_set_file_uuids_from_metadata(self, dataset_id: str) ‑> list[str]`
    :   Get all file UUIDs from the metadata of a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
        
        Returns:
            list[str]: A list of file UUIDs from the dataset metadata.

    `get_data_set_files(self, dataset_id: str, limit: int = 20000) ‑> list[dict]`
    :   Get all files in a dataset. Returns json like below
        
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
        
            Args:
                dataset_id (str): The ID of the dataset.
                limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.
        
            Returns:
                list[dict]: A list of dictionaries containing the metadata of the files in the dataset.

    `get_data_set_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) ‑> list[str]`
    :   Get existing IDs from a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            entity_id (str): The entity ID to retrieve.
        
        Returns:
            list[str]: A list of entity IDs from the specified table.

    `get_dataset_info(self, dataset_id: str, info_to_include: list[str] | None = None) ‑> dict`
    :   Get information about a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            info_to_include (Optional[list[str]]): A list of additional information to include. Defaults to None.
        
        Returns:
            dict: A dictionary containing the dataset information.
        
        Raises:
            ValueError: If info_to_include contains invalid information types.

    `get_dataset_snapshots(self, dataset_id: str) ‑> list[dict]`
    :   Returns snapshots belonging to specified datset.
        
        Args:
            dataset_id: uuid of dataset to query.
        
        Returns:
            list[dict]: A list of dictionaries containing the metadata of snapshots in the dataset.

    `get_dataset_table_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) ‑> list[dict]`
    :   Retrieve all metrics for a specific table within a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.
        
        Returns:
            list[dict]: A list of dictionaries containing the metrics for the specified table.

    `get_files_from_snapshot(self, snapshot_id: str, limit: int = 1000) ‑> list[dict]`
    :   Returns all the metadata about files in a given snapshot. Not all files can be returned at once, so the API
        is used repeatedly until all "batches" have been returned.
        
        Args:
            snapshot_id (str): The ID of the snapshot.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.
        
        Returns:
            list[dict]: A list of dictionaries containing the metadata of the files in the snapshot.

    `get_job_result(self, job_id: str, expect_failure: bool = False) ‑> requests.models.Response`
    :   Retrieve the result of a job.
        
        Args:
            job_id (str): The ID of the job.
            expect_failure (bool, optional): Whether the job is expected to fail. Defaults to False.
        
        Returns:
            dict: A dictionary containing the job result.

    `get_job_status(self, job_id: str) ‑> requests.models.Response`
    :   Retrieve the status of a job.
        
        Args:
            job_id (str): The ID of the job.
        
        Returns:
            requests.Response: The response object containing the job status.

    `get_or_create_dataset(self, dataset_name: str, billing_profile: str, schema: dict, description: str, cloud_platform: str, delete_existing: bool = False, continue_if_exists: bool = False, additional_properties_dict: dict | None = None) ‑> str`
    :   Get or create a dataset.
        
        Args:
            dataset_name (str): The name of the dataset.
            billing_profile (str): The billing profile ID.
            schema (dict): The schema of the dataset.
            description (str): The description of the dataset.
            cloud_platform (str): The cloud platform for the dataset.
            additional_properties_dict (Optional[dict], optional): Additional properties
                for the dataset. Defaults to None.
            delete_existing (bool, optional): Whether to delete the existing dataset if found.
                Defaults to False.
            continue_if_exists (bool, optional): Whether to continue if the dataset already exists.
                Defaults to False.
        
        Returns:
            str: The ID of the dataset.
        
        Raises:
            ValueError: If multiple datasets with the same name are found under the billing profile.

    `get_sas_token(self, snapshot_id: str = '', dataset_id: str = '') ‑> dict`
    :   Get the SAS token for a snapshot or dataset.
        
        Args:
            snapshot_id (str, optional): The ID of the snapshot. Defaults to "".
            dataset_id (str, optional): The ID of the dataset. Defaults to "".
        
        Returns:
            dict: A dictionary containing the SAS token and its expiry time.
        
        Raises:
            ValueError: If neither snapshot_id nor dataset_id is provided.

    `get_snapshot_info(self, snapshot_id: str, continue_not_found: bool = False, info_to_include: list[str] | None = None) ‑> dict`
    :   Get information about a snapshot.
        
        Args:
            snapshot_id (str): The ID of the snapshot.
            continue_not_found (bool, optional): Whether to accept a 404 response. Defaults to False.
            info_to_include (Optional[list[str]]): A list of additional information to include. Defaults to None.
                Options are: SOURCES, TABLES, RELATIONSHIPS, ACCESS_INFORMATION, PROFILE, PROPERTIES, DATA_PROJECT,
                CREATION_INFORMATION, DUOS
        
        Returns:
            dict: A dictionary containing the snapshot information.

    `get_table_schema_info(self, dataset_id: str, table_name: str, dataset_info: dict | None = None) ‑> dict | None`
    :   Get schema information for a specific table within a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table.
            dataset_info (dict, optional): The dataset information if already retrieved. Defaults to None.
        
        Returns:
            Union[dict, None]: A dictionary containing the table schema information, or None if the table is not found.

    `ingest_to_dataset(self, dataset_id: str, data: dict) ‑> dict`
    :   Load data into a TDR dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            data (dict): The data to be ingested.
        
        Returns:
            dict: A dictionary containing the response from the ingest operation.

    `remove_user_from_dataset(self, dataset_id: str, user: str, policy: str) ‑> None`
    :   Remove a user from a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            user (str): The email of the user to be removed.
            policy (str): The policy to be removed from the user.
                Must be one of "steward", "custodian", or "snapshot_creator".
        
        Raises:
            ValueError: If the policy is not valid.

    `soft_delete_all_table_entries(self, dataset_id: str, table_name: str, query_limit: int = 1000, check_intervals: int = 15) ‑> None`
    :   Soft deletes all records in a table.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.
            check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to 15.
        
        Returns:
            None

    `soft_delete_entries(self, dataset_id: str, table_name: str, datarepo_row_ids: list[str], check_intervals: int = 15) ‑> None`
    :   Soft delete specific records from a table.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the target table.
            datarepo_row_ids (list[str]): A list of row IDs to be deleted.
            check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to 15.
        
        Returns:
            None

    `update_dataset_schema(self, dataset_id: str, update_note: str, tables_to_add: list[dict] | None = None, relationships_to_add: list[dict] | None = None, columns_to_add: list[dict] | None = None) ‑> str | None`
    :   Update the schema of a dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            update_note (str): A note describing the update.
            tables_to_add (Optional[list[dict]], optional): A list of tables to add. Defaults to None.
            relationships_to_add (Optional[list[dict]], optional): A list of relationships to add. Defaults to None.
            columns_to_add (Optional[list[dict]], optional): A list of columns to add. Defaults to None.
        
        Returns:
            Optional[str]: The ID of the updated dataset, or None if the update failed.
        
        Raises:
            ValueError: If the schema validation fails.