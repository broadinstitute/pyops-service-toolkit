Module ops_utils.tdr_utils.tdr_ingest_utils
===========================================

Classes
-------

`BatchIngest(ingest_metadata: list[dict], tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, target_table_name: str, dataset_id: str, batch_size: int, bulk_mode: bool, cloud_type: str, terra_workspace: ops_utils.terra_utils.terra_util.TerraWorkspace | None = None, update_strategy: str = 'replace', waiting_time_to_poll: int = 60, sas_expire_in_secs: int = 3600, test_ingest: bool = False, load_tag: str | None = None, file_to_uuid_dict: dict | None = None, schema_info: dict | None = None, skip_reformat: bool = False)`
:   A class to handle batch ingestion of metadata into TDR (Terra Data Repository).
    
    Initialize the BatchIngest class.
    
    Args:
        ingest_metadata (list[dict]): The metadata to be ingested.
        tdr (TDR): An instance of the TDR class.
        target_table_name (str): The name of the target table.
        dataset_id (str): The ID of the dataset.
        batch_size (int): The size of each batch for ingestion.
        bulk_mode (bool): Flag indicating if bulk mode should be used.
        cloud_type (str): The type of cloud (GCP or AZURE).
        terra_workspace (Optional[TerraWorkspace], optional): An instance of the TerraWorkspace class. Used for Azure
            ingests so sas token can be created. Defaults to None.
        update_strategy (str, optional): The strategy for updating existing records. Defaults to "replace".
        waiting_time_to_poll (int, optional): The time to wait between polling for job status. Defaults to 60.
        sas_expire_in_secs (int, optional): The expiration time for SAS tokens in seconds.
            Azure only. Defaults to 3600.
        test_ingest (bool, optional): Flag indicating if only the first batch should be
            ingested for testing. Defaults to False.
        load_tag (Optional[str], optional): A tag to identify the load. Used so future ingests
            can pick up where left off. Defaults to None.
        file_to_uuid_dict (Optional[dict], optional): A dictionary mapping source file paths to UUIDs. If used
            will make ingest much quicker since no ingest or look up of file needed. Defaults to None.
        schema_info (Optional[dict], optional): Schema information for the tables. Validates ingest data matches up
            with schema info. Defaults to None.
        skip_reformat (bool, optional): Flag indicating if reformatting should be skipped. Defaults to False.

    ### Methods

    `run(self) ‑> None`
    :   Run the batch ingestion process.

`ConvertTerraTableInfoForIngest(table_metadata: list[dict], tdr_row_id: str = 'sample_id', columns_to_ignore: list[str] = [])`
:   Converts each row of table metadata into a dictionary that can be ingested into TDR.
    
    Input looks like
    [{
      "attributes": {
        "some_metric": 99.99,
        "some_file_path": "gs://path/to/file",
        "something_to_exclude": "exclude_me"
      },
      "entityType": "sample",
      "name": "SM-MVVVV"
    }]
    
    converts to
    
    [{
      "sample_id": "SM-MVVVV",
      "some_metric": 99.99,
      "some_file_path": "gs://path/to/file"
    }]
    
    Initialize the ConvertTerraTableInfoForIngest class.
    
    Args:
        table_metadata (list[dict]): The metadata of the table to be converted.
        tdr_row_id (str): The row ID to be used in the TDR. Defaults to 'sample_id'.
        columns_to_ignore (list[str]): List of columns to ignore during conversion. Defaults to an empty list.

    ### Methods

    `run(self) ‑> list[dict]`
    :   Convert the table metadata into a format suitable for TDR ingestion.
        
        Returns:
            list[dict]: A list of dictionaries containing the converted table metadata.

`FilterAndBatchIngest(tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, filter_existing_ids: bool, unique_id_field: str, table_name: str, ingest_metadata: list[dict], dataset_id: str, ingest_waiting_time_poll: int, ingest_batch_size: int, bulk_mode: bool, cloud_type: str, update_strategy: str, load_tag: str, test_ingest: bool = False, file_to_uuid_dict: dict | None = None, sas_expire_in_secs: int = 3600, schema_info: dict | None = None, terra_workspace: ops_utils.terra_utils.terra_util.TerraWorkspace | None = None, skip_reformat: bool = False)`
:   Initialize the FilterAndBatchIngest class.
    
    Args:
        tdr (TDR): Instance of the TDR class.
        filter_existing_ids (bool): Whether to filter out sample IDs that already exist in the dataset.
        unique_id_field (str): The unique ID field to filter on.
        table_name (str): The name of the table to ingest data into.
        ingest_metadata (list[dict]): The metadata to ingest.
        dataset_id (str): The ID of the dataset.
        ingest_waiting_time_poll (int): The waiting time to poll for ingest status.
        ingest_batch_size (int): The batch size for ingest.
        bulk_mode (bool): Whether to use bulk mode for ingest.
        cloud_type (str): The type of cloud (e.g., GCP, AZURE).
        update_strategy (str): The update strategy to use.
        load_tag (str): The load tag for the ingest. Used to make future ingests of same files go faster.
        test_ingest (bool, optional): Whether to run a test ingest. Defaults to False.
        file_to_uuid_dict (Optional[dict], optional): A dictionary mapping source files to UUIDs.
            If supplied makes ingest run faster due to just linking to already ingested file UUID. Defaults to None.
        sas_expire_in_secs (int, optional): The expiration time for SAS tokens in seconds.
            Azure only. Defaults to 3600.
        schema_info (Optional[dict], optional): Schema information for the tables.
            Used to validate ingest metrics match. Defaults to None.
        terra_workspace (Optional[TerraWorkspace], optional): Instance of the TerraWorkspace class.
            Only used for azure ingests to get token. Defaults to None.
        skip_reformat (bool, optional): Whether to skip reformatting of metrics. Defaults to False.

    ### Methods

    `run(self) ‑> None`
    :   Run the filter and batch ingest process.
        
        This method filters out sample IDs that already exist in the dataset (if specified),
        and then performs a batch ingest of the remaining metadata into the specified table.

`GetPermissionsForWorkspaceIngest(terra_workspace: ops_utils.terra_utils.terra_util.TerraWorkspace, dataset_info: dict, added_to_auth_domain: bool = False)`
:   Initialize the GetPermissionsForWorkspaceIngest class.
    
    Args:
        terra_workspace (TerraWorkspace): Instance of the TerraWorkspace class.
        dataset_info (dict): Information about the dataset.
        added_to_auth_domain (bool, optional): Flag indicating if the SA account
            has been added to the auth domain. Defaults to False.

    ### Methods

    `run(self) ‑> None`
    :   Ensure the dataset SA account has the necessary permissions on the Terra workspace.
        
        This method updates the user ACL to make the SA account a reader on the Terra workspace.
        It also checks if the workspace has an authorization domain and logs the
        necessary steps to add the SA account to the auth domain.

`ReformatMetricsForIngest(ingest_metadata: list[dict], cloud_type: str, storage_container: str | None = None, sas_token_string: str | None = None, file_to_uuid_dict: dict | None = None, schema_info: dict | None = None)`
:   Reformat metrics for ingest.
    Assumes input JSON for that will be
    like below or similar for Azure:
    {
        "file_name": blob.name,
        "file_path": f"gs://{self.bucket_name}/{blob.name}",
        "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
        "file_extension": os.path.splitext(blob.name)[1],
        "size_in_bytes": blob.size,
        "md5_hash": blob.md5_hash
    }
    
    Initialize the ReformatMetricsForIngest class.
    
    Args:
        ingest_metadata (list[dict]): The metadata to be ingested.
        cloud_type (str): The type of cloud (GCP or AZURE).
        storage_container (Optional[str], optional): The storage container name. For Azure only. Defaults to None.
        sas_token_string (Optional[str], optional): The SAS token string for Azure. Defaults to None.
        file_to_uuid_dict (Optional[dict], optional): A dictionary mapping file paths to UUIDs. Speeds up ingest
            dramatically as it can skip uploading files or looking up file UUIDs in TDR. Defaults to None.
        schema_info (Optional[dict], optional): Schema information for the tables. Defaults to None.

    ### Methods

    `run(self) ‑> list[dict]`
    :   Run the reformatting process for all metrics.
        
        Returns:
            list[dict]: A list of reformatted metrics.

`StartAndMonitorIngest(tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, ingest_records: list[dict], target_table_name: str, dataset_id: str, load_tag: str, bulk_mode: bool, update_strategy: str, waiting_time_to_poll: int)`
:   A class to start and monitor the ingestion of records into a TDR dataset.
    
    Initialize the StartAndMonitorIngest class.
    
    Args:
        tdr (TDR): An instance of the TDR class.
        ingest_records (list[dict]): The records to be ingested.
        target_table_name (str): The name of the target table.
        dataset_id (str): The ID of the dataset.
        load_tag (str): A tag to identify the load.
        bulk_mode (bool): Flag indicating if bulk mode should be used.
        update_strategy (str): The strategy for updating existing records.
        waiting_time_to_poll (int): The time to wait between polling for job status.

    ### Methods

    `run(self) ‑> None`
    :   Run the ingestion process and monitor the job until completion.