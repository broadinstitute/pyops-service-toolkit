Module ops_utils.tdr_utils.renaming_util
========================================

Classes
-------

`BatchCopyAndIngest(rows_to_ingest: list[dict], tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, target_table_name: str, cloud_type: str, update_strategy: str, workers: int, dataset_id: str, copy_and_ingest_batch_size: int, row_files_to_copy: list[list[dict]], wait_time_to_poll: int = 90)`
:   Initialize the BatchCopyAndIngest class.
    
    Args:
        rows_to_ingest (list[dict]): List of rows to ingest.
        tdr (TDR): TDR instance for interacting with the TDR API.
        target_table_name (str): Name of the target table.
        cloud_type (str): Type of cloud storage.
        update_strategy (str): Strategy for updating the data.
        workers (int): Number of workers for parallel processing copies of files to temp location.
        dataset_id (str): ID of the dataset.
        copy_and_ingest_batch_size (int): Size of each batch for copying and ingesting.
        row_files_to_copy (list[list[dict]]): List of files to copy for each row.
        wait_time_to_poll (int, optional): Time to wait between polling for ingest status.
            Defaults to ARG_DEFAULTS['waiting_time_to_poll'].

    ### Methods

    `run(self) ‑> None`
    :   Run the batch copy and ingest process.
        
        This method batches the rows to copy files and ingest them into the dataset. It also
        deletes the temporary files after ingestion.

`GetRowAndFileInfoForReingest(table_schema_info: dict, files_info: dict, table_metrics: list[dict], original_column: str, new_column: str, row_identifier: str, temp_bucket: str, update_original_column: bool = False, column_update_only: bool = False)`
:   Initialize the GetRowAndFileInfoForReingest class.
    
    Args:
        table_schema_info (dict): Schema information of the table.
        files_info (dict): A dictionary where the key is the file UUID and the value is the file metadata.
        table_metrics (list[dict]): Metrics of the TDR table to update.
        original_column (str): The column name with the original value.
        new_column (str): The column name with the new value.
        row_identifier (str): The identifier for the row. Should be the primary key.
        temp_bucket (str): The temporary bucket for storing files.
        update_original_column (bool, optional): Whether to update the original column.
            If not used will just update file paths Defaults to False.
        column_update_only (bool, optional): Whether to update only the column and
            not update the file paths. Defaults to False.

    ### Methods

    `get_new_copy_and_ingest_list(self) ‑> Tuple[list[dict], list[list]]`
    :   Get the list of rows to re-ingest and files to copy to temporary storage.
        
        This method iterates through the table metrics, identifies the rows and files that need to be re-ingested,
        and prepares lists of these rows and files.
        
        Returns:
            Tuple[list[dict], list[list]]: A tuple containing a list of rows to re-ingest and a list of files to copy.