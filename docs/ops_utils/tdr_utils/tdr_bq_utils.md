Module ops_utils.tdr_utils.tdr_bq_utils
=======================================

Classes
-------

`GetTdrAssetInfo(tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, dataset_id: str | None = None, snapshot_id: str | None = None)`
:   Initialize the GetTdrAssetInfo class.
    
    Args:
        tdr (TDR): TDR instance for interacting with the TDR API.
        dataset_id (Optional[str]): ID of the dataset.
        snapshot_id (Optional[str]): ID of the snapshot.

    ### Methods

    `run(self) ‑> dict`
    :   Execute the process to retrieve either dataset or snapshot information.
        
        Returns:
            dict: A dictionary containing the relevant information based on whether dataset_id or snapshot_id is provided.

`TdrBq(project_id: str, bq_schema: str)`
:   Initialize the TdrBq class.
    
    Args:
        project_id (str): The Google Cloud project ID.
        bq_schema (str): The BigQuery schema name.

    ### Methods

    `check_permissions_for_dataset(self, raise_on_other_failure: bool) ‑> bool`
    :   Check the permissions for accessing BigQuery for specific dataset.
        
        Args:
            raise_on_other_failure (bool): Whether to raise an exception on other failures.
        
        Returns:
            bool: True if permissions are sufficient, False otherwise.

    `get_tdr_table_contents(self, exclude_datarepo_id: bool, table_name: str, to_dataframe: bool) ‑> Any`
    :   Retrieve the contents of a TDR table from BigQuery.
        
        Args:
            exclude_datarepo_id (bool): Whether to exclude the datarepo_row_id column.
            table_name (str): The name of the table.
            to_dataframe (bool): Whether to return the results as a DataFrame.
        
        Returns:
            Any: The contents of the table, either as a DataFrame or another format.