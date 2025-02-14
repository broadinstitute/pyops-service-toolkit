Module ops_utils.tdr_utils.tdr_table_utils
==========================================

Classes
-------

`SetUpTDRTables(tdr: ops_utils.tdr_utils.tdr_api_utils.TDR, dataset_id: str, table_info_dict: dict, all_fields_non_required: bool = False, force_disparate_rows_to_string: bool = False, ignore_existing_schema_mismatch: bool = False)`
:   A class to set up TDR tables by comparing and updating schemas.
    
    Attributes:
        tdr (TDR): An instance of the TDR class.
        dataset_id (str): The ID of the dataset.
        table_info_dict (dict): A dictionary containing table information.
    
    Initialize the SetUpTDRTables class.
    
    Args:
        tdr (TDR): An instance of the TDR class.
        dataset_id (str): The ID of the dataset.
        table_info_dict (dict): A dictionary containing table information.
        all_fields_non_required (bool): A boolean indicating whether all columns are non-required.
        force_disparate_rows_to_string (bool): A boolean indicating whether disparate rows should be forced to
            string.
        ignore_existing_schema_mismatch (bool): A boolean indicating whether to not fail on data type not
            matching existing schema.

    ### Methods

    `run(self) ‑> dict`
    :   Run the setup process to ensure tables are created or updated as needed.
        
        Returns:
            dict: A dictionary with table names as keys and column information as values.