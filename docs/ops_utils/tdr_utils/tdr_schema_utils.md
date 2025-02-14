Module ops_utils.tdr_utils.tdr_schema_utils
===========================================

Classes
-------

`InferTDRSchema(input_metadata: list[dict], table_name: str, all_fields_non_required: bool = False, allow_disparate_data_types_in_column: bool = False, primary_key: str | None = None)`
:   A class to infer the schema for a table in TDR (Terra Data Repository) based on input metadata.
    
    Initialize the InferTDRSchema class.
    
    Args:
        input_metadata (list[dict]): The input metadata to infer the schema from.
        table_name (str): The name of the table for which the schema is being inferred.
        all_fields_non_required (bool): A boolean indicating whether all columns should be set to non-required
            besides for primary key.
        primary_key (str): The name of the primary key column. Used to determine column should be required
        allow_disparate_data_types_in_column (bool): A boolean indicating whether force disparate data types in a
            column to be strings.

    ### Class variables

    `PYTHON_TDR_DATA_TYPE_MAPPING`
    :

    ### Methods

    `infer_schema(self) ‑> dict`
    :   Infer the schema for the table based on the input metadata.
        
        Returns:
            dict: The inferred schema in TDR format.