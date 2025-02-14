Module ops_utils.bq_utils
=========================

Classes
-------

`BigQueryUtil(project_id: str)`
:   Initialize the BigQuery utility with user authentication.

    ### Methods

    `check_permissions_for_query(self, query: str, raise_on_other_failure: bool = True) ‑> bool`
    :   Checks if the user has permission to run a specific query.
        
        Args:
            query (str): SQL query to execute.
            raise_on_other_failure (bool): If True, raises an error if an unexpected error occurs. Default is True.
        
        Returns:
            bool: True if the user has permissions, False if a 403 Forbidden error is encountered.

    `check_permissions_to_project(self, raise_on_other_failure: bool = True) ‑> bool`
    :   Checks if the user has permission to access the project.
        
        Args:
            raise_on_other_failure (bool): If True, raises an error if an unexpected error occurs. Default is True.
        
        Returns:
            bool: True if the user has permissions, False if a 403 Forbidden error is encountered.

    `query_table(self, query: str, to_dataframe: bool = False) ‑> Any`
    :   Executes a SQL query on a BigQuery table and returns the results .
        
        Args:
            query (str): SQL query to execute.
            to_dataframe (bool): If True, returns the query results as a Pandas DataFrame. Default is False.
        
        Returns:
            list[dict]: List of dictionaries, where each dictionary represents a row of query results.

    `upload_data_to_table(self, table_id: str, rows: list[dict]) ‑> None`
    :   Uploads data directly from a list of dictionaries to a BigQuery table.
        
        Args:
            table_id (str): BigQuery table ID in the format 'project.dataset.table'.
            rows (list[dict]): List of dictionaries, where each dictionary represents a row of data.