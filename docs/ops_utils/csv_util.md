Module ops_utils.csv_util
=========================

Classes
-------

`Csv(file_path: str, delimiter: str = '\t')`
:   Initialize the Csv class.
    
    Args:
        file_path (str): The path to the CSV file.
        delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '      '.

    ### Methods

    `create_list_of_dicts_from_tsv(self, expected_headers: list[str] | None = None, allow_extra_headers: bool = False) ‑> list[dict]`
    :   Create a list of dictionaries from a TSV file.
        
        Args:
            expected_headers (Optional[list[str]], optional): The list of expected headers. If provided
                will check that all headers are present in the TSV file. Defaults to None.
            allow_extra_headers (bool, optional): Whether to allow extra headers in the TSV file.
                Only used if expected_headers is provided. Defaults to False.
        
        Returns:
            list[dict]: The list of dictionaries created from the TSV file.
        
        Raises:
            ValueError: If the expected headers are not found in the TSV file.

    `create_list_of_dicts_from_tsv_with_no_headers(self, headers_list: list[str]) ‑> list[dict]`
    :   Create a list of dictionaries from a TSV file with no headers.
        
        Args:
            headers_list (list[str]): The list of headers to use for the TSV file.
        
        Returns:
            list[dict]: The list of dictionaries created from the TSV file.

    `create_tsv_from_list_of_dicts(self, list_of_dicts: list[dict], header_list: list[str] | None = None) ‑> str`
    :   Create a TSV file from a list of dictionaries.
        
        Args:
            list_of_dicts (list[dict]): The list of dictionaries to write to the TSV file.
            header_list (Optional[list[str]], optional): The list of headers to use in the TSV file.
                If provided output columns will be in same order as list. Defaults to None.
        
        Returns:
            str: The path to the created TSV file.

    `create_tsv_from_list_of_lists(self, list_of_lists: list[list]) ‑> str`
    :   Create a TSV file from a list of lists.
        
        Args:
            list_of_lists (list[list]): The list of lists to write to the TSV file.
        
        Returns:
            str: The path to the created TSV file.

    `get_header_order_from_tsv(self) ‑> Sequence[str] | None`
    :   Get the header order from a TSV file.
        
        Returns:
            list[str]: The list of headers in the TSV file.