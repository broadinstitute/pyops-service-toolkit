Module ops_utils.requests_utils.request_util
============================================

Classes
-------

`RunRequest(token: Any, max_retries: int = 5, max_backoff_time: int = 300, create_mocks: bool = False)`
:   Initialize the RunRequest class.
    
    Args:
        token (Any): The token used for authentication.
        max_retries (int, optional): The maximum number of retries for a request. Defaults to 5.
        max_backoff_time (int, optional): The maximum backoff time in seconds. Defaults to 5 * 60.
        create_mocks (bool, optional): Used to capture responses for use with unit tests,
            outputs to a yaml file. Defaults to False.

    ### Methods

    `create_headers(self, content_type: str | None = None, accept: str | None = 'application/json') ‑> dict`
    :   Create headers for API calls.
        
        Args:
            content_type (Optional[str], optional): The content type for the request. Defaults to None.
            accept (Optional[str], optional): The accept header for the request. Defaults to "application/json".
        
        Returns:
            dict: The headers for the request.

    `run_request(self, uri: str, method: str, data: Any = None, params: dict | None = None, factor: int = 15, content_type: str | None = None, accept_return_codes: list[int] = []) ‑> requests.models.Response`
    :   Run an HTTP request with retries and backoff.
        
        Args:
            uri (str): The URI for the request.
            method (str): The HTTP method (GET, POST, DELETE, PATCH, PUT).
            data (Any, optional): The data to send in the request body. Defaults to None.
            params (Optional[dict], optional): The query parameters for the request. Defaults to None.
            factor (int, optional): The exponential backoff factor. Defaults to 15.
            content_type (Optional[str], optional): The content type for the request. Defaults to None.
            accept_return_codes (list[int], optional): List of acceptable return codes. Defaults to [].
        
        Returns:
            requests.Response: The response from the request.

    `upload_file(self, uri: str, data: dict) ‑> str`
    :   Run a POST request with files parameter.
        
        Args:
            uri (str): The URI for the request.
            data (dict): The files data to upload.
        
        Returns:
            str: The response text from the request.