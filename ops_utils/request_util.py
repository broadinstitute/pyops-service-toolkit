"""Module to handle web requests."""
from typing import Any, Optional
import requests
import backoff

from .token_util import Token
from .vars import ARG_DEFAULTS

GET = "GET"
"""Method used for API GET endpoints"""
POST = "POST"
"""Method used for API POST endpoints"""
DELETE = "DELETE"
"""Method used for API DELETE endpoints"""
PATCH = "PATCH"
"""Method used for API PATCH endpoints"""
PUT = "PUT"
"""Method used for API PUT endpoints"""


class RunRequest:
    """Class to handle web requests with retries and backoff."""

    def __init__(
            self,
            token: Token,
            max_retries: int = ARG_DEFAULTS["max_retries"],  # type: ignore[assignment]
            max_backoff_time: int = ARG_DEFAULTS["max_backoff_time"],  # type: ignore[assignment]
    ):
        """
        Initialize the RunRequest class.

        **Args:**
        - token (`ops_utils.token_util.Token`): The token used for authentication
        - max_retries (int, optional): Maximum number of retries for a request. Defaults to `5`.
        - max_backoff_time (int, optional): Maximum backoff time for a request (in seconds). Defaults to `300`.
        """
        self.token = token
        """@private"""
        self.max_retries = max_retries
        """@private"""
        self.max_backoff_time = max_backoff_time
        """@private"""

    @staticmethod
    def _create_backoff_decorator(max_tries: int, factor: int, max_time: int) -> Any:
        """
        Create a backoff decorator with the specified parameters.

        Args:
            max_tries (int): The maximum number of tries.
            factor (int): The exponential backoff factor.
            max_time (int): The maximum backoff time in seconds.

        Returns:
            Any: The backoff decorator.
        """
        return backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=max_tries,
            factor=factor,
            max_time=max_time
        )

    def run_request(
            self,
            uri: str,
            method: str,
            data: Any = None,
            files: Any = None,
            params: Optional[dict] = None,
            factor: int = 15,
            content_type: Optional[str] = None,
            accept_return_codes: list[int] = []
    ) -> requests.Response:
        """
        Run an HTTP request with retries and backoff.

        **Args:**
        - uri (str): The URI for the request.
        - method (str): The HTTP method (must be one of `GET`, `POST`, `DELETE`, `PATCH`, `PUT`).
        - data (Any, optional): The data to send in the request body. Defaults to None.
        - params (dict, optional): The query parameters for the request. Defaults to None.
        - factor (int, optional): The exponential backoff factor. Defaults to 15.
        - content_type (str, optional): The content type for the request. Defaults to None.
        - accept_return_codes (list[int], optional): List of acceptable return codes. Defaults to [].

        **Returns:**
        - requests.Response: The response from the request.
        """
        # Create a custom backoff decorator with the provided parameters
        backoff_decorator = self._create_backoff_decorator(
            max_tries=self.max_retries,
            factor=factor,
            max_time=self.max_backoff_time
        )

        # Apply decorators to request execution
        @backoff_decorator
        def _make_request() -> requests.Response:
            if method == GET:
                response = requests.get(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    params=params
                )
            elif method == POST:
                if files:
                    headers = self.create_headers(content_type=content_type)
                    print(headers)
                    print(uri)
                    print(files)
                    response = requests.post(
                        uri,
                        headers=headers, #self.create_headers(content_type=content_type),
                        files=files
                    )
                else:
                    response = requests.post(
                        uri,
                        headers=self.create_headers(content_type=content_type),
                        data=data
                    )
            elif method == DELETE:
                response = requests.delete(
                    uri,
                    headers=self.create_headers(content_type=content_type)
                )
            elif method == PATCH:
                response = requests.patch(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    data=data
                )
            elif method == PUT:
                response = requests.put(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    data=data
                )
            else:
                raise ValueError(f"Method {method} is not supported")
            # Raise an exception for non-200 status codes and codes not in acceptable_return_codes
            if ((300 <= response.status_code or response.status_code < 200)
                    and response.status_code not in accept_return_codes):
                print(response.text)
                response.raise_for_status()  # Raise an exception for non-200 status codes
            return response

        return _make_request()

    def create_headers(self, content_type: Optional[str] = None, accept: Optional[str] = "application/json") -> dict:
        """
        Create headers for API calls.

        **Args:**
        - content_type (str, optional): The content type for the request. Defaults to None.
        - accept (str, optional): The accept header for the request. Defaults to "application/json".

        **Returns:**
        - dict: The headers for the request.
        """
        self.token.get_token()
        headers = {
            "Authorization": f"Bearer {self.token.token_string}",
            "accept": accept
        }
        if content_type:
            headers["Content-Type"] = content_type
        if accept:
            headers["accept"] = accept
        return headers

    def upload_file(self, uri: str, data: dict) -> requests.Response:
        """
        Run a POST request with files parameter.

        **Args:**
        - uri (str): The URI for the request.
        - data (dict): The files data to upload.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.run_request(
            uri=uri,
            method=POST,
            files=data
        )
