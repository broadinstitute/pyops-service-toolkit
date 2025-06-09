"""Module to interact with Google Cloud Functions API."""
from google.auth import default
from googleapiclient.discovery import build
import logging
import json
from typing import Optional

ERROR_KEY = "error"

class GCPCloudFunctionCaller:
    """Class to call a GCP Cloud Function programmatically."""

    def __init__(self, project: Optional[str] = None) -> None:
        """
        Initialize the GCPCloudFunctionCaller class.

        **Args:**
        - project (str, optional): The GCP project ID. Defaults to the active project.
        """
        credentials, default_project = default()
        self.project = project or default_project
        self.service = build("cloudfunctions", "v1", credentials=credentials)

    def call_function(
            self,
            function_name: str,
            data: dict,
            check_error: bool = True,
    ) -> dict:
        """
        Call a GCP Cloud Function.

        **Args:**
        - function_name (str): The name of the Cloud Function to call.
        - data (dict): The payload to send to the Cloud Function.
        - check_error (bool): Whether to check for errors in the response. Defaults to True.

        **Returns:**
        - dict: The response from the Cloud Function.
        """
        function_path = f"projects/{self.project}/locations/us-central1/functions/{function_name}"
        request = self.service.projects().locations().functions().call(
            name=function_path, body={"data": json.dumps(data)}
        )

        response = request.execute()
        if check_error and ERROR_KEY in response:
            raise RuntimeError(f"Error calling Cloud Function {function_name}: {response[ERROR_KEY]}")
        logging.info(f"Cloud Function {function_name} called successfully.")
        return response