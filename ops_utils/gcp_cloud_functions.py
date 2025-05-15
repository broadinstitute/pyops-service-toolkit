from google.auth import default
from googleapiclient.discovery import build
import logging
from typing import Optional


class GCPCloudFunctionCaller:
    """
    Class to call a GCP Cloud Function programmatically.
    """

    def __init__(self, project: Optional[str] = None) -> None:
        """
        Initialize the GCPCloudFunctionCaller class.

        **Args:**
        - project (str, optional): The GCP project ID. Defaults to the active project.
        """
        credentials, default_project = default()
        self.project = project or default_project
        self.service = build("cloudfunctions", "v1", credentials=credentials)

    def call_function(self, function_name: str, data: dict) -> dict:
        """
        Call a GCP Cloud Function.

        **Args:**
        - function_name (str): The name of the Cloud Function to call.
        - data (dict): The payload to send to the Cloud Function.

        **Returns:**
        - dict: The response from the Cloud Function.
        """
        function_path = f"projects/{self.project}/locations/-/functions/{function_name}"
        request = self.service.projects().locations().functions().call(
            name=function_path, body={"data": data}
        )

        try:
            response = request.execute()
            logging.info(f"Cloud Function {function_name} called successfully.")
            return response
        except Exception as e:
            logging.error(f"Failed to call Cloud Function {function_name}: {e}")
            raise