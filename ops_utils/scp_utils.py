import requests

from .vars import GCP, APPLICATION_JSON
from .gcp_utils import GCPCloudFunctions
from .request_util import GET, POST, PATCH, PUT, DELETE, RunRequest

class SCP:
    PROD_LINK = "https://singlecell.broadinstitute.org/single_cell/api/v1"

    def __init__(self, request_util: RunRequest, env: str = 'prod'):
        """
        Initialize SCP class

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): Utility for making HTTP requests.
        """
        self.request_util = request_util
        if env.lower() == 'prod':
            self.tdr_link = self.PROD_LINK
        else:
            raise RuntimeError(f"Unsupported environment: {env}. Must be 'prod'.")
        """@private"""

    def find_studies(self) -> requests.Response:
        """
        Find all studies in SCP.

        **Returns:**
        - `requests.Response`: The HTTP response object containing the list of studies.
        """
        url = f"{self.tdr_link}/site/studies"
        return self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)

    def get_study_information(self, study: str) -> requests.Response:
        """
        Get study information from SCP.

        **Returns:**
        - `requests.Response`: The HTTP response object containing the study information.
        """
        url = f"{self.tdr_link}/site/studies/{study}"
        return self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)

