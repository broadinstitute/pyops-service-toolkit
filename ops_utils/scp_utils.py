import requests

from .vars import APPLICATION_JSON
from .request_util import GET, RunRequest
import json
import logging

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
            self.scp_link = self.PROD_LINK
        else:
            raise RuntimeError(f"Unsupported environment: {env}. Must be 'prod'.")
        """@private"""

    def find_all_available_studies(self) -> requests.Response:
        """
        Find all studies in SCP.

        **Returns:**
        - `requests.Response`: The HTTP response object containing the list of studies.
        """
        url = f"{self.scp_link}/site/studies"
        return self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)

    def search_studies(self, type: str, facets: str | None = None, return_all_pages: bool = False, pages_before_logging: int = 10) -> requests.Response:
        """Search studies in SCP.

        **Args:**
        - type (`str`): Can be 'gene' or 'study'
        - facets (`str`): For human use NCBITaxon_9606
        - return_all_pages (`bool`): Whether to return all pages or only the first page.
            If you just want study list that will be all listed in first page under 'matching_accessions'
        - pages_before_logging (`int`): Number of pages before logging study list

        **Returns:**
        - `requests.Response`: The HTTP response object containing study information from all pages."""
        if type not in ['gene', 'study']:
            raise ValueError(f"Invalid type: {type}. Must be 'gene' or 'study'.")

        facets_link = f"&facets={facets}" if facets else ""

        url = f"{self.scp_link}/search?type={type}{facets_link}"
        response = self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)

        if not return_all_pages:
            return response
        response_json = response.json()
        total_pages = int(response_json.get("total_pages") or 1)
        if total_pages > 1:
            logging.info(f"Found {total_pages} pages, going to loop through all of them.")

        result_keys = ["studies", "results", "search_results", "items"]
        paged_keys = [key for key in result_keys if isinstance(response_json.get(key), list)]
        if not paged_keys:
            paged_keys = [
                key for key, value in response_json.items()
                if isinstance(value, list) and key not in ["facets", "term_list"]
            ]

        for page in range(2, total_pages + 1):
            if page % pages_before_logging == 0:
                logging.info(f"Processing page {page}")
            page_url = f"{url}&page={page}"
            page_response = self.request_util.run_request(method=GET, uri=page_url, content_type=APPLICATION_JSON)
            page_json = page_response.json()

            for key in paged_keys:
                response_json[key].extend(page_json.get(key, []))

            response_json["current_page"] = page_json.get("current_page", page)

        response._content = json.dumps(response_json).encode(response.encoding or "utf-8")
        return response

    def get_study_information(self, study: str) -> requests.Response:
        """
        Get study information from SCP.

        **Returns:**
        - `requests.Response`: The HTTP response object containing the study information.
        """
        url = f"{self.scp_link}/site/studies/{study}"
        return self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)

    def download_file(
            self,
            file_name: str,
            study: str,
            destination: str | None = None,
            chunk_size: int = 1024 * 1024
    ) -> requests.Response | str:
        """
        Download file from SCP.

        If destination is provided, stream the download directly to that file path instead
        of loading the full file into memory. If destination is not provided, return the
        response object with the file contents loaded, preserving the previous behavior.

        To access file contents from the returned response example below:

                Raw bytes (e.g. for binary files like .h5ad / AnnData)
                content_bytes = response.content
                with open(file['name'], "wb") as f:
                    f.write(response.content)

        **Args:**
        - file_name (`str`): File name
        - study (`str`): study
        - destination (`str`, optional): Local file path to stream the download to.
            Defaults to None.
        - chunk_size (`int`, optional): Number of bytes to write per chunk when streaming.
            Defaults to 1 MiB.

        **Returns:**
        - `requests.Response | str`: The HTTP response object if destination is not provided,
            otherwise the destination file path.
        """
        url = f"{self.scp_link}/site/studies/{study}/download?filename={file_name}"
        if destination:
            return self.request_util.download_file(
                uri=url,
                destination=destination,
                chunk_size=chunk_size
            )
        return self.request_util.run_request(method=GET, uri=url, content_type=APPLICATION_JSON)
