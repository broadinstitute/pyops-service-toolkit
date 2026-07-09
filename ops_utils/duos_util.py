"""Utilities for working with DUOS (Data Use Oversight System)."""
import json
import logging
import re

from .request_util import GET, POST, RunRequest

DUOS_PROD_LINK = "https://consent.dsde-prod.broadinstitute.org"
"""@private"""


class DUOS:
    """Class for interacting with the DUOS API."""

    def __init__(self, request_util: RunRequest):
        """
        Initialize the DUOS class.

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): An instance of a
            request utility class to handle HTTP requests.
        """
        self.request_util = request_util
        """@private"""

    def get_all_workspaces(self) -> list[dict]:
        """
        Fetch all workspaces registered in DUOS.

        Workspaces in DUOS are stored as assets under studies
        (`study.assets.workspaces[]`) within the ElasticSearch dataset index.
        Each workspace entry contains its name, platform, URL, access level, and tags.

        **Returns:**
        - list[dict]: A list of workspace dictionaries, each containing:
            - `study_name` (str): The name of the parent study.
            - `workspace_name` (str): The name of the workspace.
            - `platform` (str): The platform (e.g. `"Terra"`, `"AnVIL"`).
            - `url` (str): The workspace URL (e.g.
              `https://app.terra.bio/#workspaces/{namespace}/{name}`).
            - `access` (str): The access level (e.g. `"Open"`, `"Controlled"`).
            - `tags` (list[str]): Tags associated with the workspace.
        """
        query = {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "studies": {
                    "terms": {"field": "study.studyId", "size": 10000},
                    "aggs": {
                        "study_details": {
                            "top_hits": {"size": 1, "_source": ["study.*"]}
                        }
                    },
                }
            },
        }

        logging.info("Fetching all workspaces registered in DUOS")
        uri = f"{DUOS_PROD_LINK}/api/dataset/search/index/v2"
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            data=json.dumps(query),
            content_type="application/json",
        )
        data = response.json()
        buckets = data.get("aggregations", {}).get("studies", {}).get("buckets", [])

        workspaces = []
        for bucket in buckets:
            hits = bucket.get("study_details", {}).get("hits", {}).get("hits", [])
            if not hits:
                continue
            study = hits[0].get("_source", {}).get("study", {})
            study_name = study.get("studyName", "")
            for ws in study.get("assets", {}).get("workspaces", []):
                workspaces.append(
                    {
                        "study_name": study_name,
                        "workspace_name": ws.get("name"),
                        "platform": ws.get("platform"),
                        "url": ws.get("url", ""),
                        "access": ws.get("access"),
                        "tags": ws.get("tags", []),
                    }
                )

        logging.info(f"Found {len(workspaces)} workspaces registered in DUOS")
        return workspaces

    def get_workspace_lookup_by_namespace_and_name(self) -> dict[tuple[str, str], dict]:
        """
        Build a lookup map of all DUOS-registered workspaces keyed by `(namespace, name)`.

        Parses the workspace URL from each entry returned by `get_all_workspaces` to
        extract the Terra namespace and workspace name.

        **Returns:**
        - dict[tuple[str, str], dict]: A dictionary mapping `(namespace, name)` tuples
            to workspace info dicts (same structure as returned by `get_all_workspaces`).
            Workspaces whose URLs do not match the expected pattern are excluded.
        """
        lookup: dict[tuple[str, str], dict] = {}
        workspace_url_patters = re.compile(r"https?://[^/]+/#workspaces/([^/]+)/(.+)")
        for ws in self.get_all_workspaces():
            match = workspace_url_patters.match(ws.get("url", ""))
            if not match:
                continue
            key = (match.group(1), match.group(2))
            lookup[key] = ws
        return lookup
