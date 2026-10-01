"""Utilities for working with DUOS (Data Use Oversight System)."""
import json
import logging
import re

import requests

from .request_util import POST, RunRequest

DUOS_PROD_LINK = "https://consent.dsde-prod.broadinstitute.org"
"""@private"""
DUOS_DEV_LINK = "https://consent.dsde-dev.broadinstitute.org"
"""@private"""

class DUOS:
    """Class for interacting with the DUOS API."""

    def __init__(self, request_util: RunRequest, env: str = "prod") -> None:
        """
        Initialize the DUOS class.

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): An instance of a
            request utility class to handle HTTP requests.
        """
        self.request_util = request_util
        if env == "prod":
            self.duos_link = DUOS_PROD_LINK
        elif env == "dev":
            self.duos_link = DUOS_DEV_LINK
        else:
            raise ValueError(f"Invalid DUOS environment: {env}. Must be 'prod' or 'dev'.")
        """@private"""

    def search_dataset_index(self, query: dict) -> requests.Response:
        """
        Search the DUOS ElasticSearch dataset index.

        **Args:**
        - query (dict): An ElasticSearch query object.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.duos_link}/api/dataset/search/index/v2",
            method=POST,
            data=json.dumps(query),
            content_type="application/json",
        )

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
        data = self.search_dataset_index(query).json()
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

    def find_studies_by_phs_id(self, phs_id: str) -> list[dict]:
        """
        Find all DUOS studies registered for a dbGaP PHS accession.

        Matches on the `study.phsId` field of the dataset search index, ignoring case and
        dbGaP version, so searching for `"phs002591"` will also match a DUOS study registered
        with `"phs002591.v2.p1"`. It is left to the caller to decide what to do if more than
        one study is returned (e.g. treat it as an error, or pick one).

        **Args:**
        - phs_id (str): The dbGaP PHS accession to search for, with or without a version
            (e.g. `"phs002591"` or `"phs002591.v2.p1"`).

        **Returns:**
        - list[dict]: The matching studies (the `study.*` fields from the search index,
            including `studyId`, `studyName`, and `phsId`), or an empty list if no DUOS
            study has this PHS ID.
        """
        base_phs_id = phs_id.strip().split(".")[0]
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "should": [
                        {"wildcard": {"study.phsId.keyword": {"value": base_phs_id, "case_insensitive": True}}},
                        {"wildcard": {"study.phsId.keyword": {"value": f"{base_phs_id}.*", "case_insensitive": True}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "aggs": {
                "studies": {
                    "terms": {"field": "study.studyId", "size": 10000},
                    "aggs": {
                        "study_details": {"top_hits": {"size": 1, "_source": ["study.*"]}}
                    },
                }
            },
        }

        logging.info(f"Searching DUOS for studies with PHS ID '{phs_id}'")
        data = self.search_dataset_index(query).json()
        buckets = data.get("aggregations", {}).get("studies", {}).get("buckets", [])

        studies = []
        for bucket in buckets:
            hits = bucket.get("study_details", {}).get("hits", {}).get("hits", [])
            if hits:
                studies.append(hits[0].get("_source", {}).get("study", {}))

        logging.info(f"Found {len(studies)} DUOS study(s) for PHS ID '{phs_id}'")
        return studies

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
        workspace_url_pattern = re.compile(r"https?://[^/]+/#workspaces/([^/]+)/(.+)")
        lookup: dict[tuple[str, str], dict] = {}
        for ws in self.get_all_workspaces():
            match = workspace_url_pattern.match(ws.get("url", ""))
            if not match:
                continue
            key = (match.group(1), match.group(2))
            lookup[key] = ws
        return lookup
