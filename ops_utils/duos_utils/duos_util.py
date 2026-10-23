"""Utilities for working with DUOS (Data Use Oversight System)."""
import copy
import json
import logging
import re
from typing import Any

import requests

from . import duos_registration_enums
from ..request_util import GET, POST, PUT, RunRequest

DUOS_PROD_LINK = "https://consent.dsde-prod.broadinstitute.org"
"""@private"""
DUOS_DEV_LINK = "https://consent.dsde-dev.broadinstitute.org"
"""@private"""

# Registration fields holding a single enum value, and the table that translates each to its display value
_REGISTRATION_ENUM_FIELDS = {
    "studyType": duos_registration_enums._STUDY_TYPES,
    "nihAnvilUse": duos_registration_enums._NIH_ANVIL_USE,
    "alternativeDataSharingPlanDataSubmitted": duos_registration_enums._ADSP_DATA_SUBMITTED,
    "alternativeDataSharingPlanAccessManagement": duos_registration_enums._ADSP_ACCESS_MANAGEMENT,
}
_CONSENT_GROUP_ENUM_FIELDS = {
    "accessManagement": duos_registration_enums._ACCESS_MANAGEMENT,
    "dataLocation": duos_registration_enums._DATA_LOCATIONS,
}


_REGISTRATION_DATE_FIELDS = [
    "embargoReleaseDate",
    "alternativeDataSharingPlanTargetDeliveryDate",
    "alternativeDataSharingPlanTargetPublicReleaseDate",
]
_ISO_DATETIME_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})T")


def _to_date(value: Any) -> Any:
    """Cut a timestamp like `2021-09-30T00:00:00Z` down to the `2021-09-30` date DUOS requires when updating."""
    if isinstance(value, str):
        match = _ISO_DATETIME_PATTERN.match(value)
        if match:
            return match.group(1)
    return value


def _registration_for_update(registration: dict) -> dict:
    """
    Convert a registration read from DUOS into the form DUOS accepts when updating.

    Reading returns enum names (e.g. `OBSERVATIONAL`) and dates as timestamps, while updating requires enum display
    values (e.g. `Observational`) and plain `YYYY-MM-DD` dates. Values that are not in the translation tables
    (including ones that are already display values) are left as is.
    """
    updated = copy.deepcopy(registration)
    for field in _REGISTRATION_DATE_FIELDS:
        if field in updated:
            updated[field] = _to_date(updated[field])
    for field, table in _REGISTRATION_ENUM_FIELDS.items():
        if isinstance(updated.get(field), str):
            updated[field] = table.get(updated[field], updated[field])
    if updated.get("alternativeDataSharingPlanReasons"):
        updated["alternativeDataSharingPlanReasons"] = [
            duos_registration_enums._ADSP_REASONS.get(reason, reason) for reason in updated["alternativeDataSharingPlanReasons"]
        ]
    for consent_group in updated.get("consentGroups") or []:
        if "morDate" in consent_group:
            consent_group["morDate"] = _to_date(consent_group["morDate"])
        for field, table in _CONSENT_GROUP_ENUM_FIELDS.items():
            if isinstance(consent_group.get(field), str):
                consent_group[field] = table.get(consent_group[field], consent_group[field])
        for file_type in consent_group.get("fileTypes") or []:
            if isinstance(file_type.get("fileType"), str):
                file_type["fileType"] = duos_registration_enums._FILE_TYPES.get(file_type["fileType"], file_type["fileType"])
    return updated


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

    def get_study_publications(self, study_id: int) -> list[dict]:
        """
        Get the primary publications already registered for a DUOS study.

        **Args:**
        - study_id (int): The DUOS study ID (the `studyId` returned by `find_studies_by_phs_id`).

        **Returns:**
        - list[dict]: The publications registered for the study. Each dictionary can include
            `pubmedId`, `title`, `doi`, `journal`, `publishedDate`, `authors`, and `tags`.
            Empty if the study has no publications.
        """
        logging.info(f"Fetching publications registered in DUOS for study {study_id}")
        return self.request_util.run_request(
            uri=f"{self.duos_link}/api/dataset/study/{study_id}/assets/publications",
            method=GET,
        ).json()

    def get_study_registration(self, study_id: int) -> dict:
        """
        Get a study in dataset registration format.

        **Args:**
        - study_id (int): The DUOS study ID.

        **Returns:**
        - dict: The study registration, which can be modified and sent back with `update_study_registration`.
        """
        return self.request_util.run_request(
            uri=f"{self.duos_link}/api/dataset/study/registration/{study_id}",
            method=GET,
        ).json()

    def update_study_registration(self, study_id: int, registration: dict) -> requests.Response:
        """
        Update a study and its datasets from a registration payload.

        Only admins, the study creator, and the study's data custodians can do this; anyone else gets a 403.

        **Args:**
        - study_id (int): The DUOS study ID.
        - registration (dict): The full registration payload, in the format returned by `get_study_registration`.
            Enum names and timestamps are converted to the forms DUOS requires for an update.

        **Returns:**
        - requests.Response: The response from the request, containing the updated study.
        """
        logging.info(f"Updating DUOS study {study_id}")
        return self.request_util.run_request(
            uri=f"{self.duos_link}/api/dataset/study/{study_id}",
            method=PUT,
            files={"dataset": (None, json.dumps(_registration_for_update(registration)), "application/json")},
        )

    def add_study_publications(self, study_id: int, publications: list[dict]) -> list[dict]:
        """
        Add publications to a DUOS study, skipping any whose PubMed ID is already registered on it.

        DUOS has no endpoint for writing publications on their own, so this reads the study registration, appends
        the new publications, and writes the registration back. Only admins, the study creator, and the study's data
        custodians can do this.

        **Args:**
        - study_id (int): The DUOS study ID.
        - publications (list[dict]): Publications to add. Each can include `pubmedId`, `title`, `journal`, `url`,
            `publishedDate`, `doi`, `authors` (list of `{"name": ...}`), and `tags`. Keys with a value of `None` are
            dropped.

        **Returns:**
        - list[dict]: The publications that were actually added (empty if all were already present).
        """
        registration = self.get_study_registration(study_id)
        existing_publications = registration.get("publications") or []
        existing_pmids = {str(pub["pubmedId"]).strip() for pub in existing_publications if pub.get("pubmedId")}

        new_publications = []
        for publication in publications:
            pmid = str(publication.get("pubmedId", "")).strip()
            if pmid and pmid in existing_pmids:
                continue
            new_publications.append({key: value for key, value in publication.items() if value is not None})
            existing_pmids.add(pmid)

        if not new_publications:
            logging.info(f"All publications are already registered on DUOS study {study_id}")
            return []

        registration["publications"] = existing_publications + new_publications
        self.update_study_registration(study_id, registration)
        logging.info(f"Added {len(new_publications)} publication(s) to DUOS study {study_id}")
        return new_publications

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
