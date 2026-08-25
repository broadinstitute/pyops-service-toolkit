"""Utilities for working with the DataIngest API."""
import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
import requests
from google.auth import default as google_auth_default
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as GoogleUserCredentials
from google.oauth2.id_token import fetch_id_token

from .vars import ARG_DEFAULTS, APPLICATION_JSON
from .request_util import GET, POST, PATCH, PUT, DELETE, RunRequest

# Constant for the locally-running DataIngest API
DATA_INGEST_LOCAL_LINK = "http://localhost:8080/api/v1"
"""@private"""

QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"

# Audience gcloud itself uses for `gcloud auth print-identity-token`. DataIngest's backend
# only checks that an ID token is issued/signed by Google, not its audience, so this
# constant is only exercised for non-interactive credentials (e.g. a service account) that can't
# produce an id_token tied to the audience of an interactive OAuth consent.
_IDENTITY_TOKEN_AUDIENCE = "32555940559.apps.googleusercontent.com"


class _IdentityToken:
    """
    Fetches and auto-refreshes a Google-issued OIDC identity token (JWT).

    DataIngest's backend authenticates callers as Google OIDC identity tokens rather than
    OAuth access tokens, so this exists instead of `ops_utils.token_util.Token`, which only
    produces the latter. Duck-types the subset of `Token`'s interface (`get_token`,
    `token_string`) that `RunRequest` relies on.
    """

    def __init__(self) -> None:
        self.token_string: str = ""
        """@private"""
        self._expiry: Optional[datetime] = None

    def get_token(self) -> str:
        """
        Return a cached identity token, refreshing it if missing or close to expiry.

        **Returns:**
        - string: The generated identity token
        """
        if not self.token_string or not self._expiry or self._expiry < datetime.now(timezone.utc) + timedelta(minutes=5):  # noqa: E501
            credentials, _ = google_auth_default()
            http_request = GoogleAuthRequest()
            if isinstance(credentials, GoogleUserCredentials):
                # Interactive user ADC (e.g. `gcloud auth application-default login`): Google's
                # token endpoint returns an id_token alongside the access_token for these.
                credentials.refresh(http_request)
                self.token_string = credentials.id_token
            else:
                # Service account / GCE metadata server credentials.
                self.token_string = fetch_id_token(http_request, _IDENTITY_TOKEN_AUDIENCE)
            self._expiry = self._decode_expiry(self.token_string)
            logging.info(f"New DataIngest identity token expires at {self._expiry.isoformat()}")
        return self.token_string

    @staticmethod
    def _decode_expiry(token: str) -> datetime:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload))["exp"]
        return datetime.fromtimestamp(exp, tz=timezone.utc)


class DataIngest:
    """Class for interacting with the DataIngest API (a lightweight, cloud-native data repository)."""

    ROLE_OPTIONS = ["STEWARD", "CUSTODIAN", "READER", "DISCOVERER"]
    """@private"""
    GROUP_ROLE_OPTIONS = ["OWNER", "MEMBER"]
    """@private"""

    def __init__(self, request_util: Optional[RunRequest] = None, base_url: str = DATA_INGEST_LOCAL_LINK):
        """
        Initialize the DataIngest class.

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`, optional): An instance of a
            request utility class to handle HTTP requests. If not provided, one is created
            with a self-refreshing Google identity token, which is what DataIngest's backend
            requires (as opposed to the OAuth access token `ops_utils.token_util.Token`
            produces).
        - base_url (str, optional): The base URL for the DataIngest API.
            Defaults to the locally-running instance at `http://localhost:8080/api/v1`.
        """
        self.request_util = request_util if request_util is not None else RunRequest(token=_IdentityToken())  # type: ignore[arg-type]  # noqa: E501
        """@private"""
        self.base_url = base_url
        """@private"""

    @staticmethod
    def _check_role(role: str) -> None:
        if role not in DataIngest.ROLE_OPTIONS:
            raise ValueError(f"Role must be one of {DataIngest.ROLE_OPTIONS}")

    @staticmethod
    def _check_group_role(role: str) -> None:
        if role not in DataIngest.GROUP_ROLE_OPTIONS:
            raise ValueError(f"Role must be one of {DataIngest.GROUP_ROLE_OPTIONS}")

    @staticmethod
    def _build_payload(**kwargs: Any) -> dict:
        """Build a JSON payload, dropping any keys whose value is None."""
        return {key: value for key, value in kwargs.items() if value is not None}

    # ---------------------------------------------------------------- Datasets

    def list_datasets(self) -> requests.Response:
        """
        List all datasets.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/datasets", method=GET)

    def create_dataset(
            self,
            name: str,
            description: Optional[str] = None,
            tags: Optional[list[str]] = None,
            ddl: Optional[str] = None,
            storage: Optional[dict] = None,
    ) -> requests.Response:
        """
        Create a new dataset.

        This is an asynchronous operation - use `wait_for_job` with the returned `jobId` to wait
        for completion.

        **Args:**
        - name (str): Name of the dataset.
        - description (str, optional): Description of the dataset.
        - tags (list[str], optional): User-defined tags for the dataset.
        - ddl (str, optional): DuckDB DDL to define an initial schema.
        - storage (dict, optional): Storage configuration, e.g.
            `{"cloud": "AWS", "region": "us-west-2", "bucket": "my-bucket", "prefix": "data/"}`.

        **Returns:**
        - requests.Response: The response from the request, containing a `jobId`.
        """
        payload = self._build_payload(name=name, description=description, tags=tags, ddl=ddl, storage=storage)
        logging.info(f"Creating dataset {name}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    def get_dataset(self, dataset_id: str) -> requests.Response:
        """
        Get a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/datasets/{dataset_id}", method=GET)

    def update_dataset(
            self,
            dataset_id: str,
            name: str,
            description: Optional[str] = None,
            tags: Optional[list[str]] = None,
            ddl: Optional[str] = None,
            storage: Optional[dict] = None,
    ) -> requests.Response:
        """
        Update a dataset's metadata.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - name (str): Name of the dataset.
        - description (str, optional): Description of the dataset.
        - tags (list[str], optional): User-defined tags for the dataset.
        - ddl (str, optional): DuckDB DDL to define an initial schema.
        - storage (dict, optional): Storage configuration for the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        payload = self._build_payload(name=name, description=description, tags=tags, ddl=ddl, storage=storage)
        logging.info(f"Updating dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}",
            method=PATCH,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    def delete_dataset(self, dataset_id: str) -> requests.Response:
        """
        Delete a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Deleting dataset {dataset_id}")
        return self.request_util.run_request(uri=f"{self.base_url}/datasets/{dataset_id}", method=DELETE)

    def list_dataset_permissions(self, dataset_id: str) -> requests.Response:
        """
        List permissions on a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/datasets/{dataset_id}/permissions", method=GET)

    def grant_dataset_permissions(self, dataset_id: str, principals: list[str], role: str) -> requests.Response:
        """
        Grant permissions on a dataset to one or more principals.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - principals (list[str]): Principal emails (user or group) to grant the role to.
        - role (str): Role to grant. Must be one of `STEWARD`, `CUSTODIAN`, `READER`, or `DISCOVERER`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        self._check_role(role)
        logging.info(f"Granting {role} on dataset {dataset_id} to {principals}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/permissions",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"principals": principals, "role": role})
        )

    def revoke_dataset_permission(self, dataset_id: str, permission_id: str) -> requests.Response:
        """
        Revoke a permission grant on a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - permission_id (str): The ID of the permission to revoke.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Revoking permission {permission_id} on dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/permissions/{permission_id}",
            method=DELETE
        )

    # -------------------------------------------------------------------- Files

    def register_files(self, dataset_id: str, files: list[dict]) -> requests.Response:
        """
        Register one or more files from object storage into a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - files (list[dict]): Files to register. Each entry looks like
            `{"uri": "s3://my-bucket/data/samples/HG00096.vcf.gz", "path": "data/samples/HG00096.vcf.gz"}`.
            `path` is optional and defaults to the URI's object key when omitted.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Registering {len(files)} file(s) to dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/files",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"files": files})
        )

    def list_files(self, dataset_id: str) -> requests.Response:
        """
        List all files registered in a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/datasets/{dataset_id}/files", method=GET)

    def get_file(self, dataset_id: str, file_id: str) -> requests.Response:
        """
        Get a single registered file, including a signed URL for reading it.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - file_id (str): The ID of the file.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/files/{file_id}",
            method=GET
        )

    def ingest_tabular_data(
            self, dataset_id: str, sources: list[dict], timeout: Optional[str] = None
    ) -> requests.Response:
        """
        Ingest one or more tabular data sources into dataset tables.

        This is an asynchronous operation - use `wait_for_job` with the returned `jobId` to wait
        for completion.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - sources (list[dict]): Ingest sources. Each entry looks like
            `{"uri": "s3://my-bucket/data/samples.parquet", "targetTable": "samples", "format": "PARQUET"}`.
            `format` defaults to `PARQUET` if omitted, and can also be `CSV` or `JSON`. An optional `schema` key
            (`{"id": "<schema-version-id>", "validateForeignKeys": true}`) can be included to validate against a
            schema version.
        - timeout (str, optional): Maximum runtime for this ingest, e.g. `"8h"`, `"90m"`.

        **Returns:**
        - requests.Response: The response from the request, containing a `jobId`.
        """
        payload = self._build_payload(sources=sources, timeout=timeout)
        logging.info(f"Ingesting {len(sources)} source(s) into dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/ingest",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    # ------------------------------------------------------------------ Schemas

    def list_schema_versions(self, dataset_id: str) -> requests.Response:
        """
        List all schema versions for a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/datasets/{dataset_id}/schemas", method=GET)

    def create_schema_version(self, dataset_id: str, ddl: str) -> requests.Response:
        """
        Create a new schema version for a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - ddl (str): DuckDB DDL that defines the schema version.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Creating new schema version for dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/schemas",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"ddl": ddl})
        )

    def get_schema_version(self, dataset_id: str, schema_id: str) -> requests.Response:
        """
        Get a specific schema version for a dataset.

        **Args:**
        - dataset_id (str): The ID of the dataset.
        - schema_id (str): The ID of the schema version.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/datasets/{dataset_id}/schemas/{schema_id}",
            method=GET
        )

    # ------------------------------------------------------------------- Groups

    def list_groups(self) -> requests.Response:
        """
        List all principal groups.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/groups", method=GET)

    def create_group(self, name: str) -> requests.Response:
        """
        Create a new principal group.

        **Args:**
        - name (str): The name of the group.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Creating group {name}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/groups",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"name": name})
        )

    def get_group(self, group_id: str) -> requests.Response:
        """
        Get a principal group.

        **Args:**
        - group_id (str): The ID of the group.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/groups/{group_id}", method=GET)

    def delete_group(self, group_id: str) -> requests.Response:
        """
        Delete a principal group.

        **Args:**
        - group_id (str): The ID of the group.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Deleting group {group_id}")
        return self.request_util.run_request(uri=f"{self.base_url}/groups/{group_id}", method=DELETE)

    def list_group_members(self, group_id: str) -> requests.Response:
        """
        List the members of a principal group.

        **Args:**
        - group_id (str): The ID of the group.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/groups/{group_id}/members", method=GET)

    def add_group_members(self, group_id: str, principals: list[str]) -> requests.Response:
        """
        Add members to a principal group.

        **Args:**
        - group_id (str): The ID of the group.
        - principals (list[str]): Principal emails (user or group) to add.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Adding {principals} to group {group_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/groups/{group_id}/members",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"principals": principals})
        )

    def update_group_member_role(self, group_id: str, email: str, role: str) -> requests.Response:
        """
        Update a group member's role.

        **Args:**
        - group_id (str): The ID of the group.
        - email (str): The email of the group member.
        - role (str): The role to assign. Must be one of `OWNER` or `MEMBER`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        self._check_group_role(role)
        logging.info(f"Updating {email} to {role} in group {group_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/groups/{group_id}/members/{email}",
            method=PUT,
            content_type=APPLICATION_JSON,
            data=json.dumps({"role": role})
        )

    def remove_group_member(self, group_id: str, email: str) -> requests.Response:
        """
        Remove a member from a principal group.

        **Args:**
        - group_id (str): The ID of the group.
        - email (str): The email of the group member to remove.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Removing {email} from group {group_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/groups/{group_id}/members/{email}",
            method=DELETE
        )

    # -------------------------------------------------------------------- Jobs

    def list_jobs(self) -> requests.Response:
        """
        List all asynchronous jobs.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/jobs", method=GET)

    def get_job(self, job_id: str) -> requests.Response:
        """
        Get the status and details of an asynchronous job.

        **Args:**
        - job_id (str): The ID of the job.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/jobs/{job_id}", method=GET)

    def wait_for_job(self, job_id: str, poll_interval: int = ARG_DEFAULTS["waiting_time_to_poll"]) -> Any:
        """
        Poll a job until it reaches a terminal state (`SUCCEEDED` or `FAILED`).

        **Args:**
        - job_id (str): The ID of the job to wait for.
        - poll_interval (int, optional): The interval in seconds to wait between status checks.
            Defaults to `90`.

        **Returns:**
        - Any: The job's `result` once it has succeeded.
        """
        while True:
            job = self.get_job(job_id).json()
            status = job["status"]
            if status == SUCCEEDED:
                logging.info(f"DataIngest job {job_id} succeeded")
                return job.get("result")
            if status == FAILED:
                raise Exception(f"DataIngest job {job_id} failed: {job.get('result')}")
            logging.info(f"DataIngest job {job_id} is {status.lower()}")
            time.sleep(poll_interval)

    # --------------------------------------------------------------- Snapshots

    def list_snapshots(self) -> requests.Response:
        """
        List all snapshots.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/snapshots", method=GET)

    def create_snapshot(
            self,
            dataset_id: str,
            name: str,
            schema_id: str,
            description: Optional[str] = None,
            queries: Optional[dict] = None,
            timeout: Optional[str] = None,
    ) -> requests.Response:
        """
        Create a snapshot from a dataset.

        This is an asynchronous operation - use `wait_for_job` with the returned `jobId` to wait
        for completion.

        **Args:**
        - dataset_id (str): The ID of the source dataset.
        - name (str): Name of the snapshot.
        - schema_id (str): Schema version ID that declares the output contract for this snapshot.
        - description (str, optional): Description of the snapshot.
        - queries (dict, optional): Map of table name to SQL. Tables absent from this map are
            included in full. SQL executes against virtual DuckDB views of the dataset tables,
            e.g. `{"samples": "SELECT * FROM samples WHERE disease = 'lung_cancer'"}`.
        - timeout (str, optional): Maximum runtime for this snapshot creation, e.g. `"4h"`, `"90m"`.

        **Returns:**
        - requests.Response: The response from the request, containing a `jobId`.
        """
        payload = self._build_payload(
            datasetId=dataset_id, name=name, schemaId=schema_id, description=description,
            queries=queries, timeout=timeout
        )
        logging.info(f"Creating snapshot {name} from dataset {dataset_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    def get_snapshot(self, snapshot_id: str) -> requests.Response:
        """
        Get a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/snapshots/{snapshot_id}", method=GET)

    def delete_snapshot(self, snapshot_id: str) -> requests.Response:
        """
        Delete a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Deleting snapshot {snapshot_id}")
        return self.request_util.run_request(uri=f"{self.base_url}/snapshots/{snapshot_id}", method=DELETE)

    def get_snapshot_export_signed_url(self, snapshot_id: str, export_format: str = "PFB") -> requests.Response:
        """
        Get a signed URL for a completed snapshot export artifact.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - export_format (str, optional): The export format. Defaults to `PFB` (currently the only
            supported snapshot export format).

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/exports",
            method=GET,
            params={"format": export_format}
        )

    def create_snapshot_export(
            self, snapshot_id: str, export_format: str = "PFB", timeout: Optional[str] = None
    ) -> requests.Response:
        """
        Create an export from a snapshot.

        This is an asynchronous operation - use `wait_for_job` with the returned `jobId` to wait
        for completion.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - export_format (str, optional): The export format. Defaults to `PFB` (currently the only
            supported snapshot export format).
        - timeout (str, optional): Maximum runtime for this export, e.g. `"4h"`.

        **Returns:**
        - requests.Response: The response from the request, containing a `jobId`.
        """
        payload = self._build_payload(format=export_format, timeout=timeout)
        logging.info(f"Creating {export_format} export for snapshot {snapshot_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/exports",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    def get_snapshot_file_signed_url(self, snapshot_id: str, file_id: str) -> requests.Response:
        """
        Get a signed URL for a file within a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - file_id (str): The ID of the file.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/files/{file_id}",
            method=GET
        )

    def get_snapshot_manifest(self, snapshot_id: str) -> requests.Response:
        """
        Get the manifest of table partitions and file references for a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/snapshots/{snapshot_id}/manifest", method=GET)

    def list_snapshot_permissions(self, snapshot_id: str) -> requests.Response:
        """
        List permissions on a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(uri=f"{self.base_url}/snapshots/{snapshot_id}/permissions", method=GET)

    def grant_snapshot_permissions(self, snapshot_id: str, principals: list[str], role: str) -> requests.Response:
        """
        Grant permissions on a snapshot to one or more principals.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - principals (list[str]): Principal emails (user or group) to grant the role to.
        - role (str): Role to grant. Must be one of `STEWARD`, `CUSTODIAN`, `READER`, or `DISCOVERER`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        self._check_role(role)
        logging.info(f"Granting {role} on snapshot {snapshot_id} to {principals}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/permissions",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps({"principals": principals, "role": role})
        )

    def revoke_snapshot_permission(self, snapshot_id: str, permission_id: str) -> requests.Response:
        """
        Revoke a permission grant on a snapshot.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - permission_id (str): The ID of the permission to revoke.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Revoking permission {permission_id} on snapshot {snapshot_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/permissions/{permission_id}",
            method=DELETE
        )

    def get_snapshot_table_signed_urls(self, snapshot_id: str, table_name: str) -> requests.Response:
        """
        Get signed URLs for the Parquet partitions of a snapshot table.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - table_name (str): The name of the table.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/tables/{table_name}",
            method=GET
        )

    def get_table_export_signed_url(
            self, snapshot_id: str, table_name: str, export_format: str = "CSV"
    ) -> requests.Response:
        """
        Get a signed URL for a completed table export artifact.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - table_name (str): The name of the table.
        - export_format (str, optional): The export format. Defaults to `CSV` (currently the only
            supported table export format).

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/tables/{table_name}/exports",
            method=GET,
            params={"format": export_format}
        )

    def create_table_export(
            self, snapshot_id: str, table_name: str, export_format: str = "CSV", timeout: Optional[str] = None
    ) -> requests.Response:
        """
        Create an export from a snapshot table.

        This is an asynchronous operation - use `wait_for_job` with the returned `jobId` to wait
        for completion.

        **Args:**
        - snapshot_id (str): The ID of the snapshot.
        - table_name (str): The name of the table.
        - export_format (str, optional): The export format. Defaults to `CSV` (currently the only
            supported table export format).
        - timeout (str, optional): Maximum runtime for this export, e.g. `"4h"`.

        **Returns:**
        - requests.Response: The response from the request, containing a `jobId`.
        """
        payload = self._build_payload(format=export_format, timeout=timeout)
        logging.info(f"Creating {export_format} export for table {table_name} in snapshot {snapshot_id}")
        return self.request_util.run_request(
            uri=f"{self.base_url}/snapshots/{snapshot_id}/tables/{table_name}/exports",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )
