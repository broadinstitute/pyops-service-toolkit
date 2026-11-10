"""Utilities for working with the AnVIL Data Explorer (ADE), which is backed by the Azul service."""
import json
import logging
import time
from typing import Collection, Optional
from urllib.parse import urlencode, urlsplit

import requests

from .request_util import GET, PUT, RunRequest

AZUL_PROD_LINK = "https://service.explore.anvilproject.org"
"""@private"""
AZUL_DEV_LINK = "https://service.anvil.gi.ucsc.edu"
"""@private"""
TOKENINFO_URL = "https://www.googleapis.com/oauth2/v3/tokeninfo"
"""@private"""
PFB_MANIFEST_FORMAT = "verbatim.pfb"
"""@private"""


class AzulReindexingError(RuntimeError):
    """Raised when Azul is unhealthy or (re)indexing, so a manifest could be empty or incomplete."""


class AzulTokenError(RuntimeError):
    """Raised when an Azul access token would not be accepted by Azul."""


class SignedUrl:
    """
    Holder for a sensitive signed URL, such as the PFB manifest URL returned by Azul.

    For controlled-access data the signed URL grants direct read access to the data, so it must not leak into
    logs. `repr`, `str` and `format` only show the URL without its query string, and the object is not JSON
    serializable. The full URL is only available through `reveal()`, which should be called at the single place
    the URL is handed to its consumer (for example, the Terra import request).
    """

    __slots__ = ("_url",)

    def __init__(self, url: str):
        """
        Initialize the SignedUrl class.

        **Args:**
        - url (str): The full signed URL, including its secret query string.
        """
        self._url = url

    def reveal(self) -> str:
        """
        Get the full signed URL.

        **Returns:**
        - str: The signed URL including its secret query string. Do not log this.
        """
        return self._url

    @property
    def host(self) -> str:
        """The host of the URL. Safe to log."""
        return urlsplit(self._url).netloc

    @property
    def location(self) -> str:
        """The URL as `scheme://host/path`, without the secret query string. Safe to log."""
        parts = urlsplit(self._url)
        return f"{parts.scheme}://{parts.netloc}{parts.path}"

    def __repr__(self) -> str:
        """Show the URL without its query string."""
        return f"<SignedUrl {self.location} ?...redacted>"

    __str__ = __repr__

    def __format__(self, _spec: str) -> str:
        """Show the URL without its query string, whatever the format spec."""
        return self.__repr__()


class AzulClient:
    """Class to interact with the Azul service behind the AnVIL Data Explorer."""

    def __init__(self, request_util: RunRequest, azul_url: str = AZUL_PROD_LINK, catalog: Optional[str] = None):
        """
        Initialize the AzulClient class.

        Azul only accepts tokens issued to its own OAuth client, not regular gcloud tokens. Create the
        `RunRequest` with a `Token` built from a file holding an Azul token (see `validate_token`).

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): Utility for making HTTP requests.
        - azul_url (str, optional): Base URL of the Azul service. Defaults to `AZUL_PROD_LINK`.
        - catalog (str, optional): Azul catalog to use (e.g. `anvil15`). If not provided, the current default
          catalog is looked up the first time one is needed.
        """
        self.request_util = request_util
        """@private"""
        self.azul_url = azul_url.rstrip("/")
        """@private"""
        self.catalog = catalog
        """@private"""

    def get_default_catalog(self) -> str:
        """
        Get the current default catalog. Catalogs are versioned (e.g. `anvil14` -> `anvil15`).

        **Returns:**
        - str: The name of the default catalog.
        """
        response = self.request_util.run_request(uri=f"{self.azul_url}/index/catalogs", method=GET)
        return response.json()["default_catalog"]

    def check_not_reindexing(self) -> None:
        """
        Check that Azul is healthy and not in the middle of (re)indexing.

        While Azul is indexing, a manifest request still succeeds but the manifest can be empty or incomplete,
        so anything built from it would be wrong. Any outstanding indexing backlog fails the check.

        **Raises:**
        - AzulReindexingError: If Azul reports it is not up, or has unindexed bundles or documents.
        """
        response = self.request_util.run_request(uri=f"{self.azul_url}/health/progress", method=GET)
        health = response.json()
        progress = health.get("progress") or {}
        bundles = int(progress.get("unindexed_bundles") or 0)
        documents = int(progress.get("unindexed_documents") or 0)
        if not health.get("up"):
            raise AzulReindexingError(f"Azul reports it is not healthy (up=false) at {self.azul_url}.")
        if bundles or documents:
            raise AzulReindexingError(
                f"Azul is indexing (unindexed_bundles={bundles}, unindexed_documents={documents}). "
                "Wait for indexing to finish before requesting a manifest."
            )

    def request_pfb_manifest(self, dataset_id: str, max_wait_seconds: int = 900) -> SignedUrl:
        """
        Request a PFB manifest for a dataset and wait for the signed URL to download it.

        **Args:**
        - dataset_id (str): The Azul `dataset_id` (the `datasets.dataset_id` value, not the TDR snapshot id).
        - max_wait_seconds (int, optional): How long to wait for the manifest to be generated. Defaults to `900`.

        **Returns:**
        - `SignedUrl`: The signed URL of the generated PFB file.

        **Raises:**
        - TimeoutError: If the manifest is not ready within `max_wait_seconds`.
        - RuntimeError: If Azul returns an unexpected status.
        """
        if not self.catalog:
            self.catalog = self.get_default_catalog()
        params = {
            "catalog": self.catalog,
            "filters": json.dumps({"datasets.dataset_id": {"is": [dataset_id]}}),
            "format": PFB_MANIFEST_FORMAT,
        }
        # The query string goes in the URI because RunRequest only sends `params` for GET requests
        response = self.request_util.run_request(
            uri=f"{self.azul_url}/fetch/manifest/files?{urlencode(params)}", method=PUT
        )
        body = response.json()
        deadline = time.monotonic() + max_wait_seconds
        # Azul reports progress in the body, not the HTTP status: 301 = still generating, 302 = ready
        while body.get("Status") == 301:
            retry_after = float(body.get("Retry-After", 1))
            if time.monotonic() + retry_after > deadline:
                raise TimeoutError(f"Azul manifest for dataset {dataset_id} not ready after {max_wait_seconds}s")
            time.sleep(retry_after)
            body = self.request_util.run_request(uri=body["Location"], method=GET).json()
        if body.get("Status") != 302:
            raise RuntimeError(f"Unexpected status from Azul manifest request: {body.get('Status')}")
        logging.info(f"Azul manifest for dataset {dataset_id} is ready")
        return SignedUrl(body["Location"])

    def validate_token(
            self,
            token: str,
            allowed_emails: Optional[Collection[str]] = None,
            min_remaining_seconds: int = 0
    ) -> dict:
        """
        Check that an Azul access token would be accepted by Azul.

        The token must have been issued to Azul's own OAuth client (its `azp`), which is read from Azul's
        Swagger page. A token copied from Azul's Swagger UI satisfies this; a regular gcloud token does not.

        **Args:**
        - token (str): The Azul access token to check.
        - allowed_emails (Collection[str], optional): If provided, the token's account must be one of these
          (case-insensitive). If not provided, the account is not checked.
        - min_remaining_seconds (int, optional): The token must have more than this many seconds left.
          Defaults to `0`.

        **Returns:**
        - dict: Google's `tokeninfo` for the token (contains `email`, `azp`, `expires_in`, ...).

        **Raises:**
        - AzulTokenError: If the token is expired, issued to the wrong client, belongs to an account that is not
          allowed, or does not have enough time left.
        """
        swagger = self.request_util.run_request(
            uri=f"{self.azul_url}/swagger/swagger-initializer.js", method=GET, accept="*/*"
        )
        # Azul injects: const client_id = "<id>.apps.googleusercontent.com";
        client_ids = [word.strip('";') for word in swagger.text.split() if ".apps.googleusercontent.com" in word]
        if not client_ids:
            raise AzulTokenError(f"Could not find Azul's OAuth client id at {self.azul_url}/swagger")

        tokeninfo = requests.get(TOKENINFO_URL, params={"access_token": token}, timeout=30)
        if tokeninfo.status_code == 400:
            raise AzulTokenError("Token is not valid (Google tokeninfo returned 400: expired or malformed)")
        tokeninfo.raise_for_status()
        info = tokeninfo.json()

        if info.get("azp") != client_ids[0]:
            raise AzulTokenError(
                f"Token's azp '{info.get('azp')}' is not Azul's OAuth client '{client_ids[0]}'. "
                "Copy the token from the Azul Swagger UI."
            )
        if allowed_emails is not None and info.get("email", "").lower() not in {e.lower() for e in allowed_emails}:
            raise AzulTokenError(f"Token belongs to '{info.get('email')}', which is not an allowed account")
        remaining = int(info.get("expires_in", 0))
        if remaining <= min_remaining_seconds:
            raise AzulTokenError(f"Token expires in {remaining}s, but more than {min_remaining_seconds}s is needed")
        return info
