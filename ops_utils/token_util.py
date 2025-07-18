"""Module for generating tokens for other module services."""
import httplib2
import pytz
import logging
import requests
import os
from typing import Optional, Union
from datetime import datetime, timedelta


class Token:
    """Class for generating tokens for other module services."""

    def __init__(
            self,
            token_file: Optional[str] = None,
            extra_scopes: Optional[list[str]] = None,
            service_account_json: Optional[str] = None
    ) -> None:
        """Initialize the Token class.

        **Args:**
        - token_file (str, optional): The path to a file containing an existing token string.
        - extra_scopes (list[str], optional): Additional scopes to request for the token.
        - service_account_json (str, optional): Path to service account JSON key file.
        """
        self.expiry: Optional[datetime] = None
        """@private"""
        self.token_string: Optional[str] = ""
        """@private"""

        # If provided with a file just use the contents of file
        if token_file:
            self.token_file = token_file
            with open(self.token_file) as f:
                self.token_string = f.read().rstrip()
            return

        self.token_file = ""

        # Default scopes
        scopes = [
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/devstorage.full_control"
        ]

        # Add extra scopes if provided
        if extra_scopes:
            scopes.extend(extra_scopes)

        # Use service account if provided
        if service_account_json:
            from oauth2client.service_account import ServiceAccountCredentials
            self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                service_account_json,
                scopes=scopes
            )
        else:
            # Fall back to application default credentials
            from oauth2client.client import GoogleCredentials
            self.credentials = GoogleCredentials.get_application_default()
            self.credentials = self.credentials.create_scoped(scopes)

    def _get_gcp_token(self) -> Union[str, None]:
        # Refresh token if it has not been set or if it is expired or close to expiry
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            http = httplib2.Http()
            self.credentials.refresh(http)
            self.token_string = self.credentials.get_access_token().access_token
            # Set expiry to use UTC since google uses that timezone
            self.expiry = self.credentials.token_expiry.replace(tzinfo=pytz.UTC)  # type: ignore[union-attr]
            # Convert expiry time to EST for logging
            est_expiry = self.expiry.astimezone(pytz.timezone("US/Eastern"))  # type: ignore[union-attr]
            logging.info(f"New token expires at {est_expiry} EST")
        return self.token_string

    def _get_sa_token(self) -> Union[str, None]:
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            SCOPES = ['https://www.googleapis.com/auth/userinfo.profile',
                      'https://www.googleapis.com/auth/userinfo.email']
            url = f"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token?scopes={','.join(SCOPES)}"  # noqa: E501
            token_response = requests.get(url, headers={'Metadata-Flavor': 'Google'})
            self.token_string = token_response.json()['access_token']
        return token_response.json()['access_token']

    def get_token(self) -> str:
        """
        Generate a token with a set expiration time.

        **Returns:**
        - string: The generated token
        """
        # If token file provided then always return contents
        if self.token_file:
            return self.token_string  # type: ignore[return-value]
        else:
            # detect if this is running as a cloud run job
            if os.getenv("CLOUD_RUN_JOB"):
                return self._get_sa_token()  # type: ignore[return-value]
            else:
                return self._get_gcp_token()  # type: ignore[return-value]
