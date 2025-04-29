import unittest
import os
from unittest.mock import patch, MagicMock

from ops_utils.token_util import Token


class TestToken(unittest.TestCase):
    """Test the Token class with a mocked GCP token"""

    @patch("oauth2client.client.GoogleCredentials.get_application_default")
    def setUp(self, mock_get_application_default):
        self.mock_get_application_default = mock_get_application_default

        # Mock the original GoogleCredentials instance
        self.mock_google_credentials_instance = MagicMock()
        self.mock_get_application_default.return_value = self.mock_google_credentials_instance

        # Mock the object returned by create_scoped
        self.mock_scoped_credentials = MagicMock()
        self.mock_google_credentials_instance.create_scoped.return_value = self.mock_scoped_credentials

        # Set refresh, token, expiry on the *scoped credentials*
        self.mock_scoped_credentials.refresh = MagicMock()

        # Instantiate the Token class
        self.gcp_token = Token(token_file=None)

    def test_init_gcp(self):
        """Test the init method using GCP token"""
        self.mock_get_application_default.assert_called_once()

        self.mock_google_credentials_instance.create_scoped.assert_called_once_with(
            ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/devstorage.full_control"]
        )

    def test_get_gcp_token(self):
        # Mock the result of get_access_token()
        mock_access_token_response = MagicMock()
        mock_access_token_response.access_token = "fake-token"
        self.mock_scoped_credentials.get_access_token.return_value = mock_access_token_response

        # Call the method
        token = self.gcp_token._get_gcp_token()

        # Assertions
        self.mock_scoped_credentials.refresh.assert_called_once()
        self.mock_scoped_credentials.get_access_token.assert_called_once()
        self.mock_scoped_credentials.token_expiry.replace.assert_called_once()

        self.assertEqual(token, "fake-token")

    @patch("ops_utils.token_util.Token._get_gcp_token")
    def test_get_token_gcp(self, mock_get_gcp_token):
        mock_get_gcp_token.return_value = "fake-token"

        # Call the method
        token = self.gcp_token.get_token()

        # Assertions
        self.assertEqual(token, "fake-token")
        mock_get_gcp_token.assert_called_once()

    @patch("ops_utils.token_util.requests.get")
    def test_get_sa_token(self, mock_requests_get):
        fake_response = MagicMock()
        fake_token = "fake-sa-token"
        fake_response.json.return_value = {"access_token": fake_token}
        mock_requests_get.return_value = fake_response

        # Call the method
        res = self.gcp_token._get_sa_token()

        # Assertions
        SCOPES = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
        url = f"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token?scopes={','.join(SCOPES)}"  # noqa: E501

        self.assertEqual(res, fake_token)
        mock_requests_get.assert_called_once_with(url, headers={'Metadata-Flavor': 'Google'})
        self.assertEqual(self.gcp_token.token_string, fake_token)

    @patch.dict(os.environ, {"CLOUD_RUN_JOB": 'true'})
    @patch("ops_utils.token_util.Token._get_sa_token")
    def test_get_token_sa_token(self, sa_token_patch):
        sa_token_patch.return_value = "fake-sa-token"

        # Call the method
        self.gcp_token.get_token()

        # Assert that the SA token method was called if CLOUD_RUN_JOB env is set
        sa_token_patch.assert_called_once()