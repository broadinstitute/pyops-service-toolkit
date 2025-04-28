import unittest
from unittest.mock import patch, MagicMock

from ops_utils.token_util import Token
from ops_utils.vars import GCP, AZURE


class TestTokenGCP(unittest.TestCase):
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
        self.gcp_token = Token(cloud=GCP, token_file=None)

    def test_init_gcp(self):
        """Test the init method using GCP token"""
        self.assertEqual(self.gcp_token.cloud, GCP)
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


class TestTokenAzure(unittest.TestCase):
    """Test the Token class with a mocked Azure token"""

    @patch("azure.identity.DefaultAzureCredential")
    def setUp(self, mock_default_azure_credential):
        self.mock_default_azure_credential = mock_default_azure_credential

        # Mock the credential instance
        self.mock_credential_instance = MagicMock()
        self.mock_default_azure_credential.return_value = self.mock_credential_instance

        # Instantiate the token class for an Azure token
        self.azure_token = Token(cloud=AZURE, token_file=None)

    def test_init_azure(self):
        """Test init with Azure cloud"""
        self.assertEqual(self.azure_token.cloud, AZURE)
        self.mock_default_azure_credential.assert_called_once_with()

    def test_get_az_token(self):
        """Test _get_az_token"""
        # Mock get_token return value
        mock_access_token_response = MagicMock()
        mock_access_token_response.token = "fake-token"
        self.mock_credential_instance.get_token.return_value = mock_access_token_response

        # Reset call history after initialization
        self.mock_credential_instance.get_token.reset_mock()

        # Call the method
        token = self.azure_token._get_az_token()

        # Assertions
        self.mock_credential_instance.get_token.assert_called_once_with("https://management.azure.com/.default")
        self.assertEqual(token, "fake-token")
        self.assertEqual(self.azure_token.token_string, "fake-token")

    @patch("ops_utils.token_util.Token._get_az_token")
    def test_get_token(self, mock_get_az_token):
        mock_get_az_token.return_value = "fake-token"

        # Call the method
        token = self.azure_token.get_token()

        # Assertions
        self.assertEqual(token, "fake-token")
        mock_get_az_token.assert_called_once()
