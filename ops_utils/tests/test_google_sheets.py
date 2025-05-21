import unittest
from ops_utils.google_sheets_util import GoogleSheets
from unittest.mock import MagicMock, patch
from google.auth import credentials
import responses
import os

SPREADSHEET_ID = "1GjeRUYtkkT1bGxVGE4DLHrA5itQGWjvCh0xNWpdHTTQ"
SHEET_NAME = "Sheet1"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "test_creds.json"

MOCK_CREDENTIALS = MagicMock(spec=credentials.CredentialsWithQuotaProject)
MOCK_CREDENTIALS.with_quota_project.return_value = MOCK_CREDENTIALS
MOCK_CREDENTIALS.universe_domain = "googleapis.com"

LOAD_FILE_PATCH = patch(
    "google.auth._default.load_credentials_from_file",
    return_value=(MOCK_CREDENTIALS, 'operations-portal-427515'),
    autospec=True,
)

def create_google_sheets_client():
    with LOAD_FILE_PATCH:
        google_sheets_util = GoogleSheets()
    return google_sheets_util


class TestGoogleSheets(unittest.TestCase):
    """Test suite for GoogleSheets class."""
    def setUp(self):
        """Set up the test case."""
        self.google_sheets_client = create_google_sheets_client()

    @responses.activate
    def test_get_cell_value(self):
        """Test get_cell_value method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/get_cell_value.yaml")
        result = self.google_sheets_client.get_cell_value(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A1",
        )
        assert result == "Stuff"

    @responses.activate
    def test_get_last_row(self):
        """Test get_last_row method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/get_last_row.yaml")
        result = self.google_sheets_client.get_last_row(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
        )
        assert result == 3

    @responses.activate
    def test_update_and_get_cell_value(self):
        """Test update_cell method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/update_and_get_cell_value.yaml")
        self.google_sheets_client.update_cell(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A4",
            value="New Value",
        )
        result = self.google_sheets_client.get_cell_value(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A4",
        )
        assert result == "New Value"

    @responses.activate
    def test_get_column_values(self):
        """Test get_column_values method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/get_column_values.yaml")
        result = self.google_sheets_client.get_column_values(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            column="A",
        )
        assert result == ["Stuff", "To", "Test"]
