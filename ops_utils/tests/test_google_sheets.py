import unittest
import gspread
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


class TestGoogleSheetsWithMockedClient(unittest.TestCase):
    """Test suite for GoogleSheets methods using a fully mocked gspread client."""

    def setUp(self):
        self.mock_worksheet = MagicMock()
        self.mock_spreadsheet = MagicMock()
        self.mock_spreadsheet.worksheet.return_value = self.mock_worksheet
        self.mock_gc = MagicMock()
        self.mock_gc.open_by_key.return_value = self.mock_spreadsheet

        self.google_sheets_client = GoogleSheets.__new__(GoogleSheets)
        self.google_sheets_client.gc = self.mock_gc

    @patch("ops_utils.google_sheets_util.gspread.service_account_from_dict")
    def test_init_with_service_account_info(self, mock_service_account_from_dict):
        mock_service_account_from_dict.return_value = self.mock_gc
        client = GoogleSheets(service_account_info={"type": "service_account"})
        mock_service_account_from_dict.assert_called_once_with({"type": "service_account"})
        self.assertIs(client.gc, self.mock_gc)

    def test_get_column_values_with_letter(self):
        self.mock_worksheet.col_values.return_value = ["a", "b", "c"]

        result = self.google_sheets_client.get_column_values(SPREADSHEET_ID, SHEET_NAME, "B")

        self.mock_worksheet.col_values.assert_called_once_with(2)
        self.assertEqual(result, ["a", "b", "c"])

    def test_get_column_values_with_number(self):
        self.mock_worksheet.col_values.return_value = ["x"]

        result = self.google_sheets_client.get_column_values(SPREADSHEET_ID, SHEET_NAME, "3")

        self.mock_worksheet.col_values.assert_called_once_with(3)
        self.assertEqual(result, ["x"])

    def test_get_last_row_all_empty(self):
        self.mock_worksheet.col_values.return_value = ["", "", ""]

        result = self.google_sheets_client.get_last_row(SPREADSHEET_ID, SHEET_NAME)

        self.assertEqual(result, 0)

    def test_get_worksheet_as_dict(self):
        self.mock_worksheet.get_all_records.return_value = [{"name": "Alice"}]

        result = self.google_sheets_client.get_worksheet_as_dict(SPREADSHEET_ID, SHEET_NAME)

        self.assertEqual(result, [{"name": "Alice"}])

    def test_batch_update_cells(self):
        updates = [{"cell": "A1", "value": "Name"}, {"cell": "B1", "value": "Age"}]

        self.google_sheets_client.batch_update_cells(SPREADSHEET_ID, SHEET_NAME, updates)

        self.mock_worksheet.batch_update.assert_called_once_with([
            {"range": "A1", "values": [["Name"]]},
            {"range": "B1", "values": [["Age"]]},
        ])

    def test_create_spreadsheet(self):
        self.mock_gc.create.return_value.id = "new-spreadsheet-id"

        result = self.google_sheets_client.create_spreadsheet("My Sheet")

        self.mock_gc.create.assert_called_once_with("My Sheet")
        self.assertEqual(result, "new-spreadsheet-id")

    def test_add_tab_creates_new_tab(self):
        self.mock_spreadsheet.worksheet.side_effect = gspread.exceptions.WorksheetNotFound()
        self.mock_spreadsheet.add_worksheet.return_value.title = "NewTab"

        result = self.google_sheets_client.add_tab(SPREADSHEET_ID, "NewTab")

        self.mock_spreadsheet.add_worksheet.assert_called_once_with(title="NewTab", rows=1000, cols=26)
        self.assertEqual(result, "NewTab")

    def test_add_tab_raises_if_exists(self):
        with self.assertRaises(ValueError):
            self.google_sheets_client.add_tab(SPREADSHEET_ID, SHEET_NAME)

    def test_add_tab_continue_if_exists(self):
        result = self.google_sheets_client.add_tab(SPREADSHEET_ID, SHEET_NAME, continue_if_exists=True)
        self.assertEqual(result, SHEET_NAME)

    def test_write_dicts_to_tab(self):
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

        self.google_sheets_client.write_dicts_to_tab(SPREADSHEET_ID, SHEET_NAME, data, row_order=["name", "age"])

        self.mock_worksheet.update.assert_called_once_with(
            [["name", "age"], ["Alice", 30], ["Bob", 25]], range_name="A1"
        )

    def test_share_spreadsheet(self):
        self.google_sheets_client.share_spreadsheet(SPREADSHEET_ID, "user@example.com", role="reader", notify=False)

        self.mock_spreadsheet.share.assert_called_once_with(
            "user@example.com", perm_type="user", role="reader", notify=False, email_message=None
        )
