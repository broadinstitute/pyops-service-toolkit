# test_google_sheets.py

from ops_utils.google_sheets_util import GoogleSheets
import responses

SPREADSHEET_ID = "1GjeRUYtkkT1bGxVGE4DLHrA5itQGWjvCh0xNWpdHTTQ"
SHEET_NAME = "Sheet1"


class TestGoogleSheets:
    """Test suite for GoogleSheets class."""

    @responses.activate
    def test_get_cell_value(self):
        """Test get_cell_value method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/get_cell_value.yaml")
        result = GoogleSheets().get_cell_value(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A1",
        )
        assert result == "Stuff"

    @responses.activate
    def test_get_last_row(self):
        """Test get_last_row method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/get_last_row.yaml")
        result = GoogleSheets().get_last_row(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
        )
        assert result == 3

    @responses.activate
    def test_update_and_get_cell_value(self):
        """Test update_cell method."""
        responses._add_from_file(file_path="ops_utils/tests/data/google_sheets_util/update_and_get_cell_value.yaml")
        GoogleSheets().update_cell(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A4",
            value="New Value",
        )
        result = GoogleSheets().get_cell_value(
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_name=SHEET_NAME,
            cell="A4",
        )
        assert result == "New Value"
