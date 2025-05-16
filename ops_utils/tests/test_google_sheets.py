# test_google_sheets.py

import pytest
from unittest.mock import patch, MagicMock

class TestGoogleSheets:
    """Test suite for GoogleSheets class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup the mock for GoogleSheets."""
        with patch('ops_utils.google_sheets_util.GoogleSheets', autospec=True) as MockGoogleSheetsUtils:
            self.mock_google_sheets = MockGoogleSheetsUtils.return_value
            yield

    def test_update_cell(self):
        """Test updating a cell in a Google Sheet."""
        with patch.object(self.mock_google_sheets, 'update_cell', return_value=None):
            self.mock_google_sheets.update_cell(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1",
                cell="A1",
                value="Test Value"
            )
            self.mock_google_sheets.update_cell.assert_called_once_with(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1",
                cell="A1",
                value="Test Value"
            )

    def test_get_cell_value(self):
        """Test getting a cell value from a Google Sheet."""
        with patch.object(self.mock_google_sheets, 'get_cell_value', return_value="Test Value"):
            result = self.mock_google_sheets.get_cell_value(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1",
                cell="A1"
            )
            assert result == "Test Value"
            self.mock_google_sheets.get_cell_value.assert_called_once_with(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1",
                cell="A1"
            )

    def test_get_last_row(self):
        """Test getting the last non-empty row in a Google Sheet."""
        with patch.object(self.mock_google_sheets, 'get_last_row', return_value=10):
            result = self.mock_google_sheets.get_last_row(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1"
            )
            assert result == 10
            self.mock_google_sheets.get_last_row.assert_called_once_with(
                spreadsheet_id="test_sheet_id",
                worksheet_name="Sheet1"
            )
