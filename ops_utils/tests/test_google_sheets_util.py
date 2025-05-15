import unittest
from unittest.mock import patch, MagicMock
from ops_utils.google_sheets_util import GoogleSheets


class TestGoogleSheets(unittest.TestCase):
    @patch('ops_utils.google_sheets_util.gspread.service_account_from_dict')
    def test_init_with_service_account_info(self, mock_service_account_from_dict):
        mock_gc = MagicMock()
        mock_service_account_from_dict.return_value = mock_gc

        service_account_info = {"type": "service_account", "project_id": "test_project"}
        sheets = GoogleSheets(service_account_info=service_account_info)

        mock_service_account_from_dict.assert_called_once_with(service_account_info)
        self.assertEqual(sheets.gc, mock_gc)

    @patch('ops_utils.google_sheets_util.default')
    @patch('ops_utils.google_sheets_util.gspread.Client')
    def test_init_without_service_account_info(self, mock_gspread_client, mock_default):
        mock_creds = MagicMock()
        mock_default.return_value = (mock_creds, None)
        mock_gspread_client.return_value = MagicMock()

        sheets = GoogleSheets()

        mock_default.assert_called_once_with(scopes=GoogleSheets._SCOPES)
        mock_creds.refresh.assert_not_called()  # Ensure no real refresh is called
        mock_gspread_client.assert_called_once_with(auth=mock_creds)

    @patch('ops_utils.google_sheets_util.GoogleSheets._open_worksheet')
    def test_update_cell(self, mock_open_worksheet):
        mock_worksheet = MagicMock()
        mock_open_worksheet.return_value = mock_worksheet

        sheets = GoogleSheets()
        sheets.update_cell("spreadsheet_id", "worksheet_name", "A1", "Test Value")

        mock_open_worksheet.assert_called_once_with("spreadsheet_id", "worksheet_name")
        mock_worksheet.update.assert_called_once_with("A1", [["Test Value"]])

    @patch('ops_utils.google_sheets_util.GoogleSheets._open_worksheet')
    def test_get_cell_value(self, mock_open_worksheet):
        mock_worksheet = MagicMock()
        mock_open_worksheet.return_value = mock_worksheet
        mock_worksheet.acell.return_value.value = "Test Value"

        sheets = GoogleSheets()
        value = sheets.get_cell_value("spreadsheet_id", "worksheet_name", "A1")

        mock_open_worksheet.assert_called_once_with("spreadsheet_id", "worksheet_name")
        mock_worksheet.acell.assert_called_once_with("A1")
        self.assertEqual(value, "Test Value")

    @patch('ops_utils.google_sheets_util.GoogleSheets._open_worksheet')
    def test_get_last_row(self, mock_open_worksheet):
        mock_worksheet = MagicMock()
        mock_open_worksheet.return_value = mock_worksheet
        mock_worksheet.col_values.return_value = ["Row1", "Row2", "", ""]

        sheets = GoogleSheets()
        last_row = sheets.get_last_row("spreadsheet_id", "worksheet_name")

        mock_open_worksheet.assert_called_once_with("spreadsheet_id", "worksheet_name")
        mock_worksheet.col_values.assert_called_once_with(1)
        self.assertEqual(last_row, 2)