from typing import Optional
from google.auth import default
from google.auth.transport.requests import Request
import gspread


class GoogleSheets:
    """Class to interact with Google Sheets API."""

    _SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, service_account_info: Optional[dict] = None):
        """
        Initialize the GoogleSheets instance using the service account or user credentials.

        **Args:**
        - service_account_info (dict): A dictionary containing the service account credentials.
        """
        if service_account_info:
            self.gc = gspread.service_account_from_dict(service_account_info)
        else:
            # This assumes gcloud auth application-default login has been run
            creds, _ = default(scopes=self._SCOPES)
            creds.refresh(Request())
            self.gc = gspread.Client(auth=creds)

    def _open_sheet(self, spreadsheet_id: str, sheet_name: str) -> gspread.Worksheet:
        """
        Open a spreadsheet by its ID.

        **Args:**
        - spreadsheet_id (str): The ID of the Google Sheet.
        """
        sheet = self.gc.open_by_key(spreadsheet_id)
        return sheet.worksheets(sheet_name)

    def update_cell(self, spreadsheet_id: str, sheet_name: str, cell: str, value: str) -> None:
        """
        Update a specific cell in the sheet.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - sheet_name (str): Sheet/tab name.
        - cell (str): A1-style cell notation.
        - value (str): Value to insert.
        """
        worksheet = self._open_sheet(spreadsheet_id, sheet_name)
        worksheet.update(cell, value)

    def get_cell_value(self, spreadsheet_id: str, sheet_name: str, cell: str) -> str:
        """
        Get the value of a specific cell.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - sheet_name (str): Sheet/tab name.
        - cell (str): A1-style cell reference.

        **Returns:**
        - str or None: Cell value or None if empty.
        """
        ws = self._open_sheet(spreadsheet_id, sheet_name)
        return ws.acell(cell).value

    def get_last_row(self, spreadsheet_id: str, sheet_name: str) -> int:
        """
        Get the last non-empty row in the specified column.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - sheet_name (str): Sheet/tab name.

        **Returns:**
        - int: The last non-empty row number.
        """
        ws = self._open_sheet(spreadsheet_id, sheet_name)
        breakpoint()
        return len(list(filter(None, ws.col_values(1)))) + 1
