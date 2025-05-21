"""Module to interact with Google Sheets API."""
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

        This method sets up the Google Sheets client using either the provided service account
        credentials or the application-default credentials. If no service account information
        is provided, ensure that the application-default credentials are properly configured.

        **Args:**
        - service_account_info (Optional[dict]): A dictionary containing the service account credentials.

        **Example:**
        To use application-default credentials, run the following command:
        ```
        gcloud auth application-default login \
        --scopes=https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/cloud-platform
        ```
        """
        if service_account_info:
            self.gc = gspread.service_account_from_dict(service_account_info)
        else:
            # This assumes gcloud auth application-default login has been run
            creds, _ = default(scopes=self._SCOPES)
            creds.refresh(Request())
            self.gc = gspread.Client(auth=creds)

    def _open_worksheet(self, spreadsheet_id: str, worksheet_name: str) -> gspread.Worksheet:
        """
        Open a spreadsheet by its ID.

        **Args:**
        - spreadsheet_id (str): The ID of the Google Sheet.
        """
        spreadsheet = self.gc.open_by_key(spreadsheet_id)
        return spreadsheet.worksheet(worksheet_name)

    def update_cell(self, spreadsheet_id: str, worksheet_name: str, cell: str, value: str) -> None:
        """
        Update a specific cell in the sheet.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.
        - cell (str): A1-style cell notation.
        - value (str): Value to insert.
        """
        worksheet = self._open_worksheet(spreadsheet_id, worksheet_name)
        worksheet.update([[value]], range_name=cell)

    def get_cell_value(self, spreadsheet_id: str, worksheet_name: str, cell: str) -> str:
        """
        Get the value of a specific cell.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.
        - cell (str): A1-style cell reference.

        **Returns:**
        - str or None: Cell value or None if empty.
        """
        ws = self._open_worksheet(spreadsheet_id, worksheet_name)
        return ws.acell(cell).value

    def get_last_row(self, spreadsheet_id: str, worksheet_name: str) -> int:
        """
        Get the last non-empty row in the specified column, accounting for trailing empty rows.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.

        **Returns:**
        - int: The last non-empty row number.
        """
        ws = self._open_worksheet(spreadsheet_id, worksheet_name)
        col_values = ws.col_values(1)  # Get all values in the first column
        for row_index in range(len(col_values), 0, -1):  # Iterate from the last row to the first
            if col_values[row_index - 1]:  # Check if the cell is not empty
                return row_index
        return 0  # Return 0 if all rows are empty

    def get_column_values(self, spreadsheet_id: str, worksheet_name: str, column: str) -> list:
        """
        Get all values in a specific column in order of row.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.
        - column (str): Column identifier (e.g., "A" or "1").

        **Returns:**
        - list: List of values in the column.
        """
        ws = self._open_worksheet(spreadsheet_id, worksheet_name)

        # Convert column letter to number if it's a letter
        if column.isalpha():
            # gspread uses 1-based indexing for columns
            column_index = 0
            for char in column.upper():
                column_index = column_index * 26 + (ord(char) - ord('A') + 1)
        else:
            # If column is already a number
            column_index = int(column)

        return ws.col_values(column_index)
