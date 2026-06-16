"""Module to interact with Google Sheets API."""
from typing import Optional
from google.auth import default
from google.auth.transport.requests import Request
import logging
import gspread


class GoogleSheets:
    """Class to interact with Google Sheets API."""

    _SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

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

    def get_worksheet_as_dict(self, spreadsheet_id: str, worksheet_name: str) -> list[dict]:
        """
        Get all data from a worksheet as a list of dictionaries.

        The first row is used as the header/keys for the dictionaries.
        Each subsequent row becomes a dictionary with the header values as keys.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.

        **Returns:**
        - list[dict]: List of dictionaries, where each dictionary represents a row
                      with column headers as keys.
        """
        ws = self._open_worksheet(spreadsheet_id, worksheet_name)
        return ws.get_all_records()

    def batch_update_cells(self, spreadsheet_id: str, worksheet_name: str, updates: list[dict[str, str]]) -> None:
        """
        Update multiple cells in a single batch request.

        This method is more efficient than calling update_cell multiple times
        as it sends all updates in a single API request.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.
        - updates (list[dict[str, str]]): List of dictionaries, each containing:
            - 'cell': A1-style cell notation (e.g., 'A1', 'B5')
            - 'value': Value to insert

        **Example:**
        ```python
        updates = [
            {'cell': 'A1', 'value': 'Name'},
            {'cell': 'B1', 'value': 'Age'},
            {'cell': 'A2', 'value': 'John'},
            {'cell': 'B2', 'value': '30'}
        ]
        gs.batch_update_cells(spreadsheet_id, worksheet_name, updates)
        ```
        """
        worksheet = self._open_worksheet(spreadsheet_id, worksheet_name)

        # Build the batch update data
        batch_data = []
        for update in updates:
            batch_data.append({
                'range': update['cell'],
                'values': [[update['value']]]
            })

        # Perform batch update
        worksheet.batch_update(batch_data)

    def create_spreadsheet(self, title: str) -> str:
        """
        Create a new Google Spreadsheet.

        **Args:**
        - title (str): The title of the new spreadsheet.

        **Returns:**
        - str: The ID of the newly created spreadsheet.
        """
        return self.gc.create(title).id

    def add_tab(
            self,
            spreadsheet_id: str,
            tab_name: str,
            rows: int = 1000,
            cols: int = 26,
            continue_if_exists: bool = False,
    ) -> str:
        """
        Add a new tab (worksheet) to an existing spreadsheet.

        **Args:**
        - spreadsheet_id (str): The ID of the Google Sheet.
        - tab_name (str): The name for the new tab.
        - rows (int): Initial number of rows (default 1000).
        - cols (int): Initial number of columns (default 26).
        - continue_if_exists (bool): If True, return the existing tab name without
                                     raising an error if the tab already exists (default False).

        **Returns:**
        - str: The title of the tab (newly created or existing).

        **Raises:**
        - ValueError: If the tab already exists and continue_if_exists is False.
        """
        spreadsheet = self.gc.open_by_key(spreadsheet_id)
        try:
            spreadsheet.worksheet(tab_name)
            # Tab already exists
            if continue_if_exists:
                logging.info(f"Tab '{tab_name}' already exists. Not failing because continue_if_exists is True.")
                return tab_name
            raise ValueError(f"Tab '{tab_name}' already exists in spreadsheet '{spreadsheet_id}'. Use continue_if_exists to bypass")
        except gspread.exceptions.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols).title

    def write_dicts_to_tab(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        data: list[dict],
        row_order: list[str],
        start_cell: str = "A1",
    ) -> None:
        """
        Write a list of dictionaries to a worksheet tab using a specified column order.

        The first row written will be the headers (from row_order), followed by
        one row per dictionary in data.

        **Args:**
        - spreadsheet_id (str): Spreadsheet ID.
        - worksheet_name (str): Sheet/tab name.
        - data (list[dict]): List of dictionaries to write.
        - row_order (list[str]): Ordered list of keys defining the column order.
                                 Also used as the header row.
        - start_cell (str): A1-style cell to begin writing at (default "A1").

        **Example:**
        ```python
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob',   'age': 25},
        ]
        gs.write_dicts_to_tab(spreadsheet_id, 'Sheet1', data, row_order=['name', 'age'])
        ```
        """
        ws = self._open_worksheet(spreadsheet_id, worksheet_name)
        rows = [row_order]  # Header row
        for record in data:
            rows.append([record.get(key, "") for key in row_order])
        ws.update(rows, range_name=start_cell)

    def share_spreadsheet(
        self,
        spreadsheet_id: str,
        email: str,
        role: str = "writer",
        notify: bool = True,
        email_message: Optional[str] = None,
    ) -> None:
        """
        Share a spreadsheet with a specified email address.

        Requires the ``https://www.googleapis.com/auth/drive`` scope. When using
        application-default credentials, make sure to include it:
        ```
        gcloud auth application-default login \
        --scopes=https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform
        ```

        **Args:**
        - spreadsheet_id (str): The ID of the Google Sheet to share.
        - email (str): The email address to share the spreadsheet with.
        - role (str): Permission role — ``"reader"``, ``"writer"``, or ``"owner"`` (default ``"writer"``).
        - notify (bool): Whether to send a notification email to the recipient (default ``True``).
        - email_message (Optional[str]): Custom message to include in the notification email.
        """
        spreadsheet = self.gc.open_by_key(spreadsheet_id)
        spreadsheet.share(email, perm_type="user", role=role, notify=notify, email_message=email_message)

