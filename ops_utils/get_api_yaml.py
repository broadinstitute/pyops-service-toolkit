"""Util for capturing API responses for use in unit tests.

@private
"""
from responses import _recorder
from ops_utils.google_sheets_util import GoogleSheets
import os
import logging
import yaml
import json

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


spreadsheet_id = "1GjeRUYtkkT1bGxVGE4DLHrA5itQGWjvCh0xNWpdHTTQ"
sheet_name = "Sheet1"
OUTPUT_YAML = 'out.yaml'


def replace_access_token_in_yaml(file_path: str, new_token: str = "REDACTED") -> None:
    """Replaces the access token in the given YAML file."""
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)

    # Traverse and replace access_token
    if isinstance(data, dict) and "responses" in data:
        for response in data["responses"]:
            if "response" in response and "body" in response["response"]:
                body = response["response"]["body"]
                try:
                    body_dict = json.loads(body)  # Parse the body as JSON
                    if "access_token" in body_dict:
                        body_dict["access_token"] = new_token
                        response["response"]["body"] = json.dumps(body_dict)  # Convert back to JSON string
                        logging.info(f"Replaced access token to be {new_token}")
                except json.JSONDecodeError:
                    logging.error("The body field is not valid JSON.")
            else:
                logging.warning("The body field is missing in one of the responses.")
    else:
        logging.warning("The YAML file does not contain a 'responses' field or is not a dictionary.")

    with open(file_path, 'w') as file:
        yaml.dump(data, file)


# Test to get yaml returned from the API call. Output will be written to
@_recorder.record(file_path=OUTPUT_YAML)
def _get_yaml() -> None:
    """Get the yaml file from the API call. Update to run whatever you want to capture yaml for"""
    GoogleSheets().update_cell(
        spreadsheet_id=spreadsheet_id,
        worksheet_name=sheet_name,
        cell="A4",
        value="New Value",
    )
    cell_value = GoogleSheets().get_cell_value(
        spreadsheet_id=spreadsheet_id,
        worksheet_name=sheet_name,
        cell="A4",
    )
    print(cell_value)


if __name__ == '__main__':
    print("Deleting old yaml file")
    # Delete the old yaml file if it exists
    try:
        os.remove(OUTPUT_YAML)
    except FileNotFoundError:
        pass
    _get_yaml()
    replace_access_token_in_yaml(OUTPUT_YAML)
    print(f'wrote to {OUTPUT_YAML}')
