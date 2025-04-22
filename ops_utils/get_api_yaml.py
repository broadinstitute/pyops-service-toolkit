from email import policy

from responses import _recorder
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.vars import GCP
from ops_utils.tdr_utils.tdr_table_utils import SetUpTDRTables
import os
import json

BILLING_ID = "ce149ca7-608b-4d5d-9612-2a43a7378885"
DATASET_ID = "eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"
DATASET_NAME = "ops_test_tdr_dataset"
OUTPUT_YAML = 'out.yaml'

# Test to get yaml returned from the API call. Output will be written to
@_recorder.record(file_path=OUTPUT_YAML)
def _get_yaml(requests_utils):
    tdr_util = TDR(request_util=requests_utils)
    results = tdr_util.get_data_set_file_uuids_from_metadata(
        dataset_id=DATASET_ID
    )
    if results:
        print(results)

if __name__ == '__main__':
    print("Deleting old yaml file")
    # Delete the old yaml file if it exists
    try:
        os.remove(OUTPUT_YAML)
    except FileNotFoundError:
        pass
    token = Token(cloud=GCP)
    requests_utils = RunRequest(token=token)
    tdr = TDR(request_util=requests_utils)
    _get_yaml(requests_utils)
    print(f'wrote to {OUTPUT_YAML}')
