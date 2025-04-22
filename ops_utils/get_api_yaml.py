from responses import _recorder
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.vars import GCP
import os

BILLING_ID = "ce149ca7-608b-4d5d-9612-2a43a7378885"
DATASET_ID = "eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"
DATASET_NAME = "ops_test_tdr_dataset"
OUTPUT_YAML = 'out.yaml'


# Test to get yaml returned from the API call. Output will be written to
@_recorder.record(file_path=OUTPUT_YAML)
def _get_yaml(requests_utils: RunRequest) -> None:
    tdr_util = TDR(request_util=requests_utils)
    results = tdr_util.get_or_create_dataset(
        dataset_name=DATASET_NAME,
        billing_profile=BILLING_ID,
        schema={},
        description="",
        cloud_platform=GCP,
        delete_existing=False,
        continue_if_exists=True
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
