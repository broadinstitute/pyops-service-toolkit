from responses import _recorder
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.vars import GCP
import os
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


dataset_id = "882da372-ab26-4598-b9d2-bca61806e6f7"

OUTPUT_YAML = 'out.yaml'


# Test to get yaml returned from the API call. Output will be written to
@_recorder.record(file_path=OUTPUT_YAML)
def _get_yaml(requests_utils: RunRequest) -> None:
    tdr = TDR(request_util=requests_utils)
    results = tdr.get_dataset_info(dataset_id)
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
    requests_utils = RunRequest(token=token, max_retries=1, max_backoff_time=10)
    _get_yaml(requests_utils)
    print(f'wrote to {OUTPUT_YAML}')
