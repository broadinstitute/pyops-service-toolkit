import responses

from unittest.mock import MagicMock, mock_open, patch
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.vars import GCP
import json

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)

class TestTerraWorkspaceUtils:
    tdr_util = TDR(request_util=request_util)


    @responses.activate
    def test_create_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/create_dataset.yaml")
        dataset_id = self.tdr_util.create_dataset(
            schema={
                "tables": [
                    {
                        "name": "ingestion_reference",
                        "columns": [
                            {
                                "name": "key", "datatype": "string",
                                "array_of": False,
                                "required": True
                            },
                            {
                                "name": "value",
                                "datatype": "string",
                                "array_of": False,
                                "required": True
                            }
                        ],
                        "primaryKey": ["key"]
                    }
                ]
            },
            cloud_platform=GCP,
            dataset_name='ops_test_tdr_dataset',
            description='Test Dataset',
            profile_id='ce149ca7-608b-4d5d-9612-2a43a7378885'
        )
        assert dataset_id == "eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"

    @responses.activate
    def test_get_dataset_files(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_files.yaml")
        dataset_files = self.tdr_util.get_dataset_files(
            dataset_id="eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"
        )
        assert len(dataset_files) == 3

    @responses.activate
    def test_ingest_to_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/ingest_to_dataset.yaml")
        data_dict = {
            "format": "array",
            "records": [
                {
                    "sample_id": "sample1",

                    "file_1": {
                        "sourcePath": "gs://fc-90271ac6-9449-462c-ae97-71d6fad6b669/test.txt",
                        "targetPath": "/test.txt",
                    }
                },
                {
                    "sample_id": "sample2",

                    "file_1": {
                        "sourcePath": "gs://fc-90271ac6-9449-462c-ae97-71d6fad6b669/test.csv",
                        "targetPath": "/test.csv",
                    }
                },
                {
                    "sample_id": "sample3",

                    "file_1": {
                        "sourcePath": "gs://fc-90271ac6-9449-462c-ae97-71d6fad6b669/test.tsv",
                        "targetPath": "/test.tsv",
                    }
                }
            ],
            "table": "test_table",
            "resolve_existing_files": "true",
            "updateStrategy": "REPLACE",
            "load_tag": "test_tag",
            "bulkMode": "false"
        }
        response = self.tdr_util.ingest_to_dataset(
            dataset_id="eccc736d-2a5a-4d54-a72e-dcdb9f10e67f",
            data=json.dumps(data_dict)
        )
        assert response

