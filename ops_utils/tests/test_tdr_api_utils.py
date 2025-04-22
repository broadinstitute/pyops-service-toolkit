import responses

from unittest.mock import MagicMock, mock_open, patch
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.vars import GCP
import json

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)

TEST_DATASET = "eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"
BILLING_PROFILE = "ce149ca7-608b-4d5d-9612-2a43a7378885"
DATASET_NAME = "ops_test_tdr_dataset"

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
            dataset_name=DATASET_NAME,
            description='Test Dataset',
            profile_id=BILLING_PROFILE
        )
        assert dataset_id == TEST_DATASET

    @responses.activate
    def test_get_dataset_files(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_files.yaml")
        dataset_files = self.tdr_util.get_dataset_files(
            dataset_id=TEST_DATASET,
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
            dataset_id=TEST_DATASET,
            data=json.dumps(data_dict)
        )
        assert response

    @responses.activate
    def test_create_file_dict(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/create_file_dict.yaml")
        dataset_details = self.tdr_util.create_file_dict(
            dataset_id=TEST_DATASET,
        )
        assert len(dataset_details) == 3
        assert '99bf3bbd-5610-4c93-90a1-48d0cf168a6d' in dataset_details

    @responses.activate
    def test_add_user_to_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/add_user_to_dataset.yaml")
        self.tdr_util.add_user_to_dataset(
            user="sahakian@broadinstitute.org",
            policy="custodian",
            dataset_id=TEST_DATASET,
        )
        assert True

    @responses.activate
    def test_remove_user_from_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/remove_user_from_dataset.yaml")
        self.tdr_util.remove_user_from_dataset(
            user="sahakian@broadinstitute.org",
            policy="custodian",
            dataset_id=TEST_DATASET,
        )
        assert True

    @responses.activate
    def test_check_if_dataset_exists(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/check_if_dataset_exists.yaml")
        datasets = self.tdr_util.check_if_dataset_exists(dataset_name=DATASET_NAME, billing_profile=BILLING_PROFILE)
        assert len(datasets) == 1
        assert datasets[0]['id'] == TEST_DATASET

    @responses.activate
    def test_get_dataset_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_info.yaml")
        dataset_info = self.tdr_util.get_dataset_info(dataset_id=TEST_DATASET)
        assert dataset_info['id'] == TEST_DATASET

    @responses.activate
    def test_get_table_schema_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_table_schema_info.yaml")
        table_info = self.tdr_util.get_table_schema_info(
            dataset_id=TEST_DATASET,
            table_name="test_table",
        )
        assert table_info['name'] == 'test_table'
        assert table_info['columns'][0] == {'name': 'sample_id', 'datatype': 'string', 'array_of': False, 'required': True}

    @responses.activate
    def test_get_dataset_table_metrics(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_table_metrics.yaml")
        table_metrics = self.tdr_util.get_dataset_table_metrics(
            dataset_id=TEST_DATASET,
            target_table_name="test_table",
        )
        assert table_metrics[0]['file_1'] == "67c43183-4109-4f24-9744-dfa77ccac72c"
        assert table_metrics[0]['sample_id'] == "sample1"

    @responses.activate
    def test_get_data_set_sample_ids(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_data_set_sample_ids.yaml")
        sample_ids = self.tdr_util.get_data_set_sample_ids(
            dataset_id=TEST_DATASET,
            target_table_name="test_table",
            entity_id="sample_id"
        )
        assert len(sample_ids) == 6
        assert "sample1" in sample_ids

    @responses.activate
    def test_get_data_set_file_uuids_from_metadata(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_data_set_file_uuids_from_metadata.yaml")
        file_uuids = self.tdr_util.get_data_set_file_uuids_from_metadata(dataset_id=TEST_DATASET)
        assert len(file_uuids) == 3
        assert "99bf3bbd-5610-4c93-90a1-48d0cf168a6d" in file_uuids