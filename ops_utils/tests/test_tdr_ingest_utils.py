import responses
from unittest.mock import MagicMock

from ops_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.terra_util import TerraWorkspace
from ops_utils.tdr_utils.tdr_ingest_utils import (
    ConvertTerraTableInfoForIngest,
    GetPermissionsForWorkspaceIngest,
    FilterAndBatchIngest
)

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)

TEST_DATASET_ID = "882da372-ab26-4598-b9d2-bca61806e6f7"
TEST_BILLING_PROJECT = "ops-integration-billing"
TEST_WORKSPACE_NAME = "sn_testing_Staging"


class TestTDRIngestUtils:
    tdr = TDR(request_util=request_util)
    workspace = TerraWorkspace(
        billing_project=TEST_BILLING_PROJECT,
        workspace_name=TEST_WORKSPACE_NAME,
        request_util=request_util
    )

    def test_convert_terra_table_info_for_ingest(self):
        terra_metrics = [
            {'attributes': {'column_a': 'aksfj', 'column_c': 'bbb', 'column_b': 'eugneu'}, 'entityType': 'sample', 'name': 'sample_a'}, {'attributes': {'column_a': '390f', 'column_c': 'fnnnf', 'column_b': 'jfa9f'}, 'entityType': 'sample', 'name': 'sample_b'}, {'attributes': {'column_a': '3jj39fj', 'column_c': '000sf9', 'column_b': 'jffnai'}, 'entityType': 'sample', 'name': 'sample_c'}
        ]
        converted_metrics = ConvertTerraTableInfoForIngest(
            table_metadata=terra_metrics,
            columns_to_ignore=['column_a'],
            tdr_row_id="sample_id"
        ).run()
        expected_converted_metrics = [{'sample_id': 'sample_a', 'column_c': 'bbb', 'column_b': 'eugneu'}, {'sample_id': 'sample_b', 'column_c': 'fnnnf', 'column_b': 'jfa9f'}, {'sample_id': 'sample_c', 'column_c': '000sf9', 'column_b': 'jffnai'}]
        assert converted_metrics == expected_converted_metrics

    @responses.activate
    def test_get_permissions_for_workspace_ingest(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_ingest_util/get_permissions_for_workspace_ingest.yaml")
        dataset_info = self.tdr.get_dataset_info(dataset_id=TEST_DATASET_ID).json()
        GetPermissionsForWorkspaceIngest(
            terra_workspace=self.workspace,
            dataset_info=dataset_info,
            added_to_auth_domain=True,
        ).run()
        assert True

    @responses.activate
    def test_filter_and_batch_ingest(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_ingest_util/filter_and_batch_ingest.yaml")
        FilterAndBatchIngest(
            tdr=self.tdr,
            filter_existing_ids=True,
            unique_id_field="sample_id",
            table_name="sample",
            ingest_metadata=[
                {'sample_id': 'sample_a', 'column_c': 'bbb', 'column_b': 'eugneu',
                 'test_file': 'gs://fc-secure-e3595d39-4029-45ff-81cf-28be21a98c53/test.1.txt'},
                {'sample_id': 'sample_b', 'column_c': 'fnnnf', 'column_b': 'jfa9f',
                 'test_file': 'gs://fc-secure-e3595d39-4029-45ff-81cf-28be21a98c53/test.2.txt'},
                {'sample_id': 'sample_c', 'column_c': '000sf9', 'column_b': 'jffnai',
                 'test_file': 'gs://fc-secure-e3595d39-4029-45ff-81cf-28be21a98c53/test.3.txt'}
            ],
            dataset_id=TEST_DATASET_ID,
            ingest_waiting_time_poll=5,
            ingest_batch_size=2,
            bulk_mode=False,
            update_strategy="replace",
            load_tag="test_load_tag",
            test_ingest=False,
            file_to_uuid_dict={'/fake/file': 'fake_uuid'},
            schema_info={
                "name": "sample",
                "columns": [
                    {
                        "name": "sample_id",
                        "datatype": "string",
                        "array_of": False,
                        "required": True
                    },
                    {
                        "name": "column_a",
                        "datatype": "string",
                        "array_of": False,
                        "required": False
                    },
                    {
                        "name": "column_b",
                        "datatype": "string",
                        "array_of": False,
                        "required": False
                    },
                    {
                        "name": "column_c",
                        "datatype": "string",
                        "array_of": False,
                        "required": False
                    },
                    {
                        "name": "test_file",
                        "datatype": "fileref",
                        "array_of": False,
                        "required": False
                    }
                ],
                "primaryKey": [
                    "sample_id"
                ]
            }
        ).run()
        assert True