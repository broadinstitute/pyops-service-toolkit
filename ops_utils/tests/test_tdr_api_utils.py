import responses

from unittest.mock import MagicMock
from ops_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR, FilterOutSampleIdsAlreadyInDataset
from ..vars import GCP
import json

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)

TEST_DATASET_ID = "eccc736d-2a5a-4d54-a72e-dcdb9f10e67f"
BILLING_PROFILE = "ce149ca7-608b-4d5d-9612-2a43a7378885"
DATASET_NAME = "ops_test_tdr_dataset"
SNAPSHOT_ID = "fc9fb496-41ff-4c9d-a825-514b86100e14"
TABLE_NAME = "test_table"

TEST_INGEST_METRICS = ingest_metrics = [
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
        },
    ]

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
        assert dataset_id == TEST_DATASET_ID

    @responses.activate
    def test_get_dataset_files(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_files.yaml")
        dataset_files = self.tdr_util.get_dataset_files(
            dataset_id=TEST_DATASET_ID,
        )
        assert len(dataset_files) == 3

    @responses.activate
    def test_ingest_to_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/ingest_to_dataset.yaml")
        data_dict = {
            "format": "array",
            "records": TEST_INGEST_METRICS,
            "table": TABLE_NAME,
            "resolve_existing_files": "true",
            "updateStrategy": "REPLACE",
            "load_tag": "test_tag",
            "bulkMode": "false"
        }
        response = self.tdr_util.ingest_to_dataset(
            dataset_id=TEST_DATASET_ID,
            data=json.dumps(data_dict)
        )
        assert response

    @responses.activate
    def test_create_file_dict(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/create_file_dict.yaml")
        dataset_details = self.tdr_util.create_file_dict(
            dataset_id=TEST_DATASET_ID,
        )
        assert len(dataset_details) == 3
        assert '99bf3bbd-5610-4c93-90a1-48d0cf168a6d' in dataset_details

    @responses.activate
    def test_add_user_to_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/add_user_to_dataset.yaml")
        self.tdr_util.add_user_to_dataset(
            user="sahakian@broadinstitute.org",
            policy="custodian",
            dataset_id=TEST_DATASET_ID,
        )
        assert True

    @responses.activate
    def test_remove_user_from_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/remove_user_from_dataset.yaml")
        self.tdr_util.remove_user_from_dataset(
            user="sahakian@broadinstitute.org",
            policy="custodian",
            dataset_id=TEST_DATASET_ID,
        )
        assert True

    @responses.activate
    def test_check_if_dataset_exists(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/check_if_dataset_exists.yaml")
        datasets = self.tdr_util.check_if_dataset_exists(dataset_name=DATASET_NAME, billing_profile=BILLING_PROFILE)
        assert len(datasets) == 1
        assert datasets[0]['id'] == TEST_DATASET_ID

    @responses.activate
    def test_get_dataset_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_info.yaml")
        dataset_info = self.tdr_util.get_dataset_info(dataset_id=TEST_DATASET_ID)
        assert dataset_info['id'] == TEST_DATASET_ID

    @responses.activate
    def test_get_table_schema_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_table_schema_info.yaml")
        table_info = self.tdr_util.get_table_schema_info(
            dataset_id=TEST_DATASET_ID,
            table_name=TABLE_NAME,
        )
        assert table_info['name'] == TABLE_NAME
        assert table_info['columns'][0] == {'name': 'sample_id', 'datatype': 'string', 'array_of': False, 'required': True}

    @responses.activate
    def test_get_dataset_table_metrics(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_table_metrics.yaml")
        table_metrics = self.tdr_util.get_dataset_table_metrics(
            dataset_id=TEST_DATASET_ID,
            target_table_name=TABLE_NAME,
        )
        assert table_metrics[0]['file_1'] == "67c43183-4109-4f24-9744-dfa77ccac72c"
        assert table_metrics[0]['sample_id'] == "sample1"

    @responses.activate
    def test_get_dataset_sample_ids(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_sample_ids.yaml")
        sample_ids = self.tdr_util.get_dataset_sample_ids(
            dataset_id=TEST_DATASET_ID,
            target_table_name=TABLE_NAME,
            entity_id="sample_id"
        )
        assert len(sample_ids) == 6
        assert "sample1" in sample_ids

    @responses.activate
    def test_get_dataset_file_uuids_from_metadata(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_file_uuids_from_metadata.yaml")
        file_uuids = self.tdr_util.get_dataset_file_uuids_from_metadata(dataset_id=TEST_DATASET_ID)
        assert len(file_uuids) == 3
        assert "99bf3bbd-5610-4c93-90a1-48d0cf168a6d" in file_uuids

    @responses.activate
    def test_filter_out_sample_ids_already_in_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/filter_out_sample_ids_already_in_dataset.yaml")
        ingest_metrics = TEST_INGEST_METRICS + [{
                "sample_id": "sample4",
                "file_1": {
                    "sourcePath": "gs://fc-90271ac6-9449-462c-ae97-71d6fad6b669/test.xlsx",
                    "targetPath": "/test.xlsx",
                }
            }
        ]
        results = FilterOutSampleIdsAlreadyInDataset(
            ingest_metrics=ingest_metrics,
            dataset_id=TEST_DATASET_ID,
            tdr=self.tdr_util,
            target_table_name=TABLE_NAME,
            filter_entity_id='sample_id'
        ).run()
        assert len(results) == 1

    @responses.activate
    def test_get_or_create_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_or_create_dataset.yaml")
        dataset_id = self.tdr_util.get_or_create_dataset(
            dataset_name=DATASET_NAME,
            billing_profile=BILLING_PROFILE,
            schema={},
            description="",
            cloud_platform=GCP,
            delete_existing=False,
            continue_if_exists=True
        )
        assert dataset_id == TEST_DATASET_ID

    @responses.activate
    def test_get_job_status(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_job_status.yaml")
        response = self.tdr_util.get_job_status(
            job_id="4Fp5px7uTV29KiMh89e6aA",
        )
        assert response.status_code == 200

    @responses.activate
    def test_get_job_result(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_job_result.yaml")
        response = self.tdr_util.get_job_result(
            job_id="4Fp5px7uTV29KiMh89e6aA",
        )
        assert response.status_code == 201

    @responses.activate
    def test_update_dataset_schema(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/update_dataset_schema.yaml")
        dataset_id = self.tdr_util.update_dataset_schema(
            dataset_id=TEST_DATASET_ID,
            update_note="test",
            tables_to_add=[
                {
                    "name": "test_2",
                    "columns": [
                        {
                            "name": "key",
                            "datatype": "string",
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
        )
        assert dataset_id == TEST_DATASET_ID

    @responses.activate
    def test_get_dataset_snapshots(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_dataset_snapshots.yaml")
        snapshots_dict = self.tdr_util.get_dataset_snapshots(
            dataset_id=TEST_DATASET_ID,
        )
        assert snapshots_dict['items'][0]['id'] == SNAPSHOT_ID

    @responses.activate
    def test_get_files_from_snapshot(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_files_from_snapshot.yaml")
        files_dict = self.tdr_util.get_files_from_snapshot(
            snapshot_id=SNAPSHOT_ID,
        )
        assert len(files_dict) == 2
        assert files_dict[0]['fileId'] == "89135808-96ec-4500-af05-f6bf6b7301f3"

    @responses.activate
    def test_soft_delete_entries(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/soft_delete_entries.yaml")
        self.tdr_util.soft_delete_entries(
            dataset_id=TEST_DATASET_ID,
            table_name=TABLE_NAME,
            datarepo_row_ids=['10983270-be5b-4f03-b3c8-52b51797a31e', '72c016f1-2ed9-4d15-9e71-2a4d6ca7a4a1']
        )
        assert True

    @responses.activate
    def test_soft_delete_all_table_entries(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/soft_delete_all_table_entries.yaml")
        self.tdr_util.soft_delete_all_table_entries(
            dataset_id=TEST_DATASET_ID,
            table_name=TABLE_NAME
        )
        assert True

    @responses.activate
    def test_get_snapshot_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/get_snapshot_info.yaml")
        snapshot_info = self.tdr_util.get_snapshot_info(
            snapshot_id=SNAPSHOT_ID
        )
        assert snapshot_info['id'] == SNAPSHOT_ID

    @responses.activate
    def test_delete_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/delete_file.yaml")
        job_id = self.tdr_util.delete_file(
            dataset_id=TEST_DATASET_ID,
            file_id="99bf3bbd-5610-4c93-90a1-48d0cf168a6d"
        )
        assert job_id

    @responses.activate
    def test_delete_files(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/delete_files.yaml")
        self.tdr_util.delete_files(
            dataset_id=TEST_DATASET_ID,
            file_ids=["ae2438c7-23ef-46e3-80c7-d8a3ef72fe54", "67c43183-4109-4f24-9744-dfa77ccac72c"]
        )
        assert True

    @responses.activate
    def test_delete_dataset(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_util/delete_dataset.yaml")
        self.tdr_util.delete_dataset(
            dataset_id=TEST_DATASET_ID,
        )
        assert True

