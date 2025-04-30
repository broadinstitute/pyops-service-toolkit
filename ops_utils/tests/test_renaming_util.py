from unittest import TestCase
from unittest.mock import patch, MagicMock

from ops_utils.tdr_utils.renaming_util import (
    GetRowAndFileInfoForReingest,
    BatchCopyAndIngest,
)


class TestGetRowAndFileInfoForReingest(TestCase):
    def setUp(self):
        dataset_id = "fake-dataset-id"
        self.tdr_table_schema = {
            'name': 'sample',
            'columns': [
                {'name': 'sample_id', 'datatype': 'string', 'array_of': False, 'required': True},
                {'name': 'data_type', 'datatype': 'string', 'array_of': False, 'required': False},
                {'name': 'fastq1_path', 'datatype': 'fileref', 'array_of': False, 'required': False},
                {'name': 'collaborator_sample_id', 'datatype': 'string', 'array_of': False, 'required': False},
            ],
            'primaryKey': ['sample_id'],
            'partitionMode': 'none',
            'datePartitionOptions': None,
            'intPartitionOptions': None,
            'rowCount': None
        }

        self.files_dict = {
            '38438b22-5d73-41e9-8ee7-ea6e5c65e731': {
                'fileId': '38438b22-5d73-41e9-8ee7-ea6e5c65e731',
                'collectionId': dataset_id,
                'path': '/some_path/file.fastq1',
                'size': 12,
                'checksums': [{'checksum': 'cfeeffcf', 'type': 'crc32c'}, {'checksum': '4fa151e95de9165038536a4001cd2e5e', 'type': 'md5'}],
                'created': '2024-07-11T14:27:43.218Z',
                'description': None,
                'fileType': 'file',
                'fileDetail': {
                    'datasetId': dataset_id,
                    'mimeType': None,
                    'accessUrl': 'gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq',
                    'loadTag': 'fake-load-tag'
                },
                'directoryDetail': None
            },
        }

        self.table_metrics = [
            {
                "sample_id": "fake-sample-id",
                "fastq1_path": "38438b22-5d73-41e9-8ee7-ea6e5c65e731",
                "data_type": "WGS",
                "collaborator_sample_id": "fake-collab-sample-id",
            }
        ]

        self.original_column = "sample_id"
        self.new_column = "collaborator_sample_id"
        self.row_file_info = GetRowAndFileInfoForReingest(
            table_schema_info=self.tdr_table_schema,
            files_info=self.files_dict,
            table_metrics=self.table_metrics,
            row_identifier=self.original_column,
            original_column=self.original_column,
            new_column=self.new_column,
            temp_bucket="fake-temp-bucket",
        )

    def test_get_new_copy_and_ingest_list(self):
        # Run the method
        actual_ingest_list, actual_files_to_copy = self.row_file_info.get_new_copy_and_ingest_list()
        # We expect that the sample_id in the file name gets replaced with the collaborator_sample_id
        expected_ingest_list = [
            {
                'sample_id': 'fake-sample-id',
                'fastq1_path': {
                    'sourcePath': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq',
                    'targetPath': '/some_path/fake-collab-sample-id.fastq'
                }
            }
        ]
        expected_files_to_copy = [
                [
                    {
                        'source_file': 'gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq',
                        'full_destination_path': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq'
                    }
                ]
        ]


        # Assertions
        self.assertEqual(expected_files_to_copy, actual_files_to_copy)
        self.assertEqual(expected_ingest_list, actual_ingest_list)

    def test_create_paths(self):
        file_info = self.files_dict["38438b22-5d73-41e9-8ee7-ea6e5c65e731"]

        # Run the method
        temp_path, updated_metadata_path, access_url = self.row_file_info._create_paths(
            file_info=file_info,
            og_basename=self.original_column,
            new_basename=self.new_column
        )
        # Assertions
        self.assertEqual(temp_path, "fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq")
        self.assertEqual(updated_metadata_path, "/some_path/fake-sample-id.fastq")
        self.assertEqual(access_url, "gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq")

    def test_create_row_dict(self):
        row_dict = self.table_metrics[0]

        # Run the method
        new_row_dict, tmp_copy_list = self.row_file_info._create_row_dict(row_dict=row_dict, file_ref_columns=["fastq1_path"])

        # Assertions
        self.assertEqual(
            new_row_dict,
            {
                'sample_id': 'fake-sample-id',
                'fastq1_path': {
                    'sourcePath': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq',
                    'targetPath': '/some_path/fake-collab-sample-id.fastq'
                }
            }
        )
        self.assertEqual(
            tmp_copy_list,
            [
                {
                    'source_file': 'gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq',
                    'full_destination_path': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq'
                }
            ]
        )

class TestBatchCopyAndIngest(TestCase):

    @patch("ops_utils.tdr_utils.renaming_util.TDR")
    def setUp(self, mock_tdr):
        self.mock_tdr = mock_tdr

        # Mock the TDR instance
        self.mock_tdr_instance = MagicMock()
        self.mock_tdr_instance.return_value = self.mock_tdr


        self.rows_to_ingest = [
            {
                'sample_id': 'fake-sample-id',
                'fastq1_path': {
                    'sourcePath': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq',
                    'targetPath': '/some_path/fake-collab-sample-id.fastq'
                }
            }
        ]
        self.row_files_to_copy = [
            [
                {
                    'source_file': 'gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq',
                    'full_destination_path': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq'
                }
            ]
        ]
        self.dataset_id = "fake-dataset-id"
        self.table_name = "sample"
        self.update_strategy = "merge"

        self.batch_copy = BatchCopyAndIngest(
            rows_to_ingest=self.rows_to_ingest,
            tdr=self.mock_tdr_instance,
            target_table_name=self.table_name,
            update_strategy=self.update_strategy,
            dataset_id=self.dataset_id,
            row_files_to_copy=self.row_files_to_copy,
        )

    @patch("ops_utils.tdr_utils.renaming_util.GCPCloudFunctions")
    @patch("ops_utils.tdr_utils.renaming_util.StartAndMonitorIngest")
    def test_run(self, mock_ingest, mock_gcp):
        mock_gcp_instance = MagicMock()
        mock_gcp.return_value = mock_gcp_instance

        mock_ingest_instance = MagicMock()
        mock_ingest.return_value = mock_ingest_instance

        # Run the method
        self.batch_copy.run()

        # Assertions
        files_to_copy = [
            {
                'source_file': 'gs://datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-sample-id.fastq',
                'full_destination_path': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq'
            }
        ]
        mock_gcp_instance.multithread_copy_of_files_with_validation.assert_called_once_with(
            files_to_copy=files_to_copy,
            workers=10,
            max_retries=5
        )
        ingest_records = [
            {
                'sample_id': 'fake-sample-id',
                'fastq1_path': {
                    'sourcePath': 'fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq',
                    'targetPath': '/some_path/fake-collab-sample-id.fastq'
                }
            }
        ]
        mock_ingest.assert_called_once_with(
            tdr=self.mock_tdr_instance,
            ingest_records=ingest_records,
            target_table_name=self.table_name,
            dataset_id=self.dataset_id,
            load_tag=f"{self.table_name}_re-ingest",
            bulk_mode=False,
            update_strategy=self.update_strategy,
            waiting_time_to_poll=90
        )
        mock_ingest_instance.run.assert_called_once()
        mock_gcp_instance.delete_multiple_files.assert_called_once_with(
            files_to_delete=['fake-temp-bucket/datarepo-8919718-bucket/fake-dataset-id/38438b22-5d73-41e9-8ee7-ea6e5c65e731/some_path/fake-collab-sample-id.fastq'],
            workers=10,
        )
