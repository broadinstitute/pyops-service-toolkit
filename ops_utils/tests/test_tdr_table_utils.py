from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from ops_utils.tdr_utils.tdr_table_utils import SetUpTDRTables, MatchSchemas

TARGET_TABLE = "sample"
PRIMARY_KEY = f'{TARGET_TABLE}_id'

NEW_TABLE_INFO = {
    TARGET_TABLE: {
        "table_name": TARGET_TABLE,
        "primary_key": PRIMARY_KEY,
        "ingest_metadata": [
            {
                PRIMARY_KEY: "sample_a",
                "participant_id": "participant_a",
                "sex": "male",
            },
            {
                PRIMARY_KEY: "sample_b",
                "participant_id": "participant_b",
                "sex": "female",
            }
        ],
        "datePartitionOptions": None
    }
}


class TestSetUpTDRTables(TestCase):

    @patch("ops_utils.tdr_utils.tdr_api_utils.TDR")
    def setUp(self, mock_tdr):
        self.mock_tdr = mock_tdr

        # Mock the TDR instance
        self.mock_tdr_instance = MagicMock()
        self.mock_tdr_instance.return_value = self.mock_tdr

        self.dataset_id = "fake_dataset_id"
        self.tdr_table_util = SetUpTDRTables(
            tdr=self.mock_tdr_instance,
            dataset_id=self.dataset_id,
            table_info_dict=NEW_TABLE_INFO,
        )

    def test_run_new_table_nonexistent_in_dataset(self):
        """Tests the 'run' method when the new table to ingest DOES NOT already exist in the dataset"""
        columns_before  = [
            {'name': 'column_a', 'datatype': 'string', 'array_of': False, 'required': True},
            {'name': 'column_b', 'datatype': 'string', 'array_of': False, 'required': True}
        ]
        columns_after = columns_before + [
            {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
        ]
        # The "get_dataset_info" method is called twice
        # The return value of the first call is the dataset info before the new table is added
        # The return value of the second call is the dataset info after the new table is added
        self.mock_tdr_instance.get_dataset_info.side_effect = [
            MagicMock(
                status_code=200,
                json=MagicMock(
                    return_value={
                        'id': 'fake-dataset-id',
                        'name': 'fake-dataset-name', 'schema': {
                            'tables': [
                                {
                                    'name': 'some-fake-table',
                                    'columns': columns_before,
                                    'primaryKey': ['column_a'],
                                    'partitionMode': 'none',
                                    'datePartitionOptions': None,
                                    'intPartitionOptions': None,
                                    'rowCount': None
                                }
                            ],
                            'relationships': [],
                            'assets': []
                        },
                    }
                )
            ),
            MagicMock(
                status_code=200,
                json=MagicMock(
                    return_value={
                        'id': 'fake-dataset-id',
                        'name': 'fake-dataset-name',
                        'schema': {
                            'tables': [
                                {
                                    'name': 'some-fake-table',
                                    'columns': columns_after,
                                    'primaryKey': ['column_a'],
                                    'partitionMode': 'none',
                                    'datePartitionOptions': None,
                                    'intPartitionOptions': None,
                                    'rowCount': None
                                }
                            ],
                            'relationships': [],
                            'assets': []
                        },
                    }
                )
            ),
        ]
        # Run the method
        actual_table_names_and_keys = self.tdr_table_util.run()
        # Assertions
        self.mock_tdr_instance.update_dataset_schema.assert_called_once_with(
            dataset_id=self.dataset_id,
            update_note=f"Creating tables in dataset {self.dataset_id}",
            tables_to_add=[
                {
                    'name': 'sample',
                    'columns': [
                        {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                        {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                        {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
                    ],
                    'primaryKey': ['sample_id']
                }
            ]
        )
        expected_table_names_and_keys = {
            'some-fake-table': {
                'column_a': {'name': 'column_a', 'datatype': 'string', 'array_of': False, 'required': True},
                'column_b': {'name': 'column_b', 'datatype': 'string', 'array_of': False, 'required': True},
                'sample_id': {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                'participant_id': {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                'sex': {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
            }
        }
        self.assertEqual(actual_table_names_and_keys, expected_table_names_and_keys)
        # Assert that get_dataset_info was called twice with the same parameters
        self.mock_tdr_instance.get_dataset_info.assert_has_calls(
            calls=[
                call(dataset_id=self.dataset_id, info_to_include=['SCHEMA']),
                call(dataset_id=self.dataset_id, info_to_include=['SCHEMA']),
            ]
        )

    def test_run_new_table_exists_in_dataset(self):
        """Tests the 'run' method when the new table to ingest ALREADY EXISTS in the dataset AND
        does not need to be updated"""

        columns = [
            {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
        ]

        self.mock_tdr_instance.get_dataset_info.return_value = MagicMock(
            status_code=200,
            json=MagicMock(
                return_value={
                    'id': 'fake-dataset-id',
                    'name': 'fake-dataset-name',
                    'schema': {
                        'tables': [
                            {
                                'name': 'sample',
                                'columns': columns,
                                'primaryKey': ['sample_id'],
                                'partitionMode': 'none',
                                'datePartitionOptions': None,
                                'intPartitionOptions': None,
                                'rowCount': None
                            }
                        ],
                        'relationships': [],
                        'assets': []
                    },
                }
            )
        )
        # Call the method
        actual_table_names_and_keys = self.tdr_table_util.run()
        # Assertions
        expected_table_names_and_keys = {
            'sample': {
                'sample_id': {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                'participant_id': {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                'sex': {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
            }
        }
        self.mock_tdr_instance.update_dataset_schema.assert_not_called()
        self.assertEqual(actual_table_names_and_keys, expected_table_names_and_keys)
        # Assert that get_dataset_info was called twice with the same parameters
        self.mock_tdr_instance.get_dataset_info.assert_has_calls(
            calls=[
                call(dataset_id=self.dataset_id, info_to_include=['SCHEMA']),
                call().json(),
                call(dataset_id=self.dataset_id, info_to_include=['SCHEMA']),
                call().json(),
            ]
        )

    @patch("ops_utils.tdr_utils.tdr_table_utils.sys.exit")
    def test_run_new_table_exists_in_dataset_needs_updating(self, mock_sys_exit):
        """Tests the 'run' method when the new table to ingest ALREADY EXISTS in the dataset AND
        NEEDS to be updated"""

        self.mock_tdr_instance.get_dataset_info.return_value = MagicMock(
            status_code=200,
            json=MagicMock(
                return_value={
                    'id': 'fake-dataset-id',
                    'name': 'fake-dataset-name',
                    'schema': {
                        'tables': [
                            {
                                'name': 'sample',
                                'columns': [
                                    # Missing the "sex" column from the EXISTING dataset
                                    {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                                    {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                                ],
                                'primaryKey': ['column_a'],
                                'partitionMode': 'none',
                                'datePartitionOptions': None,
                                'intPartitionOptions': None,
                                'rowCount': None
                            }
                        ],
                        'relationships': [],
                        'assets': []
                    },
                }
            )
        )

        # Run the method
        self.tdr_table_util.run()

        # Assertions
        self.mock_tdr_instance.get_dataset_info.assert_any_call(
            dataset_id=self.dataset_id, info_to_include=["SCHEMA"]
        )

        # Assert sys.exit(1) was called due to schema mismatch and no ignore flag
        mock_sys_exit.assert_called_once_with(1)

    def test_compare_table_matching_schemas(self):
        expected_schema = {
            'name': 'sample',
            'columns': [
                {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False}
            ]
        }

        existing_schema = [
            {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False}
        ]

        # Call the method
        columns_to_update = self.tdr_table_util._compare_table(
            reference_dataset_table=expected_schema,
            target_dataset_table=existing_schema,
            table_name="sample"
        )
        # Assertions
        self.assertEqual(columns_to_update, [])

    def test_compare_table_mis_matched_schemas(self):
        expected_schema = {
            'name': 'sample',
            'columns': [
                {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'participant', 'required': True, 'datatype': 'string', 'array_of': False}
            ]
        }

        existing_schema = [
            {'name': 'sample_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'participant_id', 'required': True, 'datatype': 'string', 'array_of': False},
            {'name': 'sex', 'required': True, 'datatype': 'string', 'array_of': False}
        ]

        # Call the method
        columns_to_update = self.tdr_table_util._compare_table(
            reference_dataset_table=expected_schema,
            target_dataset_table=existing_schema,
            table_name="sample"
        )
        # Assertions
        self.assertEqual(
            columns_to_update, [{'name': 'participant', 'required': True, 'datatype': 'string', 'array_of': False, "action": "add"}]
        )


class TestMatchSchemas(TestCase):
    """Tests for the MatchSchemas class"""

    def setUp(self):
        # Create mock TDR instance
        self.mock_tdr = MagicMock()

        # Define test data for original dataset
        self.orig_dataset_info = {
            "name": "original_dataset",
            "schema": {
                "tables": [
                    {
                        "name": "table_a",
                        "columns": [
                            {"name": "id", "datatype": "string", "mode": "required"},
                            {"name": "value", "datatype": "string", "mode": "nullable"}
                        ]
                    },
                    {
                        "name": "table_b",
                        "columns": [
                            {"name": "id", "datatype": "string", "mode": "required"},
                            {"name": "count", "datatype": "integer", "mode": "nullable"}
                        ]
                    }
                ]
            }
        }

        # Define test data for destination dataset
        self.dest_dataset_info = {
            "name": "destination_dataset",
            "schema": {
                "tables": [
                    {
                        "name": "table_a",
                        "columns": [
                            {"name": "id", "datatype": "string", "mode": "required"},
                            {"name": "value", "datatype": "string", "mode": "nullable"}
                        ]
                    }
                ]
            }
        }

        self.dest_dataset_id = "dest-dataset-123"

        # Create MatchSchemas instance
        self.match_schemas = MatchSchemas(
            orig_dataset_info=self.orig_dataset_info,
            dest_dataset_info=self.dest_dataset_info,
            dest_dataset_id=self.dest_dataset_id,
            tdr=self.mock_tdr
        )

    def test_init(self):
        """Test initialization of MatchSchemas"""
        self.assertEqual(self.match_schemas.orig_dataset_info, self.orig_dataset_info)
        self.assertEqual(self.match_schemas.dest_dataset_info, self.dest_dataset_info)
        self.assertEqual(self.match_schemas.dest_dataset_id, self.dest_dataset_id)
        self.assertEqual(self.match_schemas.tdr, self.mock_tdr)

    def test_run_adds_missing_tables(self):
        """Test that the run method adds tables that exist in the original dataset but not in the destination"""
        # Run the matching process
        self.match_schemas.run()

        # Verify that update_dataset_schema was called with the correct parameters
        self.mock_tdr.update_dataset_schema.assert_called_once()

        # Get the arguments from the call
        args, kwargs = self.mock_tdr.update_dataset_schema.call_args

        # Check dataset_id
        self.assertEqual(kwargs['dataset_id'], self.dest_dataset_id)

        # Check that the tables_to_add contains table_b
        self.assertEqual(len(kwargs['tables_to_add']), 1)
        self.assertEqual(kwargs['tables_to_add'][0]['name'], 'table_b')

        # Check that the update note is set
        self.assertTrue('update_note' in kwargs)

    def test_run_no_missing_tables(self):
        """Test that the run method doesn't update anything when all tables already exist"""
        # Modify destination dataset to include all tables from the original dataset
        self.dest_dataset_info = {
            "name": "destination_dataset",
            "schema": {
                "tables": [
                    {
                        "name": "table_a",
                        "columns": [
                            {"name": "id", "datatype": "string", "mode": "required"},
                            {"name": "value", "datatype": "string", "mode": "nullable"}
                        ]
                    },
                    {
                        "name": "table_b",
                        "columns": [
                            {"name": "id", "datatype": "string", "mode": "required"},
                            {"name": "count", "datatype": "integer", "mode": "nullable"}
                        ]
                    }
                ]
            }
        }

        # Create a new MatchSchemas instance with the updated destination dataset
        match_schemas = MatchSchemas(
            orig_dataset_info=self.orig_dataset_info,
            dest_dataset_info=self.dest_dataset_info,
            dest_dataset_id=self.dest_dataset_id,
            tdr=self.mock_tdr
        )

        # Run the matching process
        match_schemas.run()

        # Verify that update_dataset_schema was not called
        self.mock_tdr.update_dataset_schema.assert_not_called()

    def test_run_multiple_missing_tables(self):
        """Test that the run method adds multiple missing tables"""
        # Add another table to the original dataset
        self.orig_dataset_info["schema"]["tables"].append({
            "name": "table_c",
            "columns": [
                {"name": "id", "datatype": "string", "mode": "required"},
                {"name": "description", "datatype": "string", "mode": "nullable"}
            ]
        })

        # Run the matching process
        self.match_schemas.run()

        # Verify that update_dataset_schema was called with the correct parameters
        self.mock_tdr.update_dataset_schema.assert_called_once()

        # Get the arguments from the call
        args, kwargs = self.mock_tdr.update_dataset_schema.call_args

        # Check that the tables_to_add contains both missing tables
        self.assertEqual(len(kwargs['tables_to_add']), 2)
        table_names = [table['name'] for table in kwargs['tables_to_add']]
        self.assertIn('table_b', table_names)
        self.assertIn('table_c', table_names)
