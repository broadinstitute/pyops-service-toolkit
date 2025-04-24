import pytest
import unittest
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import Forbidden

from ops_utils.bq_utils import BigQueryUtil


class TestBigQueryUtils(unittest.TestCase):

    @patch("ops_utils.bq_utils.bigquery.Client")
    def setUp(self, mock_bigquery_client):
        self.mock_client_instance = MagicMock()
        mock_bigquery_client.return_value = self.mock_client_instance

        self.project_id = "fake_project_id"
        self.table_id = "fake_table_id"
        self.bq_util = BigQueryUtil(project_id=self.project_id)
        self.query = "SELECT * FROM `my_dataset.my_table`"
        self.fake_query_result = [{"columnA": "foo", "columnB": "bar"}, {"columnA": "baz", "columnB": "qux"}]
        self.sample_data = [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]

    def test_upload_data_to_table(self):
        mock_table = MagicMock()
        mock_table.num_rows = 0
        self.mock_client_instance.get_table.return_value = mock_table

        # Mock insert_rows_json to return no errors
        self.mock_client_instance.insert_rows_json.return_value = []

        # Run the method
        self.bq_util.upload_data_to_table(table_id=self.table_id, rows=self.sample_data)

        # Assertions
        self.mock_client_instance.get_table.assert_called_with(self.table_id)
        self.mock_client_instance.insert_rows_json.assert_called_once_with(self.table_id, self.sample_data)
        self.assertEqual(self.mock_client_instance.get_table.call_count, 2)  # Once before insert, once after

    @patch("ops_utils.bq_utils.BigQueryUtil._delete_existing_records")
    def test_upload_data_to_table_delete_existing_data(self, mock_delete_existing_records):
        mock_table = MagicMock()
        mock_table.num_rows = 0
        self.mock_client_instance.get_table.return_value = mock_table

        mock_delete_existing_records.return_value = None

        # Mock insert_rows_json to return no errors
        self.mock_client_instance.insert_rows_json.return_value = []

        # Run the method
        self.bq_util.upload_data_to_table(table_id=self.table_id, rows=self.sample_data, delete_existing_data=True)

        # Assertions
        self.mock_client_instance.get_table.assert_called_with(self.table_id)
        self.mock_client_instance.insert_rows_json.assert_called_once_with(self.table_id, self.sample_data)
        mock_delete_existing_records.assert_called_once_with(self.table_id)
        self.assertEqual(self.mock_client_instance.get_table.call_count, 2)  # Once before insert, once after


    def test_query_table(self):
        # Mock the BQ client "query" call
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = self.fake_query_result

        self.mock_client_instance.query.return_value = mock_query_job

        # Run the method
        result = self.bq_util.query_table(query=self.query)

        # Assert that the BQ client runs query with the defined select statement
        self.mock_client_instance.query.assert_called_once_with(self.query)
        # Assert that BQ client calls the "result" method
        mock_query_job.result.assert_called_once()
        # Asserts that the expected result is returned
        self.assertEqual(result, self.fake_query_result)

    @patch("ops_utils.bq_utils.BigQueryUtil._check_permissions")
    def test_check_permissions_to_project(self, mock_check_perms):
        # Mock the return value for the _check_permissions method
        mock_check_perms.return_value = True

        # Run the method
        res = self.bq_util.check_permissions_to_project()

        # Assertions
        mock_check_perms.assert_called_once_with("SELECT 1", True)
        self.assertTrue(res)

    @patch("ops_utils.bq_utils.BigQueryUtil._check_permissions")
    def test_check_permissions_for_query(self, mock_check_perms):
        # Mock the return value for the _check_permissions method
        mock_check_perms.return_value = False

        # Run the method
        res = self.bq_util.check_permissions_for_query(query=self.query)

        # Assertions
        mock_check_perms.assert_called_once_with(self.query, True)
        self.assertFalse(res)

    def test__check_permissions_valid_permissions(self):
        self.mock_client_instance.query.result.return_value = self.fake_query_result

        # Run the method
        res = self.bq_util._check_permissions(qry=self.query)
        # Assert that the permissions are valid
        self.assertTrue(res)

    def test__check_permissions_invalid_permissions(self):
        # Simulate Forbidden error when .result() is called
        self.mock_client_instance.query.return_value.result.side_effect = Forbidden("403 Permission Denied")

        # Run the method
        res = self.bq_util._check_permissions(qry=self.query)

        # Assert that the permissions are invalid
        self.assertFalse(res)

    def test__check_permissions_invalid_permissions_raise_on_other_failure(self):
        # Simulate Exception error when .result() is called
        self.mock_client_instance.query.return_value.result.side_effect = Exception("Some fake error")

        # Assert that the exception is raised when "raise_on_other_failure" is set to True
        with pytest.raises(Exception, match="Some fake error"):
            self.bq_util._check_permissions(qry=self.query, raise_on_other_failure=True)

    def test__check_permissions_invalid_permissions_not_raise_on_other_failure(self):
        # Simulate Exception error when .result() is called
        self.mock_client_instance.query.return_value.result.side_effect = Exception("Some fake error")

        res = self.bq_util._check_permissions(qry=self.query, raise_on_other_failure=False)
        # Assert that "False" is returned when an exception is encountered by "raise_on_other_failure" is set to False
        self.assertFalse(res)

    def test__delete_existing_records(self):
        # Run the _delete_existing_records method
        self.bq_util._delete_existing_records(table_id=self.table_id)

        # Create the "delete" query using the fake table ID
        fake_delete_query = f"DELETE FROM `{self.table_id}` WHERE TRUE"
        # Asert that the query was run with the expected select statement
        self.mock_client_instance.query.assert_called_with(fake_delete_query)
