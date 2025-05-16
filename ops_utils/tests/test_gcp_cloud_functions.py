import unittest
from unittest.mock import patch, MagicMock
from ops_utils.gcp_cloud_functions import GCPCloudFunctionCaller


class TestGCPCloudFunctionCaller(unittest.TestCase):
    """Test suite for GCPCloudFunctionCaller class."""

    @patch("ops_utils.gcp_cloud_functions.build")
    @patch("ops_utils.gcp_cloud_functions.default")
    def setUp(self, mock_default, mock_build):
        """Set up the test case with mocked dependencies."""
        # Mock credentials and project
        mock_default.return_value = (MagicMock(), "test-project")
        self.mock_service = MagicMock()
        mock_build.return_value = self.mock_service

        # Initialize the GCPCloudFunctionCaller with mocked dependencies
        self.cloud_function_caller = GCPCloudFunctionCaller()

    def test_call_function_success(self):
        """Test call_function with a successful response."""
        # Mock the response from the API
        mock_request = MagicMock()
        mock_request.execute.return_value = {"result": "success"}
        self.mock_service.projects().locations().functions().call.return_value = mock_request

        # Call the method
        response = self.cloud_function_caller.call_function(
            function_name="test-function",
            data={"key": "value"}
        )

        # Assertions
        self.assertEqual(response, {"result": "success"})
        self.mock_service.projects().locations().functions().call.assert_called_once_with(
            name="projects/test-project/locations/us-central1/functions/test-function",
            body={"data": '{"key": "value"}'}
        )

    def test_call_function_failure(self):
        """Test call_function with an exception."""
        # Mock the API to raise an exception
        self.mock_service.projects().locations().functions().call.side_effect = Exception("API error")

        # Call the method and assert it raises an exception
        with self.assertRaises(Exception) as context:
            self.cloud_function_caller.call_function(
                function_name="test-function",
                data={"key": "value"}
            )

        self.assertIn("API error", str(context.exception))
        self.mock_service.projects().locations().functions().call.assert_called_once_with(
            name="projects/test-project/locations/us-central1/functions/test-function",
            body={"data": '{"key": "value"}'}
        )