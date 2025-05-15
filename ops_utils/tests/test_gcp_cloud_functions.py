from ops_utils.gcp_cloud_functions import GCPCloudFunctionCaller
from unittest.mock import MagicMock, patch
import pytest

@pytest.fixture
@patch("ops_utils.gcp_cloud_functions.default", return_value=(MagicMock(), "test-project"))
def cloud_function_caller(mock_default):
    return GCPCloudFunctionCaller(project="test-project")

@patch("ops_utils.gcp_cloud_functions.default")
@patch("ops_utils.gcp_cloud_functions.build")
@patch.object(GCPCloudFunctionCaller, "call_function", return_value={"result": "success"})
def test_calls_function_successfully(mock_call_function, mock_build, mock_default, cloud_function_caller):
    mock_default.return_value = (MagicMock(), "test-project")
    mock_service = MagicMock()
    mock_build.return_value = mock_service

    response = cloud_function_caller.call_function("test-function", {"key": "value"})

    assert response == {"result": "success"}
    mock_call_function.assert_called_once_with("test-function", {"key": "value"})

@patch("ops_utils.gcp_cloud_functions.default")
@patch("ops_utils.gcp_cloud_functions.build")
def test_uses_default_project_if_not_provided(mock_build, mock_default):
    mock_default.return_value = (MagicMock(), "default-project")
    mock_service = MagicMock()
    mock_request = MagicMock()
    mock_request.execute.return_value = {"result": "success"}
    mock_service.projects().locations().functions().call.return_value = mock_request
    mock_build.return_value = mock_service

    caller = GCPCloudFunctionCaller()
    response = caller.call_function("test-function", {"key": "value"})

    assert response == {"result": "success"}
    mock_service.projects().locations().functions().call.assert_called_once_with(
        name="projects/default-project/locations/us-central1/functions/test-function",
        body={"data": '{"key": "value"}'}
    )
