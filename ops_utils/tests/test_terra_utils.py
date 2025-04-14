import responses
from unittest.mock import MagicMock
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.terra_utils.terra_util import Terra, TerraGroups, TerraWorkspace

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)


class TestTerraWorkspaceUtils:
    workspace = TerraWorkspace(workspace_name="test_workspace",billing_project="test_billing_project", request_util=request_util)

    @responses.activate
    def test_get_workspace(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace")
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace.yaml")
        workspace_info = self.workspace.get_workspace_info()
        assert workspace_info