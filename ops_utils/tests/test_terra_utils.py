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

    @responses.activate
    def test_get_workspace_bucket(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace")
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace.yaml")
        workspace_bucket = self.workspace.get_workspace_bucket()
        assert workspace_bucket == "gs-bucket-id"

    @responses.activate
    def test_get_workspace_entity_info(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/entities")
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_entity_info.yaml")
        entify_info = self.workspace.get_workspace_entity_info()
        assert entify_info['idName'] == "file_metadata_id"


    @responses.activate
    def test_get_workspace_acl(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/acl")
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_acl_info.yaml")
        workspace_acl = self.workspace.get_workspace_acl()
        assert workspace_acl

    @responses.activate
    def test_get_workspace_metrics(self):
        responses.patch("https://api.firecloud.org/api/workspaces/dsp-ops-gp-development-terra/js_test/entityQuery")
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_metrics.yaml")
        workspace_metrics = self.workspace.get_gcp_workspace_metrics(entity_type='file_metadata')
        assert workspace_metrics

    @responses.activate
    def test_get_workspace_workflows(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/methodconfigs")
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace_workflows.yaml")
        workflows = self.workspace.get_workspace_workflows()
        assert workflows

    @responses.activate
    def test_check_workspace_public(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace")
        responses.patch("https://sam.dsde-prod.broadinstitute.org/api/resources/v2/workspace/test-workspace-guid/policies/reader/public")
        responses._add_from_file(file_path="ops_utils/tests/data/check_workspace_public.yaml")
        workspace_public = self.workspace.check_workspace_public()
        assert not workspace_public 


    @responses.activate
    def test_create_workspace(self):
        responses.patch("https://api.firecloud.org/api/workspaces")
        responses._add_from_file(file_path="ops_utils/tests/data/create_workspace.yaml")
        new_workspace_metadata = self.workspace.check_workspace_public()
        assert new_workspace_metadata


    @responses.activate
    def test_update_workspace_attributes(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/updateAttributes")
        responses._add_from_file(file_path="ops_utils/tests/data/update_workspace_attributes.yaml")
        attributes = [{"op": "AddUpdateAttribute","attributeName": "dataset_id","addUpdateAttribute": 'ex-dataset-guid'}]
        updated_workspace_metadata = self.workspace.update_workspace_attributes(attributes=attributes)
        assert updated_workspace_metadata


    @responses.activate
    def test_update_user_acl(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/acl")
        responses._add_from_file(file_path="ops_utils/tests/data/update_workspace_attributes.yaml")
        update_acl = self.workspace.update_user_acl(email='test-account@integration-project.iam.gserviceaccount.com', access_level='reader')
        assert update_acl

    @responses.activate
    def test_update_multiple_user_acl(self):
        responses.patch("https://api.firecloud.org/api/workspaces/test_billing_project/test_workspace/acl")
        responses._add_from_file(file_path="ops_utils/tests/data/update_multiple_acl.yaml")
        acl_list = [ { "email": "test@broadinstitute.org", "accessLevel": "READER", "canShare": False, "canCompute": False, }, { "email": "test-account@integration-project.iam.gserviceaccount.com", "accessLevel": "READER", "canShare": False, "canCompute": False, } ]
        update_multiple_acl = self.workspace.update_multiple_users_acl(acl_list=acl_list, invite_users_not_found=False)
        assert update_multiple_acl