import responses
from unittest.mock import MagicMock, mock_open, patch
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.terra_utils.terra_util import Terra, TerraGroups, TerraWorkspace
from ops_utils.terra_utils.terra_workflow_configs import WorkflowConfigs
mock_token = MagicMock()
request_util = RunRequest(token=mock_token)


class TestTerraWorkspaceUtils:
    workspace = TerraWorkspace(workspace_name="test_workspace",billing_project="test_billing_project", request_util=request_util)

    @responses.activate
    def test_get_workspace(self):
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace.yaml")
        workspace_info = self.workspace.get_workspace_info()
        assert workspace_info

    @responses.activate
    def test_get_workspace_bucket(self):
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace.yaml")
        workspace_bucket = self.workspace.get_workspace_bucket()
        assert workspace_bucket == "gs-bucket-id"

    @responses.activate
    def test_get_workspace_entity_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_entity_info.yaml")
        entify_info = self.workspace.get_workspace_entity_info()
        assert entify_info['file_metadata']['idName'] == "file_metadata_id"


    @responses.activate
    def test_get_workspace_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_acl_info.yaml")
        workspace_acl = self.workspace.get_workspace_acl()
        assert workspace_acl

    @responses.activate
    def test_get_workspace_metrics(self):
        responses._add_from_file(file_path="ops_utils/tests/data/workspace_metrics.yaml")
        workspace_metrics = self.workspace.get_gcp_workspace_metrics(entity_type='file_metadata')
        assert workspace_metrics

    @responses.activate
    def test_get_workspace_workflows(self):
        responses._add_from_file(file_path="ops_utils/tests/data/get_workspace_workflows.yaml")
        workflows = self.workspace.get_workspace_workflows()
        assert workflows

    @responses.activate
    def test_check_workspace_public(self):
        responses._add_from_file(file_path="ops_utils/tests/data/check_workspace_public.yaml")
        workspace_public = self.workspace.check_workspace_public()
        assert not workspace_public 


    @responses.activate
    def test_create_workspace(self):
        responses._add_from_file(file_path="ops_utils/tests/data/create_workspace.yaml")
        new_workspace_metadata = self.workspace.create_workspace()
        assert new_workspace_metadata


    @responses.activate
    def test_update_workspace_attributes(self):
        responses._add_from_file(file_path="ops_utils/tests/data/update_workspace_attributes.yaml")
        attributes = [{"op": "AddUpdateAttribute","attributeName": "dataset_id","addUpdateAttribute": 'ex-dataset-guid'}]
        workspace_update = self.workspace.update_workspace_attributes(attributes=attributes)
        assert workspace_update == None


    @responses.activate
    def test_update_user_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/update_acl.yaml")
        update_acl = self.workspace.update_user_acl(email='test-account@integration-project.iam.gserviceaccount.com', access_level='READER')
        assert update_acl 

    @responses.activate
    def test_update_multiple_user_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/update_multiple_acl.yaml")
        acl_list = [ { "email": "test@broadinstitute.org", "accessLevel": "READER", "canShare": False, "canCompute": False, }, { "email": "test-account@integration-project.iam.gserviceaccount.com", "accessLevel": "READER", "canShare": False, "canCompute": False, } ]
        update_multiple_acl = self.workspace.update_multiple_users_acl(acl_list=acl_list, invite_users_not_found=False)
        assert update_multiple_acl

    @responses.activate
    def test_upload_metadata_metadata(self):
        responses._add_from_file(file_path="ops_utils/tests/data/put_library_metadata.yaml")
        patch('__main__.open', mock_open(read_data="entity:sample_id\tsample_alias\nRP-123_ABC\tABC"))
        upload_metadata_res = self.workspace.upload_metadata_to_workspace_table("sample.tsv")
        assert upload_metadata_res