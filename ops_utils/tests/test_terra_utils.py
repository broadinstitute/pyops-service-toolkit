import re
import pytest
import unittest
import responses
from unittest.mock import MagicMock, mock_open, patch

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace, Terra, TerraGroups, RAWLS_LINK, SAM_LINK

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)


class TestTerraWorkspaceUtils:
    workspace = TerraWorkspace(
        workspace_name="test_workspace",
        billing_project="test_billing_project",
        request_util=request_util
    )

    @responses.activate
    def test_get_workspace(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/get_workspace.yaml")
        workspace_info = self.workspace.get_workspace_info().json()
        assert workspace_info

    @responses.activate
    def test_get_workspace_bucket(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/get_workspace.yaml")
        workspace_bucket = self.workspace.get_workspace_bucket()
        assert workspace_bucket == "gs-bucket-id"

    @responses.activate
    def test_get_workspace_entity_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/workspace_entity_info.yaml")
        entify_info = self.workspace.get_workspace_entity_info().json()
        assert entify_info['file_metadata']['idName'] == "file_metadata_id"

    @responses.activate
    def test_get_workspace_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/workspace_acl_info.yaml")
        workspace_acl = self.workspace.get_workspace_acl().json()
        assert workspace_acl

    @responses.activate
    def test_get_workspace_metrics(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/workspace_metrics.yaml")
        workspace_metrics = self.workspace.get_gcp_workspace_metrics(entity_type='file_metadata')
        assert workspace_metrics

    @responses.activate
    def test_get_workspace_workflows(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/get_workspace_workflows.yaml")
        workflows = self.workspace.get_workspace_workflows().json()
        assert workflows

    @responses.activate
    def test_check_workspace_public(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/check_workspace_public.yaml")
        workspace_public = self.workspace.check_workspace_public().json()
        assert not workspace_public

    @responses.activate
    def test_create_workspace(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/create_workspace.yaml")
        new_workspace_metadata = self.workspace.create_workspace().json()
        assert new_workspace_metadata

    @responses.activate
    def test_update_workspace_attributes(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/update_workspace_attributes.yaml")
        attributes = [{"op": "AddUpdateAttribute", "attributeName": "dataset_id", "addUpdateAttribute": 'ex-dataset-guid'}]
        workspace_update = self.workspace.update_workspace_attributes(attributes=attributes).json()
        assert workspace_update

    @responses.activate
    def test_update_user_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/update_acl.yaml")
        update_acl = self.workspace.update_user_acl(
            email='test-account@integration-project.iam.gserviceaccount.com', access_level='READER').json()
        assert update_acl

    @responses.activate
    def test_update_multiple_user_acl(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/update_multiple_acl.yaml")
        acl_list = [{"email": "test@broadinstitute.org", "accessLevel": "READER", "canShare": False, "canCompute": False, },
                    {"email": "test-account@integration-project.iam.gserviceaccount.com", "accessLevel": "READER", "canShare": False, "canCompute": False, }]
        update_multiple_acl = self.workspace.update_multiple_users_acl(acl_list=acl_list, invite_users_not_found=False).json()
        assert update_multiple_acl

    @responses.activate
    def test_upload_metadata_metadata(self):
        responses._add_from_file(file_path="ops_utils/tests/data/terra_util/put_library_metadata.yaml")
        with patch('ops_utils.terra_util.open', mock_open(read_data="entity:sample_id\tsample_alias\nRP-123_ABC\tABC")):
            upload_metadata_res = self.workspace.upload_metadata_to_workspace_table("sample.tsv")
        assert upload_metadata_res


class TestTerra(unittest.TestCase):

    @patch("ops_utils.terra_util.RunRequest")
    def setUp(self, mock_request_util):
        self.mock_request_util = mock_request_util

        # Mock the RunRequest instance
        self.mock_request_instance = MagicMock()
        self.mock_request_instance.return_value = self.mock_request_util

        # Instantiate the Terra class with the mocked request_util
        self.terra = Terra(request_util=self.mock_request_instance)

    def test_fetch_accessible_workspaces(self):
        # Run the method
        self.terra.fetch_accessible_workspaces(fields=None)

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{RAWLS_LINK}/workspaces?",
            method="GET"
        )

    def test_get_pet_account_json(self):
        # Run the method
        self.terra.get_pet_account_json()

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/google/v1/user/petServiceAccount/key",
            method="GET"
        )


class TestTerraGroups(unittest.TestCase):

    @patch("ops_utils.terra_util.RunRequest")
    def setUp(self, mock_request_util):
        self.mock_request_util = mock_request_util

        # Mock the RunRequest instance
        self.mock_request_instance = MagicMock()
        self.mock_request_instance.return_value = self.mock_request_util

        # Instantiate the TerraGroups class with the mocked request_util
        self.terra_groups = TerraGroups(request_util=self.mock_request_instance)

    def test_check_role_accepted_role(self):
        res = self.terra_groups._check_role("member")
        self.assertIsNone(res)

    def test_check_role_non_accepted_role(self):
        with pytest.raises(ValueError, match=re.escape("Role must be one of ['member', 'admin']")):
            self.terra_groups._check_role("invalid_role")

    def test_remove_user_from_group(self):
        self.terra_groups.remove_user_from_group(group="fake-group", email="fake-email@fake.com", role="member")

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group/member/fake-email@fake.com",
            method="DELETE",
        )

    def test_create_group(self):
        self.terra_groups.create_group(group_name="fake-group", continue_if_exists=False)

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group",
            method="POST",
            accept_return_codes=[]
        )

    def test_create_group_already_exists(self):
        self.terra_groups.create_group(group_name="fake-group", continue_if_exists=True)

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group",
            method="POST",
            accept_return_codes=[409]
        )

    def test_delete_group(self):
        self.terra_groups.delete_group(group_name="fake-group")

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group",
            method="DELETE",
        )

    def test_add_user_to_group(self):
        self.terra_groups.add_user_to_group(
            group="fake-group", email="fake-email@fake.com", role="member", continue_if_exists=False
        )

        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group/member/fake-email@fake.com",
            method="PUT",
            accept_return_codes=[]
        )

    def test_add_user_to_group_already_exists(self):
        self.terra_groups.add_user_to_group(
            group="fake-group", email="fake-email@fake.com", role="member", continue_if_exists=True
        )
        self.mock_request_instance.run_request.assert_called_once_with(
            uri=f"{SAM_LINK}/groups/v1/fake-group/member/fake-email@fake.com",
            method="PUT",
            accept_return_codes=[409]
        )
