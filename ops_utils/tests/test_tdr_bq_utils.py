from ops_utils.tdr_utils.tdr_bq_utils import GetTdrAssetInfo, TdrBq
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
import os
import pytest
import responses
from unittest.mock import MagicMock, patch


def setup_tdr_client():
    mock_token = MagicMock()
    request_util = RunRequest(token=mock_token)
    tdr_client = TDR(request_util)
    return tdr_client

@pytest.fixture()
def gcloud_auth_test_setup():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "test_creds.json"
    mock_credentials = MagicMock()
    mock_credentials.with_quota_project.return_value = mock_credentials
    mock_credentials.universe_domain = "googleapis.com"
    
    with (
        patch("google.cloud.secretmanager.SecretManagerServiceClient", autospec=True) as mock_secret_manager,
        patch("google.auth._default.load_credentials_from_file", return_value=(mock_credentials, 'project_id'), autospec=True)
    ):
        mock_secret_manager.return_value.access_secret_version.return_value.payload.data = b'some_api_key'
        yield mock_credentials


@pytest.mark.usefixtures("gcloud_auth_test_setup")
class TestTdrBqUtils():
    tdr_client = setup_tdr_client()

    @responses.activate
    def test_get_dataset_asset_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_bq_util/get_dataset_assets.yaml")
        tdr_assets_by_dataset = GetTdrAssetInfo(tdr=self.tdr_client, dataset_id="dataset_guid").run()
        assert tdr_assets_by_dataset['bq_project'] == 'datarepo-id'

    @responses.activate
    def test_get_snapshot_asset_info(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_bq_util/get_snapshot_assets.yaml")
        tdr_assets_by_snapshot = GetTdrAssetInfo(tdr=self.tdr_client, snapshot_id="snapshot_guid").run()        
        assert tdr_assets_by_snapshot['bq_schema'] == 'Full_View_Snapshot_of_ops_integration_test_dataset_1745959108339'

    @responses.activate
    def test_check_dataset_permissions(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_bq_util/check_dataset_permissions.yaml")
        check_permissions = TdrBq(project_id='project_id', bq_schema='bq_schema').check_permissions_for_dataset(raise_on_other_failure=False)
        assert check_permissions    

    @responses.activate
    def test_get_table_data(self):
        responses._add_from_file(file_path="ops_utils/tests/data/tdr_bq_util/get_tdr_table_content.yaml")
        table_content = TdrBq(project_id='project_id', bq_schema='test_dataset').get_tdr_table_contents(table_name='tmp_test_table', exclude_datarepo_id=False, to_dataframe=False)
        assert len(table_content) == 6