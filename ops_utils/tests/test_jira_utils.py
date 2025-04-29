import os
from ops_utils.jira_util import Jira, BROAD_INSTITUTE_SERVER
import responses
from unittest.mock import MagicMock, mock_open, patch
from google.auth import credentials
import pytest


def setup_jira_client_mock():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "test_creds.json"

    MOCK_CREDENTIALS = MagicMock(spec=credentials.CredentialsWithQuotaProject)
    MOCK_CREDENTIALS.with_quota_project.return_value = MOCK_CREDENTIALS
    MOCK_CREDENTIALS.universe_domain = "googleapis.com"
    
    with (
        patch("google.cloud.secretmanager.SecretManagerServiceClient", autospec=True) as mock_secret_manager,
        patch("google.auth._default.load_credentials_from_file", return_value=(MOCK_CREDENTIALS, 'operations-portal-427515'), autospec=True)
    ):
        mock_secret_manager.return_value.access_secret_version.return_value.payload.data = b'some_api_key'
        jira_client = Jira(server=BROAD_INSTITUTE_SERVER, gcp_project_id='ops-team-metrics', jira_api_key_secret_name='jira_api_key')
        return jira_client
    

class TestJiraUtils:

    jira_client = setup_jira_client_mock()

    @responses.activate
    def test_jira_update_ticket_fields(self):
        responses._add_from_file(file_path="ops_utils/tests/data/jira_util/update_ticket_fields.yaml")
        self.jira_client.update_ticket_fields(issue_key='POD-2674', field_update_dict={'description': "test description",'priority': {"name": 'Low'}})

    @responses.activate
    def test_jira_add_comment(self):
        responses._add_from_file(file_path="ops_utils/tests/data/jira_util/add_comment_to_ticket.yaml")
        self.jira_client.add_comment(issue_key="POD-2674", comment="test comment")

    @responses.activate
    def test_jira_transition_ticket(self):
        responses._add_from_file(file_path="ops_utils/tests/data/jira_util/transition_ticket.yaml")
        self.jira_client.transition_ticket(issue_key='POD-2674', transition_id=41, )

    @responses.activate
    def test_jira_get_issues_by_criteria(self):
        responses._add_from_file(file_path="ops_utils/tests/data/jira_util/get_issues_by_criteria.yaml")
        criteria="project = 'Pipeline Operations Development' AND sprint = 'POD Sprint 48' AND status = Done"
        issues = self.jira_client.get_issues_by_criteria(criteria)
        assert issues
