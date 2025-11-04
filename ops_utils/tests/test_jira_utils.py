import os
import unittest
from unittest.mock import patch, MagicMock
from ops_utils.jira_util import JiraUtil


class TestJiraUtil(unittest.TestCase):

    def setUp(self):
        self.server = "https://broadinstitute.atlassian.net/"
        self.gcp_project_id = "test-project"
        self.secret_name = "test-secret"

        self.mock_jira = MagicMock()
        self.util = JiraUtil.__new__(JiraUtil)
        self.util.jira_connection = self.mock_jira

    @patch("ops_utils.jira_util.secretmanager.SecretManagerServiceClient")
    @patch("ops_utils.jira_util.Jira")
    def test_connect_to_jira_with_token_file(self, mock_jira, mock_secret_manager):
        with patch("builtins.open", unittest.mock.mock_open(read_data="fake-token")):
            with patch("os.path.expanduser", return_value="/fake/.jira_api_key"):
                util = JiraUtil(self.server, self.gcp_project_id, self.secret_name)

        mock_jira.assert_called_once_with(
            url=self.server,
            username=f"{os.getenv('USER')}@broadinstitute.org",
            password="fake-token"
        )
        self.assertIsNotNone(util.jira_connection)

    @patch("ops_utils.jira_util.secretmanager.SecretManagerServiceClient")
    @patch("ops_utils.jira_util.Jira")
    def test_connect_to_jira_with_secret_manager(self, mock_jira, mock_secret_manager):
        # make file not found, trigger Secret Manager path
        with patch("builtins.open", side_effect=FileNotFoundError):
            mock_client = MagicMock()
            mock_client.access_secret_version.return_value.payload.data.decode.return_value = "secret-token"
            mock_secret_manager.return_value = mock_client

            util = JiraUtil(self.server, self.gcp_project_id, self.secret_name)

        mock_jira.assert_called_once_with(
            url=self.server,
            username=f"{os.getenv('USER')}@broadinstitute.org",
            password="secret-token"
        )

    def test_update_ticket_fields(self):
        self.util.update_ticket_fields("ISSUE-1", {"field": "value"})
        self.mock_jira.issue_update.assert_called_once_with("ISSUE-1", {"field": "value"})

    def test_add_comment(self):
        self.util.add_comment("ISSUE-2", "test comment")
        self.mock_jira.issue_add_comment.assert_called_once_with("ISSUE-2", "test comment")

    def test_transition_ticket(self):
        self.util.transition_ticket("ISSUE-3", 123)
        self.mock_jira.set_issue_status_by_transition_id.assert_called_once_with("ISSUE-3", 123)

    def test_get_issues_by_criteria(self):
        self.mock_jira.post.return_value = {"issues": []}

        criteria = "project = TEST"
        fields = ["summary", "status"]
        result = self.util.get_issues_by_criteria(criteria, max_results=50, fields=fields, expand_info="changelog")

        expected_payload = {
            "jql": criteria,
            "maxResults": 50,
            "fields": fields,
            "expand": "changelog"
        }

        self.mock_jira.post.assert_called_once_with("rest/api/3/search/jql", data=expected_payload)
        self.assertEqual(result, {"issues": []})
