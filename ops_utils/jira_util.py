import os
import logging
from jira import JIRA
from google.cloud import secretmanager
from typing import Union, Optional

WORKBENCH_SERVER = "https://broadworkbench.atlassian.net/"
BROAD_INSTITUTE_SERVER = "https://broadinstitute.atlassian.net/"
DEFAULT_PROJECT_ID = "ops-team-metrics"


class Jira:
    JIRA_API_KEY_SECRET_NAME = "jira_api_key"

    def __init__(self, server: str) -> None:
        self.jira = self._connect_to_jira(server)

    def _connect_to_jira(self, server: str) -> JIRA:
        """Obtains credentials and establishes the Jira connection. User must have token stored in ~/.jira_api_key"""

        # Jira server and user details
        jira_server = server

        if os.getenv("RUN_IN_CLOUD") == "yes":
            jira_user = f'{os.getenv("JIRA_USER")}@broadinstitute.org'
        else:
            jira_user = f'{os.getenv("USER")}@broadinstitute.org'

        # This is necessary for when scripts are using this utility in a Cloud Function
        try:
            jira_api_token_file_path = os.path.expanduser("~/.jira_api_key")
            with open(jira_api_token_file_path, "r") as token_file:
                token = token_file.read().strip()
        except FileNotFoundError:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{DEFAULT_PROJECT_ID}/secrets/{self.JIRA_API_KEY_SECRET_NAME}/versions/latest"
            token = client.access_secret_version(name=name).payload.data.decode("UTF-8")

        return JIRA(server=jira_server, basic_auth=(jira_user, token))

    def update_ticket_fields(self, issue_key: str, field_update_dict: dict) -> None:
        """Update a Jira ticket with new field values"""
        issue = self.jira.issue(issue_key)
        issue.update(fields=field_update_dict)

    def add_comment(self, issue_key: str, comment: str) -> None:
        """Add a comment to a Jira ticket"""
        self.jira.add_comment(issue_key, comment)

    def transition_ticket(self, issue_key: str, transition_id: int) -> None:
        """Transition a Jira ticket to a new status"""
        self.jira.transition_issue(issue_key, transition_id)

    def get_issues_by_criteria(
            self,
            criteria: str,
            max_results: int = 200,
            fields: Optional[Union[list, str]] = None,
            expand_info: Optional[str] = None
    ) -> dict:
        logging.info(f"Getting issues by criteria: {criteria}")
        if fields:
            return self.jira.search_issues(criteria, fields=fields, maxResults=max_results, expand=expand_info)
        return self.jira.search_issues(criteria, maxResults=max_results, expand=expand_info)
