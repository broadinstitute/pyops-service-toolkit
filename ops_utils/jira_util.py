import os
import logging
from jira import JIRA
from google.cloud import secretmanager
from typing import Optional

WORKBENCH_SERVER = "https://broadworkbench.atlassian.net/"
"""The default Broad workbench server address"""
BROAD_INSTITUTE_SERVER = "https://broadinstitute.atlassian.net/"
"""The default Broad Jira server address"""
# TODO this should be made to be more general and something that can be configured by a user
DEFAULT_PROJECT_ID = "ops-team-metrics"
"""The default GCP project ID used to locate the Jira API key"""


class Jira:
    _JIRA_API_KEY_SECRET_NAME = "jira_api_key"

    def __init__(self, server: str) -> None:
        """
        Initializes the Jira instance using the provided server

        **Args:**
        - server (str): The server URL to connect to. For example: `https://broadinstitute.atlassian.net/`
        """

        self.jira = self._connect_to_jira(server)
        """@private"""

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
            name = f"projects/{DEFAULT_PROJECT_ID}/secrets/{self._JIRA_API_KEY_SECRET_NAME}/versions/latest"
            token = client.access_secret_version(name=name).payload.data.decode("UTF-8")

        return JIRA(server=jira_server, basic_auth=(jira_user, token))

    def update_ticket_fields(self, issue_key: str, field_update_dict: dict) -> None:
        """
        Update a Jira ticket with new field values

        **Args:**
        - issue_key (str): The issue key to update
        - field_update_dict (dict): The field values to update. Formatted with the field ID as the key,
        and the updated value as the key's value.
        """
        issue = self.jira.issue(issue_key)
        issue.update(fields=field_update_dict)

    def add_comment(self, issue_key: str, comment: str) -> None:
        """
        Add a comment to a Jira ticket

        **Args:**
        - issue_key (str): The issue key to update
        - comment (str): The comment to add
        """
        self.jira.add_comment(issue_key, comment)

    def transition_ticket(self, issue_key: str, transition_id: int) -> None:
        """
        Transition a Jira ticket to a new status

        **Args:**
        - issue_key (str): The issue key to update
        - transition_id (int): The status ID to transition the issue to
        """
        self.jira.transition_issue(issue_key, transition_id)

    def get_issues_by_criteria(
            self,
            criteria: str,
            max_results: int = 200,
            fields: Optional[list[str]] = None,
            expand_info: Optional[str] = None
    ) -> list[dict]:
        """
        Get all issues by defining specific criteria

        **Args:**
        - criteria (str): The criteria to search for. This should be formatted in a supported JIRA search filter
        (i.e. `project = '{project}' AND sprint = '{sprint_name}' AND status = {status}`)
        - max_results (int): The maximum number of results to return. Defaults to 200.
        - fields (list[string], optional): The fields to include in the return for each
        ticket (i.e. `["summary", "status", "assignee"]`)

        **Returns:**
        - list[dict]: The list of issues matching the criteria
        """
        logging.info(f"Getting issues by criteria: {criteria}")
        if fields:
            return self.jira.search_issues(criteria, fields=fields, maxResults=max_results, expand=expand_info)
        return self.jira.search_issues(criteria, maxResults=max_results, expand=expand_info)
