"""Utilities for working with Terra."""
import json
import logging
import re
from typing import Any, Optional
import requests
import time
import zipfile
import os

from . import deprecated
from .vars import GCP, APPLICATION_JSON
from .gcp_utils import GCPCloudFunctions
from .request_util import GET, POST, PATCH, PUT, DELETE, RunRequest

# Constants for Terra API links
TERRA_DEV_LINK = "https://firecloud-orchestration.dsde-dev.broadinstitute.org/api"
"""@private"""
TERRA_PROD_LINK = "https://api.firecloud.org/api"
"""@private"""
LEONARDO_LINK = "https://leonardo.dsde-prod.broadinstitute.org/api"
"""@private"""
WORKSPACE_LINK = "https://workspace.dsde-prod.broadinstitute.org/api/workspaces/v1"
"""@private"""
SAM_LINK = "https://sam.dsde-prod.broadinstitute.org/api"
"""@private"""
RAWLS_LINK = "https://rawls.dsde-prod.broadinstitute.org/api"
"""@private"""

MEMBER = "member"
ADMIN = "admin"


class Terra:
    """Class for generic Terra utilities."""

    def __init__(self, request_util: RunRequest, env: str = "prod"):
        """
        Initialize the Terra class.

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): An instance of a
            request utility class to handle HTTP requests.
        """
        self.request_util = request_util
        """@private"""

    def fetch_accessible_workspaces(self, fields: Optional[list[str]]) -> requests.Response:
        """
        Fetch the list of accessible workspaces.

        **Args:**
        - fields (list[str], optional): A list of fields to include in the response. If None, all fields are included.

        **Returns:**
        - requests.Response: The response from the request.
        """
        fields_str = "fields=" + ",".join(fields) if fields else ""
        url = f'{RAWLS_LINK}/workspaces?{fields_str}'
        return self.request_util.run_request(
            uri=url,
            method=GET
        )

    def get_pet_account_json(self) -> requests.Response:
        """
        Get the service account JSON.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{SAM_LINK}/google/v1/user/petServiceAccount/key"
        return self.request_util.run_request(
            uri=url,
            method=GET
        )


class TerraGroups:
    """A class to manage Terra groups and their memberships."""

    GROUP_MEMBERSHIP_OPTIONS = [MEMBER, ADMIN]
    """@private"""
    CONFLICT_STATUS_CODE = 409
    """@private"""

    def __init__(self, request_util: RunRequest):
        """
        Initialize the TerraGroups class.

        **Args:**
        - request_util (`ops_utils.request_util.RunRequest`): An instance of a request
         utility class to handle HTTP requests.
        """
        self.request_util = request_util
        """@private"""

    def _check_role(self, role: str) -> None:
        """
        Check if the role is valid.

        Args:
            role (str): The role to check.

        Raises:
            ValueError: If the role is not one of the allowed options.
        """
        if role not in self.GROUP_MEMBERSHIP_OPTIONS:
            raise ValueError(f"Role must be one of {self.GROUP_MEMBERSHIP_OPTIONS}")

    def remove_user_from_group(self, group: str, email: str, role: str) -> requests.Response:
        """
        Remove a user from a group.

        **Args:**
        - group (str): The name of the group.
        - email (str): The email of the user to remove.
        - role (str): The role of the user in the group
            (must be one of `ops_utils.terra_utils.MEMBER` or `ops_utils.terra_utils.ADMIN`).

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{SAM_LINK}/groups/v1/{group}/{role}/{email}"
        self._check_role(role)
        logging.info(f"Removing {email} from group {group}")
        return self.request_util.run_request(
            uri=url,
            method=DELETE
        )

    def create_group(self, group_name: str, continue_if_exists: bool = False) -> requests.Response:
        """
        Create a new group.

        **Args:**
        - group_name (str): The name of the group to create.
        - continue_if_exists (bool, optional): Whether to continue if the group already exists. Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{SAM_LINK}/groups/v1/{group_name}"
        accept_return_codes = [self.CONFLICT_STATUS_CODE] if continue_if_exists else []
        response = self.request_util.run_request(
            uri=url,
            method=POST,
            accept_return_codes=accept_return_codes
        )
        if continue_if_exists and response.status_code == self.CONFLICT_STATUS_CODE:
            logging.info(f"Group {group_name} already exists. Continuing.")
            return response
        else:
            logging.info(f"Created group {group_name}")
            return response

    def delete_group(self, group_name: str) -> requests.Response:
        """
        Delete a group.

        **Args:**
        - group_name (str): The name of the group to delete.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{SAM_LINK}/groups/v1/{group_name}"
        logging.info(f"Deleting group {group_name}")
        return self.request_util.run_request(
            uri=url,
            method=DELETE
        )

    def add_user_to_group(
            self, group: str, email: str, role: str, continue_if_exists: bool = False
    ) -> requests.Response:
        """
        Add a user to a group.

        **Args:**
        - group (str): The name of the group.
        - email (str): The email of the user to add.
        - role (str): The role of the user in the group
            (must be one of `ops_utils.terra_utils.MEMBER` or `ops_utils.terra_utils.ADMIN`).
        - continue_if_exists (bool, optional): Whether to continue if the user is already in the group.
                Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{SAM_LINK}/groups/v1/{group}/{role}/{email}"
        self._check_role(role)
        accept_return_codes = [self.CONFLICT_STATUS_CODE] if continue_if_exists else []
        logging.info(f"Adding {email} to group {group} as {role}")
        return self.request_util.run_request(
            uri=url,
            method=PUT,
            accept_return_codes=accept_return_codes
        )


class TerraWorkspace:
    """Terra workspace class to manage workspaces and their attributes."""

    CONFLICT_STATUS_CODE = 409
    """@private"""

    def __init__(self, billing_project: str, workspace_name: str, request_util: RunRequest, env: str = "prod"):
        """
        Initialize the TerraWorkspace class.

        **Args:**
        - billing_project (str): The billing project associated with the workspace.
        - workspace_name (str): The name of the workspace.
        - request_util (`ops_utils.request_util.RunRequest`): An instance of a
            request utility class to handle HTTP requests.
        """
        self.billing_project = billing_project
        """@private"""
        self.workspace_name = workspace_name
        """@private"""
        self.workspace_id = None
        """@private"""
        self.resource_id = None
        """@private"""
        self.storage_container = None
        """@private"""
        self.bucket = None
        """@private"""
        self.wds_url = None
        """@private"""
        self.account_url: Optional[str] = None
        """@private"""
        self.request_util = request_util
        """@private"""
        if env.lower() == "dev":
            self.terra_link = TERRA_DEV_LINK
            """@private"""
        elif env.lower() == "prod":
            self.terra_link = TERRA_PROD_LINK
            """@private"""
        else:
            raise ValueError(f"Invalid environment: {env}. Must be 'dev' or 'prod'.")

    def __repr__(self) -> str:
        """
        Return a string representation of the TerraWorkspace instance.

        Returns:
            str: The string representation of the TerraWorkspace instance.
        """
        return f"{self.billing_project}/{self.workspace_name}"

    def _yield_all_entity_metrics(self, entity: str, total_entities_per_page: int = 40000) -> Any:
        """
        Yield all entity metrics from the workspace.

        Args:
            entity (str): The type of entity to query.
            total_entities_per_page (int, optional): The number of entities per page. Defaults to 40000.

        Yields:
            Any: The JSON response containing entity metrics.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/entityQuery/{entity}?pageSize={total_entities_per_page}"  # noqa: E501
        response = self.request_util.run_request(
            uri=url,
            method=GET,
            content_type=APPLICATION_JSON
        )
        raw_text = response.text
        first_page_json = json.loads(
            raw_text,
            parse_float=lambda x: int(float(x)) if float(x).is_integer() else float(x)
        )
        yield first_page_json
        total_pages = first_page_json["resultMetadata"]["filteredPageCount"]
        logging.info(
            f"Looping through {total_pages} pages of data")

        for page in range(2, total_pages + 1):
            logging.info(f"Getting page {page} of {total_pages}")
            next_page = self.request_util.run_request(
                uri=url,
                method=GET,
                content_type=APPLICATION_JSON,
                params={"page": page}
            )
            raw_text = next_page.text
            page_json = json.loads(
                raw_text,
                parse_float=lambda x: int(float(x)) if float(x).is_integer() else float(x)
            )
            yield page_json

    @staticmethod
    def validate_terra_headers_for_tdr_conversion(table_name: str, headers: list[str]) -> None:
        """Check that all headers follow the standards required by TDR.

        **Args:**
        - table_name (str): The name of the Terra table.
        - headers (list[str]): The headers of the Terra table to validate.

        **Raises:**
        - ValueError if any headers are considered invalid by TDR standards
        """
        tdr_header_allowed_pattern = "^[a-zA-Z][_a-zA-Z0-9]*$"
        tdr_max_header_length = 63

        headers_containing_too_many_characters = []
        headers_contain_invalid_characters = []

        for header in headers:
            if len(header) > tdr_max_header_length:
                headers_containing_too_many_characters.append(header)
            if not re.match(tdr_header_allowed_pattern, header):
                headers_contain_invalid_characters.append(header)

        base_error_message = """In order to proceed, please update the problematic header(s) in you Terra table,
        and then re-attempt the import once all problematic header(s) have been updated to follow TDR rules for
        header naming."""
        too_many_characters_error_message = f"""The following header(s) in table "{table_name}" contain too many
        characters: "{', '.join(headers_containing_too_many_characters)}". The max number of characters for a header
        allowed in TDR is {tdr_max_header_length}.\n"""
        invalid_characters_error_message = f"""The following header(s) in table "{table_name}" contain invalid
        characters: "{', '.join(headers_contain_invalid_characters)}". TDR headers must start with a letter, and must
        only contain numbers, letters, and underscore characters.\n"""

        error_to_report = ""
        if headers_containing_too_many_characters:
            error_to_report += too_many_characters_error_message
        if headers_contain_invalid_characters:
            error_to_report += invalid_characters_error_message
        if error_to_report:
            error_to_report += base_error_message
            raise ValueError(error_to_report)

    def get_workspace_info(self) -> requests.Response:
        """
        Get workspace information.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}"
        logging.info(
            f"Getting workspace info for {self.billing_project}/{self.workspace_name}")
        return self.request_util.run_request(uri=url, method=GET)

    def get_gcp_workspace_metrics(self, entity_type: str, remove_dicts: bool = False) -> list[dict]:
        """
        Get metrics for a specific entity type in the workspace (specifically for Terra on GCP).

        **Args:**
        - entity_type (str): The type of entity to get metrics for.
        - remove_dicts (bool, optional): Whether to remove dictionaries from the workspace metrics. Defaults to `False`.

        **Returns:**
        - list[dict]: A list of dictionaries containing entity metrics.
        """
        results = []
        logging.info(f"Getting {entity_type} metadata for {self.billing_project}/{self.workspace_name}")

        for page in self._yield_all_entity_metrics(entity=entity_type):
            results.extend(page["results"])

        # If remove_dicts is True, remove dictionaries from the workspace metrics
        if remove_dicts:
            for row in results:
                row['attributes'] = self._remove_dict_from_attributes(row['attributes'])
        return results

    def get_specific_entity_metrics(self, entity_type: str, entity_name: str) -> requests.Response:
        """
        Get specific entity metrics for a given entity type and name.

        **Args:**
        - entity_type (str): The type of entity to get metrics for.
        - entity_name (str): The name of the entity to get metrics for.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/entities/{entity_type}/{entity_name}"
        return self.request_util.run_request(uri=url, method=GET)

    def _remove_dict_from_attributes(self, attributes: dict) -> dict:
        """
        Remove dictionaries from the attributes.

        Args:
            attributes (dict): The attributes to remove dictionaries from.

        Returns:
            dict: The updated attributes with no dictionaries.
        """
        for key, value in attributes.items():
            attributes[key] = self._remove_dict_from_cell(value)
        return attributes

    def _remove_dict_from_cell(self, cell_value: Any) -> Any:
        """
        Remove a dictionary from a cell.

        Args:
            cell_value (Any): The dictionary to remove.

        Returns:
            Any: The updated cell with no dictionaries.
        """
        if isinstance(cell_value, dict):
            entity_name = cell_value.get("entityName")
            # If the cell value is a dictionary, check if it has an entityName key
            if entity_name:
                # If the cell value is a dictionary with an entityName key, return the entityName
                return entity_name
            entity_list = cell_value.get("items")
            if entity_list or entity_list == []:
                # If the cell value is a list of dictionaries, recursively call this function on each dictionary
                return [
                    self._remove_dict_from_cell(entity) for entity in entity_list
                ]
            return cell_value
        return cell_value

    def get_workspace_bucket(self) -> str:
        """
        Get the workspace bucket name. Does not include the `gs://` prefix.

        **Returns:**
        - str: The bucket name.
        """
        return self.get_workspace_info().json()["workspace"]["bucketName"]

    def get_workspace_entity_info(self, use_cache: bool = True) -> requests.Response:
        """
        Get workspace entity information.

        **Args:**
        - use_cache (bool, optional): Whether to use cache. Defaults to `True`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        use_cache = "true" if use_cache else "false"  # type: ignore[assignment]
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/entities?useCache={use_cache}"
        return self.request_util.run_request(uri=url, method=GET)

    def get_workspace_acl(self) -> requests.Response:
        """
        Get the workspace access control list (ACL).

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        return self.request_util.run_request(
            uri=url,
            method=GET
        )

    def update_user_acl(
            self,
            email: str,
            access_level: str,
            can_share: bool = False,
            can_compute: bool = False,
            invite_users_not_found: bool = False,
    ) -> requests.Response:
        """
        Update the access control list (ACL) for a user in the workspace.

        **Args:**
        - email (str): The email of the user.
        - access_level (str): The access level to grant to the user.
        - can_share (bool, optional): Whether the user can share the workspace. Defaults to `False`.
        - can_compute (bool, optional): Whether the user can compute in the workspace. Defaults to `False`.
        - invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to `False`

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/acl?" + \
              f"inviteUsersNotFound={str(invite_users_not_found).lower()}"
        payload = {
            "email": email,
            "accessLevel": access_level,
            "canShare": can_share,
            "canCompute": can_compute,
        }
        logging.info(
            f"Updating user {email} to {access_level} in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type=APPLICATION_JSON,
            data="[" + json.dumps(payload) + "]"
        )

        if response.json()["usersNotFound"] and not invite_users_not_found:
            # Will be a list of one user
            user_not_found = response.json()["usersNotFound"][0]
            raise Exception(
                f'The user {user_not_found["email"]} was not found and access was not updated'
            )
        return response

    @deprecated(
        """Firecloud functionality has been sunset. There is currently no support for adding library attributes in Terra."""  # noqa: E501
    )
    def put_metadata_for_library_dataset(self, library_metadata: dict, validate: bool = False) -> requests.Response:
        """
        Update the metadata for a library dataset.

        **Args:**
        - library_metadata (dict): The metadata to update.
        - validate (bool, optional): Whether to validate the metadata. Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        acl = f"{self.terra_link}/library/{self.billing_project}/{self.workspace_name}" + \
              f"/metadata?validate={str(validate).lower()}"
        return self.request_util.run_request(
            uri=acl,
            method=PUT,
            data=json.dumps(library_metadata)
        )

    def update_multiple_users_acl(
            self, acl_list: list[dict], invite_users_not_found: bool = False
    ) -> requests.Response:
        """
        Update the access control list (ACL) for multiple users in the workspace.

        **Args:**
        - acl_list (list[dict]): A list of dictionaries containing the ACL information for each user.
        - invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to `False`

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/acl?" + \
            f"inviteUsersNotFound={str(invite_users_not_found).lower()}"
        logging.info(
            f"Updating users in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type=APPLICATION_JSON,
            data=json.dumps(acl_list)
        )

        if response.json()["usersNotFound"] and not invite_users_not_found:
            # Will be a list of one user
            users_not_found = [u["email"] for u in response.json()["usersNotFound"]]
            raise Exception(
                f"The following users were not found and access was not updated: {users_not_found}"
            )
        return response

    def create_workspace(
            self,
            auth_domain: list[dict] = [],
            attributes: dict = {},
            continue_if_exists: bool = False,
    ) -> requests.Response:
        """
        Create a new workspace in Terra.

        **Args:**
        - auth_domain (list[dict], optional): A list of authorization domains. Should look
                like `[{"membersGroupName": "some_auth_domain"}]`. Defaults to an empty list.
        - attributes (dict, optional): A dictionary of attributes for the workspace. Defaults to an empty dictionary.
        - continue_if_exists (bool, optional): Whether to continue if the workspace already exists. Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        payload = {
            "namespace": self.billing_project,
            "name": self.workspace_name,
            "authorizationDomain": auth_domain,
            "attributes": attributes,
            "cloudPlatform": GCP
        }
        # If workspace already exists then continue if exists
        accept_return_codes = [self.CONFLICT_STATUS_CODE] if continue_if_exists else []
        logging.info(f"Creating workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=f"{self.terra_link}/workspaces",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload),
            accept_return_codes=accept_return_codes
        )
        if continue_if_exists and response.status_code == self.CONFLICT_STATUS_CODE:
            logging.info(f"Workspace {self.billing_project}/{self.workspace_name} already exists")
        return response

    def create_workspace_attributes_ingest_dict(self, workspace_attributes: Optional[dict] = None) -> list[dict]:
        """
        Create an ingest dictionary for workspace attributes.

        **Args:**
        - workspace_attributes (dict, optional): A dictionary of workspace attributes. Defaults to None.

        **Returns:**
        - list[dict]: A list of dictionaries containing the workspace attributes.
        """
        # If not provided then call API to get it
        workspace_attributes = (
            workspace_attributes if workspace_attributes
            else self.get_workspace_info().json()["workspace"]["attributes"]
        )

        ingest_dict = []
        for key, value in workspace_attributes.items():
            # If value is dict just use 'items' as value
            if isinstance(value, dict):
                value = value.get("items")
            # If value is list convert to comma separated string
            if isinstance(value, list):
                value = ", ".join(value)
            ingest_dict.append(
                {
                    "attribute": key,
                    "value": str(value) if value else None
                }
            )
        return ingest_dict

    def upload_metadata_to_workspace_table(self, entities_tsv: str) -> requests.Response:
        """
        Upload metadata to the workspace table.

        **Args:**
        - entities_tsv (str): The path to the TSV file containing the metadata to upload.

        **Returns:**
        - requests.Response: The response from the request.
        """
        endpoint = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/flexibleImportEntities"
        data = {"entities": open(entities_tsv, "rb")}
        return self.request_util.upload_file(
            uri=endpoint,
            data=data
        )

    def get_workspace_workflows(self) -> requests.Response:
        """
        Get the workflows for the workspace.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs?allRepos=true"
        return self.request_util.run_request(
            uri=uri,
            method=GET
        )

    def import_workflow(self, workflow_dict: dict, continue_if_exists: bool = False) -> requests.Response:
        """
        Import a workflow into the workspace.

        **Args:**
        - workflow_dict (dict): The dictionary containing the workflow information.
        - continue_if_exists (bool, optional): Whether to continue if the workflow
                already exists. Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        uri = f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs"
        workflow_json = json.dumps(workflow_dict)
        accept_return_codes = [self.CONFLICT_STATUS_CODE] if continue_if_exists else []
        return self.request_util.run_request(
            uri=uri,
            method=POST,
            data=workflow_json,
            content_type=APPLICATION_JSON,
            accept_return_codes=accept_return_codes
        )

    def delete_workspace(self) -> requests.Response:
        """
        Delete a Terra workspace.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}",
            method=DELETE
        )

    def update_workspace_attributes(self, attributes: list[dict]) -> requests.Response:
        """
        Update the attributes for the workspace.

        **Args:**
        - attributes (dict): The attributes to update.

        **Returns:**
        - requests.Response: The response from the request.
        """
        return self.request_util.run_request(
            uri=f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/updateAttributes",
            method=PATCH,
            data=json.dumps(attributes),
            content_type=APPLICATION_JSON
        )

    def leave_workspace(
            self, workspace_id: Optional[str] = None, ignore_direct_access_error: bool = False
    ) -> requests.Response:
        """
        Leave a workspace. If workspace ID not supplied, will look it up.

        **Args:**
        - workspace_id (str, optional): The workspace ID. Defaults to None.
        - ignore_direct_access_error (Optional[bool], optional): Whether to ignore direct access errors.
             Defaults to `False`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        if not workspace_id:
            workspace_info = self.get_workspace_info().json()
            workspace_id = workspace_info['workspace']['workspaceId']
        accepted_return_code = [403] if ignore_direct_access_error else []

        res = self.request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/workspace/{workspace_id}/leave",
            method=DELETE,
            accept_return_codes=accepted_return_code
        )
        if (res.status_code == 403
                and res.json()["message"] == "You can only leave a resource that you have direct access to."):
            logging.info(
                f"Did not remove user from workspace with id '{workspace_id}' as current user does not have direct"
                f"access to the workspace (they could be an owner on the billing project)"
            )
        return res

    def change_workspace_public_setting(self, public: bool) -> requests.Response:
        """
        Change a workspace's public setting.

        **Args:**
        - public (bool, optional): Whether the workspace should be public. Set to `True` to be made
         public, `False` otherwise.

        **Returns:**
        - requests.Response: The response from the request.
        """
        body = [
            {
                "settingType": "PubliclyReadable",
                "config": {
                    "enabled": public
                }
            }
        ]
        return self.request_util.run_request(
            uri=f"{RAWLS_LINK}/workspaces/v2/{self.billing_project}/{self.workspace_name}/settings",
            method=PUT,
            content_type=APPLICATION_JSON,
            data=json.dumps(body)
        )

    def check_workspace_public(self, bucket: Optional[str] = None) -> requests.Response:
        """
        Check if a workspace is public.

        **Args:**
        - bucket (str, optional): The bucket name (provided without the `gs://` prefix). Will look
        it up if not provided. Defaults to None.

        **Returns:**
        - requests.Response: The response from the request.
        """
        workspace_bucket = bucket if bucket else self.get_workspace_bucket()
        bucket_prefix_stripped = workspace_bucket.removeprefix("fc-secure-").removeprefix("fc-")
        return self.request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/workspace/{bucket_prefix_stripped}/policies/reader/public",
            method=GET
        )

    def delete_entity_table(self, entity_to_delete: str) -> requests.Response:
        """Delete an entire entity table from a Terra workspace.

        **Args:**
        - entity_to_delete (str): The name of the entity table to delete.

        **Returns:**
        - requests.Response: The response from the request.
        """
        response = self.request_util.run_request(
            uri=f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/entityTypes/{entity_to_delete}",
            method=DELETE
        )
        if response.status_code == 204:
            logging.info(
                f"Successfully deleted entity table: '{entity_to_delete}' from workspace: "
                f"'{self.billing_project}/{self.workspace_name}'"
            )
        else:
            logging.error(
                f"Encountered the following error while attempting to delete '{entity_to_delete}' "
                f"table: {response.text}"
            )
        return response

    def save_entity_table_version(self, entity_type: str, version_name: str) -> None:
        """Save an entity table version in a Terra workspace.

        **Args:**
        - entity_type (str): The name of the entity table to save a new version for
        - version_name (str): The name of the new version
        """
        # Get the workspace metrics
        workspace_metrics = self.get_gcp_workspace_metrics(entity_type=entity_type)
        file_name = f"{entity_type}.json"
        # Write the workspace metrics to a JSON file
        with open(file_name, "w") as json_file:
            json.dump(workspace_metrics, json_file)

        # Create a zip file with the same naming convention that Terra backend uses
        timestamp_ms = int(time.time() * 1000)
        zip_file_name = f"{entity_type}.v{timestamp_ms}.zip"
        with zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_name, arcname=f"json/{file_name}")

        # Upload the zip file to subdirectory within the workspace's bucket (where Terra expects it to live)
        workspace_info = self.get_workspace_info().json()
        path_to_upload_to = os.path.join(
            "gs://", workspace_info["workspace"]["bucketName"], ".data-table-versions", entity_type, zip_file_name
        )
        gcp_util = GCPCloudFunctions(project=workspace_info["workspace"]["googleProject"])
        # Attempt to get the currently active gcloud account. Default to the workspace creator if that fails
        try:
            active_account = gcp_util.get_active_gcloud_account()
        except Exception as e:
            active_account = workspace_info["workspace"]["createdBy"]
            logging.error(
                f"Encountered the following exception while attempting to get current GCP account: {e}. "
                f"Will set the owner of the new metadata version as the workspace creator instead."
            )
        gcp_util.upload_blob(
            source_file=zip_file_name,
            destination_path=path_to_upload_to,
            custom_metadata={
                "createdBy": active_account,
                "entityType": entity_type,
                "timestamp": timestamp_ms,
                "description": version_name,
            }
        )

    def add_user_comment_to_submission(self, submission_id: str, user_comment: str) -> requests.Response:
        """
        Add a user comment to a submission in Terra.

        **Args:**
        - submission_id (str): The ID of the submission to add a comment to.
        - user_comment (str): The comment to add to the submission.

        **Returns:**
        - requests.Response: The response from the request.
        """
        logging.info(f"Attempting to add user comment: '{user_comment}' to submission: '{submission_id}'")
        return self.request_util.run_request(
            uri=f"{RAWLS_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/submissions/{submission_id}",
            method=PATCH,
            content_type=APPLICATION_JSON,
            data=json.dumps({"userComment": user_comment}),
        )

    def initiate_submission(
            self,
            method_config_namespace: str,
            method_config_name: str,
            entity_type: str,
            entity_name: str,
            expression: str,
            user_comment: Optional[str],
            use_call_cache: bool = True
    ) -> requests.Response:
        """
        Initiate a submission within a Terra workspace.

        Note - the workflow being initiated MUST already be imported into the workspace.

        **Args:**
        - method_config_namespace (str): The namespace of the method configuration.
        - method_config_name (str): The name of the method configuration to use for the submission
        (i.e. the workflow name).
        - entity_type (str): The entity type to be used as input to the workflow (e.x. "sample", or "sample_set").
        - entity_name (str): The name of the entity to be used as input to the workflow (e.x. "sample_1", or
        "sample_set_1").
        - expression (str): The "expression" to use. For example, if the `entity_type` is `sample` and the workflow is
        launching one sample, this can be left as `this`. If the `entity_type` is `sample_set`, but one workflow should
        be launched PER SAMPLE, the expression should be `this.samples`.
        - user_comment (str, optional): The user comment to add to the submission.
        - use_call_cache (bool, optional): Whether to use the call caching. Defaults to `True`.

        **Returns:**
        - requests.Response: The response from the request.
        """
        payload = {
            "methodConfigurationNamespace": method_config_namespace,
            "methodConfigurationName": method_config_name,
            "entityType": entity_type,
            "entityName": entity_name,
            "expression": expression,
            "useCallCache": use_call_cache,
            "deleteIntermediateOutputFiles": False,
            "useReferenceDisks": False,
            "ignoreEmptyOutputs": False,
        }
        if user_comment:
            payload["userComment"] = user_comment

        return self.request_util.run_request(
            uri=f"{self.terra_link}/workspaces/{self.billing_project}/{self.workspace_name}/submissions",
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload),
        )
      
    def retry_failed_submission(self, submission_id: str) -> requests.Response:
        """
        Retry a failed submission in Terra.

        **Args:**
        - submission_id (str): The ID of the submission to retry.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{RAWLS_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/submissions/{submission_id}/retry"
        payload = {"retryType": "Failed"}
        logging.info(
            f"Retrying failed submission: '{submission_id}' in workspace {self.billing_project}/{self.workspace_name}"
        )
        return self.request_util.run_request(
            uri=url,
            method=POST,
            content_type=APPLICATION_JSON,
            data=json.dumps(payload)
        )

    def get_submission_status(self, submission_id: str) -> requests.Response:
        """
        Get the status of a submission in Terra.

        **Args:**
        - submission_id (str): The ID of the submission.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{RAWLS_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/submissions/{submission_id}"
        logging.info(
            f"Getting status for submission: '{submission_id}' in workspace {self.billing_project}/{self.workspace_name}"
        )
        return self.request_util.run_request(
            uri=url,
            method=GET
        )

    def get_workspace_submission_status(self) ->requests.Response:
        """
        Get the status of all submissions in a Terra workspace.

        **Returns:**
        - requests.Response: The response from the request.
        """
        url = f"{RAWLS_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/submissions"
        logging.info(
            f"Getting status for all submissions in workspace {self.billing_project}/{self.workspace_name}"
        )
        return self.request_util.run_request(
            uri=url,
            method=GET
        )

    def get_workspace_submission_stats(self, method_name: Optional[str] = None) -> tuple[int, list[str]]:
        """
        Get submission statistics for a Terra workspace, optionally filtered by method name.

        **Args:**
        - method_name (str, optional): The name of the method to filter statistics by. Defaults to None.

        **Returns:**
        - tuple[int, list[str]]: A tuple containing the total number of running and pending workflows,
          and a list of IDs for workflows that are still running.
        """
        submissions = self.get_workspace_submission_status().json()
        # If method_name is provided, filter submissions to only those with that method name
        running_submissions = [
            s
            for s in submissions
            if s["status"] not in ["Done", "Aborted"] and
               (s["methodConfigurationName"] == method_name if method_name else True)
        ]
        logging.info(
            f"{len(running_submissions)} running submissions in "
            f"{self.billing_project}/{self.workspace_name}"
            f" with method name '{method_name}'" if method_name else ""
        )
        total_running_and_pending_workflows = 0
        still_running_ids = []
        for submission in running_submissions:
            wf_status = submission["workflowStatuses"]
            running_and_queued_workflows = wf_status["Queued"] + wf_status["Running"] + wf_status['Submitted']
            total_running_and_pending_workflows += running_and_queued_workflows
            submission_detailed = self.get_submission_status(submission_id=submission["submissionId"]).json()
            for workflow in submission_detailed["workflows"]:
                if workflow["status"] in ["Running", "Submitted", "Queued"]:
                    entity_id = workflow["workflowEntity"]["entityName"]
                    still_running_ids.append(entity_id)
        return total_running_and_pending_workflows, still_running_ids


