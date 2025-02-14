Module ops_utils.terra_utils.terra_util
=======================================

Classes
-------

`Terra(request_util: Any)`
:   Initialize the Terra class.
    
    Args:
        request_util (Any): An instance of a request utility class to handle HTTP requests.

    ### Methods

    `fetch_accessible_workspaces(self, fields: list[str] | None) ‑> list[dict]`
    :

`TerraGroups(request_util: Any)`
:   A class to manage Terra groups and their memberships.
    
    Initialize the TerraGroups class.
    
    Args:
        request_util (Any): An instance of a request utility class to handle HTTP requests.

    ### Class variables

    `GROUP_MEMBERSHIP_OPTIONS`
    :

    ### Methods

    `add_user_to_group(self, group: str, email: str, role: str, continue_if_exists: bool = False) ‑> int`
    :   Add a user to a group.
        
        Args:
            group (str): The name of the group.
            email (str): The email of the user to add.
            role (str): The role of the user in the group.
            continue_if_exists (bool, optional): Whether to continue if the user is already in the group.
                Defaults to False.
        Returns:
            int: The response code

    `create_group(self, group_name: str, continue_if_exists: bool = False) ‑> int`
    :   Create a new group.
        
        Args:
            group_name (str): The name of the group to create.
            continue_if_exists (bool, optional): Whether to continue if the group already exists. Defaults to False.
        Returns:
            int: The response code

    `delete_group(self, group_name: str) ‑> int`
    :   Delete a group.
        
        Args:
            group_name (str): The name of the group to delete.
        Returns:
            int: The status code

    `remove_user_from_group(self, group: str, email: str, role: str) ‑> int`
    :   Remove a user from a group.
        
        Args:
            group (str): The name of the group.
            email (str): The email of the user to remove.
            role (str): The role of the user in the group.
        Returns:
            int: The response code

`TerraWorkspace(billing_project: str, workspace_name: str, request_util: Any)`
:   Initialize the TerraWorkspace class.
    
    Args:
        billing_project (str): The billing project associated with the workspace.
        workspace_name (str): The name of the workspace.
        request_util (Any): An instance of a request utility class to handle HTTP requests.

    ### Static methods

    `validate_terra_headers_for_tdr_conversion(table_name: str, headers: list[str]) ‑> None`
    :

    ### Methods

    `change_workspace_public_setting(self, public: bool) ‑> None`
    :   Make a workspace public.

    `check_workspace_public(self, bucket: str | None = None) ‑> bool`
    :   Check if a workspace is public.

    `create_workspace(self, auth_domain: list[dict] = [], attributes: dict = {}, continue_if_exists: bool = False, cloud_platform: str = 'gcp') ‑> dict | None`
    :   Create a new workspace in Terra.
        
        Args:
            auth_domain (list[dict], optional): A list of authorization domains. Should look
                like [{"membersGroupName": "some_auth_domain"}]. Defaults to an empty list.
            attributes (dict, optional): A dictionary of attributes for the workspace. Defaults to an empty dictionary.
            continue_if_exists (bool, optional): Whether to continue if the workspace already exists. Defaults to False.
            cloud_platform (str, optional): The cloud platform for the workspace. Defaults to GCP.
        
        Returns:
            dict: The response from the Terra API containing the workspace details.

    `create_workspace_attributes_ingest_dict(self, workspace_attributes: dict | None = None) ‑> list[dict]`
    :   Create an ingest dictionary for workspace attributes.
        
        Args:
            workspace_attributes (Optional[dict], optional): A dictionary of workspace attributes. Defaults to None.
        
        Returns:
            list[dict]: A list of dictionaries containing the workspace attributes.

    `delete_workspace(self) ‑> int`
    :   Delete a Terra workspace.
        
        Returns:
            int: The response status code

    `get_gcp_workspace_metrics(self, entity_type: str, remove_dicts: bool = False) ‑> list[dict]`
    :   Get metrics for a specific entity type in the workspace.
        
        Args:
            entity_type (str): The type of entity to get metrics for.
            remove_dicts (bool, optional): Whether to remove dictionaries from the workspace metrics. Defaults to False.
        
        Returns:
            list[dict]: A list of dictionaries containing entity metrics.

    `get_workspace_acl(self) ‑> dict`
    :   Get the workspace access control list (ACL).
        
        Returns:
            dict: The JSON response containing the workspace ACL.

    `get_workspace_bucket(self) ‑> str`
    :   Get the workspace bucket name. Does not include the gs:// prefix.
        
        Returns:
            str: The bucket name.

    `get_workspace_entity_info(self, use_cache: bool = True) ‑> dict`
    :   Get workspace entity information.
        
        Args:
            use_cache (bool, optional): Whether to use cache. Defaults to True.
        
        Returns:
            dict: The JSON response containing workspace entity information.

    `get_workspace_info(self) ‑> dict`
    :   Get workspace information.
        
        Returns:
            dict: The JSON response containing workspace information.

    `get_workspace_workflows(self) ‑> dict`
    :   Get the workflows for the workspace.
        
        Returns:
            dict: The JSON response containing the workspace workflows.

    `import_workflow(self, workflow_dict: dict, continue_if_exists: bool = False) ‑> int`
    :   Import a workflow into the workspace.
        
        Args:
            workflow_dict (dict): The dictionary containing the workflow information.
            continue_if_exists (bool, optional): Whether to continue if the workflow
                already exists. Defaults to False.
        
        Returns:
            int: The response status code

    `leave_workspace(self, workspace_id: str | None = None, ignore_direct_access_error: bool = False) ‑> None`
    :   Leave a workspace. If workspace ID not supplied will look it up
        
        Args:
            workspace_id (Optional[str], optional): The workspace ID. Defaults to None.
            ignore_direct_access_error (Optional[bool], optional): Whether to ignore direct access errors.
             Defaults to False.

    `put_metadata_for_library_dataset(self, library_metadata: dict, validate: bool = False) ‑> dict`
    :   Update the metadata for a library dataset.
        
        Args:
            library_metadata (dict): The metadata to update.
            validate (bool, optional): Whether to validate the metadata. Defaults to False.
        Returns:
            dict: The JSON response containing the updated library attributes.

    `retrieve_sas_token(self, sas_expiration_in_secs: int) ‑> str`
    :   Retrieve the SAS token for the workspace.
        
        Args:
            sas_expiration_in_secs (int): The expiration time for the SAS token in seconds.
        
        Returns:
            str: The SAS token.

    `set_azure_terra_variables(self) ‑> None`
    :   Get all needed variables and set them for the class.

    `set_workspace_id(self, workspace_info: dict) ‑> None`
    :   Set the workspace ID.
        
        Args:
            workspace_info (dict): The dictionary containing workspace information.

    `update_multiple_users_acl(self, acl_list: list[dict], invite_users_not_found: bool = False) ‑> dict`
    :   Update the access control list (ACL) for multiple users in the workspace.
        
        Args:
            acl_list (list[dict]): A list of dictionaries containing the ACL information for each user.
            invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to False
        
        Returns:
            dict: The JSON response containing the updated ACL.

    `update_user_acl(self, email: str, access_level: str, can_share: bool = False, can_compute: bool = False, invite_users_not_found: bool = False) ‑> dict`
    :   Update the access control list (ACL) for a user in the workspace.
        
        Args:
            email (str): The email of the user.
            access_level (str): The access level to grant to the user.
            can_share (bool, optional): Whether the user can share the workspace. Defaults to False.
            can_compute (bool, optional): Whether the user can compute in the workspace. Defaults to False.
            invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to False
        
        Returns:
            dict: The JSON response containing the updated ACL.

    `update_workspace_attributes(self, attributes: list[dict]) ‑> None`
    :   Update the attributes for the workspace.
        
        Args:
            attributes (dict): The attributes to update.
        
        Returns:
            int: The response status code

    `upload_metadata_to_workspace_table(self, entities_tsv: str) ‑> str`
    :   Upload metadata to the workspace table.
        
        Args:
            entities_tsv (str): The path to the TSV file containing the metadata.
        
        Returns:
            str: The response from the upload request.