Module ops_utils.terra_utils.terra_workflow_configs
===================================================

Classes
-------

`GetWorkflowNames()`
:   Initialize the GetWorkflowNames class.
    
    Loads the YAML file and extracts workflow names.

    ### Methods

    `get_workflow_names(self) ‑> list[str]`
    :   Get the list of workflow names.
        
        Returns:
            list: A list of workflow names.

`WorkflowConfigs(workflow_name: str, billing_project: str, terra_workspace_util: ops_utils.terra_utils.terra_util.TerraWorkspace, set_input_defaults: bool = False, extra_default_inputs: dict = {})`
:   Initialize the WorkflowConfigs class.
    
    Args:
        workflow_name (str): The name of the workflow to configure.
        billing_project (str): The billing project to use for the workflow.
        terra_workspace_util (TerraWorkspace): The TerraWorkspace utility object.
        set_input_defaults (bool): Whether to set the default input values for the workflow configuration.
        tdr_billing_profile (str): The TDR billing profile for workflow.
    
    Raises:
        ValueError: If the workflow name is not found in the YAML file.

    ### Methods

    `import_workflow(self, continue_if_exists: bool = False) ‑> int`
    :   Import the workflow into the Terra workspace.
        
        Args:
            continue_if_exists (bool, optional): Whether to continue if the workflow already exists. Defaults to False.
        
        Returns:
            int: The status code of the import operation.