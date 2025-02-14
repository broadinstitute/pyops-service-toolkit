Module ops_utils.azure_utils
============================

Classes
-------

`AzureBlobDetails(account_url: str, sas_token: str, container_name: str)`
:   

    ### Methods

    `download_blob(self, blob_name: str, dl_path: pathlib.Path)`
    :

    `get_blob_details(self, max_per_page: int = 500) ‑> list[dict]`
    :

`SasTokenUtil(token: str)`
:   

    ### Methods

    `seconds_until_token_expires(self) ‑> datetime.timedelta | None`
    :