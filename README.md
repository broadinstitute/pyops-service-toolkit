# Ops Util Module Package
This repository contains shared Python utilities. There are utilities that help with interactions with Terra, TDR, 
BigQuery, and GCP to name a few. There are also additional utilities that do not interact with external services, 
such as utilities that assist in interacting with tabular data. The purpose of this repository is to provide a 
central place to all utilities to live, so that code can stay DRY and consistent across other repositories that need 
the same types of functionality. 

## Installing Module Package
``` sh
pip3 install git+https://github.com/broadinstitute/ops_util_module.git#egg=ops_util_module
```

## Pinning or Installing a Specific Version
This needs to be filled out once we get versioned tags released.

## Example Usage
Once you've installed the package, you can structure import statements like this:  
``` python
# TDR api utils
from ops_utils.tdr_utils.tdr_api_utils import TDR
```
If that works successfully, you could then run something like: 
```
print(TDR.TDR_LINK)
``` 
which should return `https://data.terra.bio/api/repository/v1`

