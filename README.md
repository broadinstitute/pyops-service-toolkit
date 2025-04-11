# Ops Util Module Package
This repository contains shared Python utilities. There are utilities that help with interactions with Terra, TDR, 
BigQuery, and GCP to name a few. There are also additional utilities that do not interact with external services, 
such as utilities that assist in interacting with tabular data. The purpose of this repository is to provide a 
central place to all utilities to live, so that code can stay DRY and consistent across other repositories that need 
the same types of functionality. 

## Installing Module Package
This will automatically install the latest release version.
``` sh
pip3 install git+https://github.com/broadinstitute/ops_util_module.git#egg=ops_util_module
```

## Releases and Installing a Specific Version

### Versioning 
With each PR, a GitHub action will run to ensure that the [VERSION.txt](VERSION.txt) file has been updated. This 
version file is used as the tag for each new release. Update this version file according to what has been changed. 
Given the versioning standard outlined as MAJOR.MINOR.PATCH, update your version according to what has been updated 
as part of your PR. Increment MAJOR versioning when adding breaking changes, increment MINOR versioning when 
adding functionality in a backwards-compatible manner, and increment PATCH versioning when making 
backwards-compatible bug fixes. For more information on versioning, see [here](https://semver.org/).  

### Releases 
With each merge to `main`, a GitHub action will automatically run to create a new release and tag it using the 
version indicated in the [VERSION.txt](VERSION.txt) file. All releases can be found [here](https://github.com/broadinstitute/ops_util_module/releases).

### Installing a specific version 
In downstream repositories or tools that are using this module, you can pin or install specific versions using the 
following 
syntax: 
```bash
git+https://github.com/broadinstitute/ops_util_module.git@{VERSION}#egg=ops_util_module
```
So for example, if you wanted to install version `0.1.0`, you would pin the version in your `requirements.txt` file 
like this: 
```bash
git+https://github.com/broadinstitute/ops_util_module.git@v0.1.0#egg=ops_util_module
```


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

