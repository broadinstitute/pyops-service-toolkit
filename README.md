# Ops Util Module Package
This repository contains shared Python utilities. There are utilities that help with interactions with Terra, TDR,
BigQuery, and GCP to name a few. There are also additional utilities that do not interact with external services,
such as utilities that assist in interacting with tabular data. The purpose of this repository is to provide a
central place to all utilities to live, so that code can stay DRY and consistent across other repositories that need
the same types of functionality.

## Installing Module Package
This will automatically install the latest release version.
``` sh
pip3 install git+https://github.com/broadinstitute/pyops-service-toolkit.git#egg=pyops-service-toolkit
```

## Releases and Installing a Specific Version

### Versioning

With each PR, a GitHub action will run to ensure that the [VERSION.txt](VERSION.txt) file has been updated. The
contents of the `VERSION.txt` file are used both for tagging the release _and_ adding release notes. When updating
this file, be sure to update both the version AND the release notes that should be used.

* Update the version within the `VERSION.txt` file according to what has been changed.
  * Given the versioning standard outlined as MAJOR.MINOR.PATCH, update your version according to what has been updated
  as part of your PR. Increment MAJOR versioning when adding breaking changes, increment MINOR versioning when
  adding functionality in a backwards-compatible manner, and increment PATCH versioning when making
  backwards-compatible bug fixes. For more information on versioning, see [here](https://semver.org/).
* Update the release notes with a short description of what has been changed as part of your PR


### Releases 
With each merge to `main`, a GitHub action will automatically run to create a new release and tag it using the 
version indicated in the [VERSION.txt](VERSION.txt) file. All releases and their tags can be found [here](https://github.com/broadinstitute/pyops-service-toolkit/releases).

### Installing a specific version 
In downstream repositories or tools that are using this module, **it is highly encouraged that a specific version is 
pinned in the `requirements.txt` file.** This is recommended to prevent any breaking changes that are introduced in 
this repository from breaking your downstream code. A specific version can be pinned or installed using the following syntax: 

```bash
git+https://github.com/broadinstitute/pyops-service-toolkit.git@{VERSION_TAG}#egg=pyops-service-toolkit
```

So for example, if you wanted to install version `v1.1.0`, you would pin the version in your `requirements.txt` file 
like this: 
```bash
git+https://github.com/broadinstitute/pyops-service-toolkit.git@v1.1.0#egg=pyops-service-toolkit
```

Or installed using pip:
```bash
pip install git+https://github.com/broadinstitute/pyops-service-toolkit.git@v1.1.0#egg=pyops-service-toolkit
```

## Example Usage
Once you've installed the package, you can structure import statements like this:
``` python
# TDR api utils
from ops_utils.tdr_utils.tdr_api_utils import TDR
```

You could then run something like: 
```
print(TDR.TDR_LINK)
```
which should return `https://data.terra.bio/api/repository/v1`

---

If you're interested in contributing to this repository, see [CONTRIBUTING.md](CONTRIBUTING.md).