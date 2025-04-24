[![Auto Release](https://github.com/broadinstitute/pyops-service-toolkit/actions/workflows/auto-release.yml/badge.svg)](https://github.com/broadinstitute/pyops-service-toolkit/actions/workflows/auto-release.yml)
[![Docs Site Build](https://github.com/broadinstitute/pyops-service-toolkit/actions/workflows/docs.yml/badge.svg)](https://github.com/broadinstitute/pyops-service-toolkit/actions/workflows/docs.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![codecov](https://codecov.io/gh/broadinstitute/pyops-service-toolkit/graph/badge.svg?token=J2YT04WO3C)](https://app.codecov.io/gh/broadinstitute/pyops-service-toolkit/tree/main)


# 🧰 pyops-service-toolkit
This repository contains shared Python utilities designed to streamline development across projects. It includes tools
for interacting with external services such as Terra, TDR, BigQuery, and Google Cloud Platform (GCP), as well as
utilities for working with local data, like helpers for manipulating tabular datasets. The goal of this repository is
to centralize these common tools, helping teams write DRY, consistent, and reusable code across different codebases.

### 🔗 [Explore our searchable documentation](https://broadinstitute.github.io/pyops-service-toolkit/ops_utils.html)


# 🚀 Getting Started
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


# 🔧 pre-commit
pre-commit is configured in this repository. Automatic checks (configured in [.pre-commit-config.yaml](.pre-commit-config.yaml))
will run with each commit. Each time you go to commit changes, pre-commit will point out failing checks, which should
be addressed before adding files to your branch. If there are checks that are failing that you don't think are valid or
shouldn't be fixed, they can be ignored individually by adding the following to the line of code that's failing
the check: `# type: ignore[assignment]`. Be sure to replace `assignment` with the actual check-type that's failing
(which is reported in the failure message).

To override the check and commit directly, run `git commit -m "YOU COMMIT MESSAGE" --no-verify`
The only time you may want to use the `--no-verify` flag is if your commit is WIP and you just need to commit
something for testing.

To add more hooks, browse available hooks [here](https://pre-commit.com/hooks.html)


---

# 🤝 Contributing
If you're interested in contributing to this repository, see [CONTRIBUTING.md](CONTRIBUTING.md).
