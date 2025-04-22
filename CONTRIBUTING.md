# Contributing to pyops-service-toolkit

Thank you for your interest in contributing to this repository! This document outlines the process for making contributions, testing changes, and submitting pull requests.

## What is the purpose of pyops-service-toolkit?
The purpose of this repository is to be a central location where all "utility" code lives. There is currently
code that interacts with a variety of services, including Terra, TDR, GCP, and Azure. There are also some utility
functions that assist with interacting with BigQuery (reading and writing) and some utilities that don't necessarily
interact with external tooling - such as CSV utilities that help in reading and writing from tabular data. These
functions aim at creating a standard way in which we do "regular" tasks - such as creating a new Terra workspace,
for example.

## Contributing
Since this is a shared codebase that is imported by many downstream repositories and tools, extra caution is
required when making changes, _especially_ breaking changes.

### Adding New Features
If there is functionality that you believe should be shared that is not currently available, please add it! If
the functionality is interacting with an external service that we already have code for, please add the new function(s)
to the appropriate utility file. For example, if creating new Terra functionality, it should be added to
[terra_util.py](ops_utils/terra_utils/terra_util.py). If adding functionality that interacts with a service that we
do not already have support for (or just does not belong in a category that already exists), add a new file under
[ops_utils](ops_utils). With your submitted PR, be sure to update the [VERSION.txt](VERSION.txt) appropriately by
incrementing the version and adding release notes. See [README.txt](README.md#versioning) for more information
about incrementing versions correctly.

### Fixing Bugs
If possible, try to address bug fixes in a backwards compatible way.

### Breaking Changes
Is possible, it's best to avoid breaking changes. Prioritize trying to making changes in a backwards-compatible
manner. If this isn't possible, however, please make it abundantly clear in your PR that changes are breaking.
Increment the [VERSION.txt](VERSION.txt) appropriately - see [README.txt](README.md#versioning) for more information
about incrementing versions correctly.

## Testing Changes
Once you're ready to test your changes, you can follow these general steps before putting your changes up for review:

1. Make your changes and push to your remote branch.
2. Find the latest commit hash from your branch.
3. Install that specific hash version by using the following syntax:
```bash
pip install git+https://github.com/broadinstitute/pyops-service-toolkit.git@{COMMIT_HASH}#egg=pyops-service-toolkit
```
4. Test your changes with your branch's version of `pyops-service-toolkit` installed. Make changes as needed and
   push to your remote branch. If changes are made, you'll need to first uninstall using `pip uninstall
   pyops-service-toolkit`. And then re-install with your NEW commit hash again (change the syntax from step 3 to
   have the latest commit hash to re-install the latest changes).
5. Once your changes are working as expected, you can put your changes up for review.

## Submitting PRs
When submitting a PR, try to add as much detail to the description as possible that will help the reviewer in
evaluating the change. If there is a relevant ticket, please link it. If there are screenshots, links (to workspaces,
datasets, etc.), or logging that will help the review, please include those too. Tag one of the Operations team
members for review. At least one review is required to merge changes into `main`.
