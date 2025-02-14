from . import gcp_utils, azure_utils, terra_utils, tdr_utils, requests_utils, token_util


def comma_separated_list(value: str) -> list:
    """Return a list of values from a comma-separated string. Can be used as type in argparse."""
    return value.split(",")
