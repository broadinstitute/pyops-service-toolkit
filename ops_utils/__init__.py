r"""
# pyops-service-toolkit

Welcome to the documentation for
[pyops-service-toolkit](https://github.com/broadinstitute/pyops-service-toolkit).
Use the sidebar menu on the left to browse available submodules, or search our documentation
if you're looking for something specific! If there's something missing, or if you have
questions, reach out to the Operations team at `support@pipeline-ops.zendesk.com`.

"""

import warnings
import functools


# To avoid breaking changes and converting these to have underscores,
# we can instead add the @private annotation in the docstring
def comma_separated_list(value: str) -> list:
    """@private
    Return a list of values from a comma-separated string. Can be used as type in argparse.
    """
    return value.split(",")


def deprecated(reason: str):  # type: ignore[no-untyped-def]
    """
    @private
    Wrapper function to be used for deprecated functionality. Use the @deprecated
    decorator for a function and provide a reason. Anytime the function is called, a
    deprecation warning will be raised.
    """
    def decorator(func):  # type: ignore[no-untyped-def]
        @functools.wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
            warnings.warn(
                f"{func.__name__} is deprecated: {reason}",
                category=DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator
