import warnings
import functools


def comma_separated_list(value: str) -> list:
    """Return a list of values from a comma-separated string. Can be used as type in argparse."""
    return value.split(",")


# A wrapper function to be used for deprecated functionality. Use the @deprecated
# decorator for a function and provide a reason. Anytime the function is called, a
# deprecation warning will be raised.
def deprecated(reason: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated: {reason}",
                category=DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator
