import warnings

from ops_utils import comma_separated_list, deprecated



@deprecated("Use an alternative function.")
def deprecated_function(x):
    """A 'deprecated' function for testing purposes."""
    return x + 1

def test_comma_separated_list():
    res = comma_separated_list("foo,bar,baz")
    assert res == ["foo", "bar", "baz"]

def test_deprecated():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Ensure all warnings are captured

        result = deprecated_function(2)

        # Assert the function still works
        assert result == 3
        # Assert one warning was raised
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        # Check the warning message
        assert "deprecated_function is deprecated: Use an alternative function." == str(w[0].message)
