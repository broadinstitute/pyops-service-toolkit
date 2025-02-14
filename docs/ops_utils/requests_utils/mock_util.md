Module ops_utils.requests_utils.mock_util
=========================================

Functions
---------

`activate_recorder() ‑> Any`
:   

`activate_responses() ‑> Any`
:   

`make_filename(func: Any) ‑> pathlib.Path`
:   

`mock_responses(activate: bool = False, update_results: bool = False) ‑> Any`
:   Decorator to record then mock requests made with the requests module.
    
    When update_results is True, will store requests to a yaml file. When it
    is false, it will retrieve the results, allowing to run tests offline.
    
    Usage:
        import requests
        from python.tests.utils.mock_responses import mock_responses
    
    
        class MyTestCase(TestCase):
            @mock_responses(update_results=settings.TESTS_UPDATE_STORED_RESULTS)
            def test_mytest(self):
                request.get("https://example.com)
                ...