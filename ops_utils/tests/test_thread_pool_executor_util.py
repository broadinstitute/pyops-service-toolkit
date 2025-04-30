from unittest import TestCase
from unittest.mock import MagicMock

from ops_utils.thread_pool_executor_util import MultiThreadedJobs



def always_succeeds(*args, **kwargs):
    """A test function that always succeeds"""
    return sum(args)

def always_fails(*args, **kwargs):
    """A test function that always fails"""
    raise ValueError("Permanent failure")


class TestMultiThreadedJobs(TestCase):
    def setUp(self):
        self.multithreaded = MultiThreadedJobs()

    def test_function_succeeds_first_try(self):
        mock_func = MagicMock(return_value="success")
        status, result = self.multithreaded.execute_with_retries(mock_func, job_args_list=["arg1"], max_retries=3)

        self.assertTrue(status)
        self.assertEqual(result, "success")
        mock_func.assert_called_once_with("arg1")

    def test_function_fails_then_succeeds(self):
        mock_func = MagicMock()
        mock_func.side_effect = [Exception("fail 1"), "success"]
        status, result = self.multithreaded.execute_with_retries(mock_func, job_args_list=[], max_retries=3)

        self.assertTrue(status)
        self.assertEqual(result, "success")
        self.assertEqual(mock_func.call_count, 2)

    def test_function_fails_all_retries(self):
        mock_func = MagicMock(side_effect=Exception("fail"))
        status, result = self.multithreaded.execute_with_retries(mock_func, job_args_list=[], max_retries=3)

        self.assertFalse(status)
        self.assertIsNone(result)
        self.assertEqual(mock_func.call_count, 3)


    def test_run_multi_threaded_job_collect_output_success(self):
        results = self.multithreaded.run_multi_threaded_job(
            workers=1,
            function=always_succeeds,
            list_of_jobs_args_list=[[1, 2], [3, 4]],
            collect_output=True
        )
        self.assertEqual(results, [3, 7])

    def test_run_multi_threaded_job_fails_then_raises(self):
        with self.assertRaises(Exception) as e:
            self.multithreaded.run_multi_threaded_job(
                workers=1,
                function=always_fails,
                list_of_jobs_args_list=[[1, 2]],
                collect_output=False,
                fail_on_error=True
            )
        self.assertEqual("1 jobs failed after retries.", str(e.exception))