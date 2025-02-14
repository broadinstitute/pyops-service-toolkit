Module ops_utils.thread_pool_executor_util
==========================================

Classes
-------

`MultiThreadedJobs()`
:   

    ### Methods

    `execute_with_retries(self, function: Callable, job_args_list: list[typing.Any], max_retries: int) ‑> Any`
    :   Execute a function with retries.
        
        Args:
            function (Callable): The function to execute.
            job_args_list (list[Any]): The list of arguments to pass to the function.
            max_retries (int): The maximum number of retries.
        
        Returns:
            Any: The result of the function if it executes successfully, None otherwise.

    `run_multi_threaded_job(self, workers: int, function: Callable, list_of_jobs_args_list: list[typing.Any], collect_output: bool = False, max_retries: int = 3, fail_on_error: bool = True, verbose: bool = False, jobs_complete_for_logging: int = 500) ‑> list[typing.Any] | None`
    :   Run jobs in parallel and allow for retries. Optionally collect outputs of the jobs.
        
        Args:
            workers (int): The number of worker threads.
            function (Callable): The function to execute.
            list_of_jobs_args_list (list[Any]): The list of job arguments.
            collect_output (bool, optional): Whether to collect and return job outputs. Defaults to False.
            max_retries (int, optional): The maximum number of retries. Defaults to 3.
            fail_on_error (bool, optional): Whether to fail on error. Defaults to True.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            jobs_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 250.
        
        Returns:
            Optional[list[Any]]: A list of job results if `collect_output` is True, otherwise None.