Module ops_utils.tdr_utils.tdr_job_utils
========================================

Classes
-------

`MonitorTDRJob(tdr: Any, job_id: str, check_interval: int, return_json: bool)`
:   A class to monitor the status of a TDR job until completion.
    
    Attributes:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
    
    Initialize the MonitorTDRJob class.
    
    Args:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
        return_json (bool): Whether to get and return the result of the job as json.

    ### Methods

    `run(self) ‑> dict | None`
    :   Monitor the job until completion.
        
        Returns:
            dict: The result of the job.

`SubmitAndMonitorMultipleJobs(tdr: Any, job_function: Callable, job_args_list: list[tuple], batch_size: int = 100, check_interval: int = 5, verbose: bool = False)`
:   Initialize the SubmitAndMonitorMultipleJobs class.
    
    Args:
        tdr (Any): An instance of the TDR class.
        job_function (Callable): The function to submit a job.
        job_args_list (list[tuple]): A list of tuples containing the arguments for each job.
        batch_size (int, optional): The number of jobs to process in each batch. Defaults to 100.
        check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 5.
        verbose (bool, optional): Whether to log detailed information about each job. Defaults to False.

    ### Methods

    `run(self) ‑> None`
    :   Run the process to submit and monitor multiple jobs in batches.
        
        Logs the progress and status of each batch and job.
        
        Returns:
            None