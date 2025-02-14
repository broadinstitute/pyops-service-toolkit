Module ops_utils.gcp_utils
==========================

Classes
-------

`GCPCloudFunctions(project: str | None = None)`
:   A class to interact with Google Cloud Storage (GCS) for various file operations.
    Authenticates using the default credentials and sets up the storage client.
    Does NOT use Token class for authentication.
    
    Initialize the GCPCloudFunctions class.
    Authenticates using the default credentials and sets up the storage client.

    ### Methods

    `copy_cloud_file(self, src_cloud_path: str, full_destination_path: str, verbose: bool = False) ‑> None`
    :   Copy a file from one GCS location to another.
        
        Args:
            src_cloud_path (str): The source GCS path.
            full_destination_path (str): The destination GCS path.
            verbose (bool, optional): Whether to log progress. Defaults to False.

    `copy_onprem_to_cloud(self, onprem_src_path: str, cloud_dest_path: str) ‑> None`
    :   Copy a file from an on-premises location to GCS.
        
        Args:
            onprem_src_path (str): The source path of the file on-premises.
            cloud_dest_path (str): The destination GCS path.
        
        Raises:
            Exception: If the source file does not exist or the user does not have permission to access it.

    `delete_cloud_file(self, full_cloud_path: str) ‑> None`
    :   Delete a file from GCS.
        
        Args:
            full_cloud_path (str): The GCS path of the file to delete.

    `delete_multiple_files(self, files_to_delete: list[str], workers: int = 5, max_retries: int = 3, verbose: bool = False, job_complete_for_logging: int = 500) ‑> None`
    :   Delete multiple cloud files in parallel using multi-threading.
        
        Args:
            files_to_delete (list[str]): List of GCS paths of the files to delete.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries. Defaults to 3.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            job_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.

    `get_file_contents_of_most_recent_blob_in_bucket(self, bucket_name: str) ‑> tuple[str, str] | None`
    :   Gets the most recent blob in the bucket. If the blob with the most recent timestamp doesn't have
        any logging besides the basic "storage_byte_hours" logging, will continue to look at the next most
        recent file until a log with useful information is encountered. This is useful when combing through
        GCP activity logs for Terra workspace buckets.
        
        Args:
            bucket_name (str): The GCS bucket name.
        Returns:
            Optional tuple of the blob found and the file contents from the blob

    `get_filesize(self, target_path: str) ‑> int`
    :   Get the size of a file in GCS.
        
        Args:
            target_path (str): The GCS path of the file.
        
        Returns:
            int: The size of the file in bytes.

    `get_object_md5(self, file_path: str, chunk_size: int = 256000, logging_bytes: int = 1000000000, returned_md5_format: str = 'hex') ‑> str`
    :   Calculate the MD5 checksum of a file in GCS.
        
        Args:
            file_path (str): The GCS path of the file.
            chunk_size (int, optional): The size of each chunk to read. Defaults to 256 KB.
            logging_bytes (int, optional): The number of bytes to read before logging progress. Defaults to 1 GB.
            returned_md5_format (str, optional): The format of the MD5 checksum to return. Defaults to "hex".
                Options are "hex" or "base64". hex = md5sum returns and base63 = gsutil stores.
        
        Returns:
            str: The MD5 checksum of the file.

    `list_bucket_contents(self, bucket_name: str, file_extensions_to_ignore: list[str] = [], file_strings_to_ignore: list[str] = [], file_extensions_to_include: list[str] = [], file_name_only: bool = False) ‑> list[dict]`
    :   List contents of a GCS bucket and return a list of dictionaries with file information.
        
        Args:
            bucket_name (str): The name of the GCS bucket. If includes gs://, it will be removed.
            file_extensions_to_ignore (list[str], optional): List of file extensions to ignore. Defaults to [].
            file_strings_to_ignore (list[str], optional): List of file name substrings to ignore. Defaults to [].
            file_extensions_to_include (list[str], optional): List of file extensions to include. Defaults to [].
            file_name_only (bool, optional): Whether to return only the file list and no extra info. Defaults to False.
        
        Returns:
            list[dict]: A list of dictionaries containing file information.

    `load_blob_from_full_path(self, full_path: str) ‑> Any`
    :   Load a GCS blob object from a full GCS path.
        
        Args:
            full_path (str): The full GCS path.
        
        Returns:
            Any: The GCS blob object.

    `loop_and_log_validation_files_multithreaded(self, files_to_validate: list[dict], log_difference: bool, workers: int = 5, max_retries: int = 3, job_complete_for_logging: int = 500) ‑> list[dict]`
    :   Validate if multiple cloud files are identical based on their MD5 hashes using multithreading.
        
        Args:
            files_to_validate (list[Dict]): List of dictionaries containing source and destination file paths.
            log_difference (bool): Whether to log differences if files are not identical. Set false if you are running
                                   this at the start of a copy/move operation to check if files are already copied.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries for all jobs. Defaults to 3.
            job_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.
        
        Returns:
            list[Dict]: List of dictionaries containing files that are not identical.

    `move_cloud_file(self, src_cloud_path: str, full_destination_path: str) ‑> None`
    :   Move a file from one GCS location to another.
        
        Args:
            src_cloud_path (str): The source GCS path.
            full_destination_path (str): The destination GCS path.

    `move_or_copy_multiple_files(self, files_to_move: list[dict], action: str, workers: int, max_retries: int, verbose: bool = False, jobs_complete_for_logging: int = 500) ‑> None`
    :   Move or copy multiple files in parallel.
        
        Args:
            files_to_move (list[dict]): List of dictionaries containing source and destination file paths.
            action (str): The action to perform ('move' or 'copy').
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            jobs_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.
        
        Raises:
            Exception: If the action is not 'move' or 'copy'.

    `multithread_copy_of_files_with_validation(self, files_to_copy: list[dict], workers: int, max_retries: int) ‑> None`
    :   Copy multiple files in parallel with validation.
        
        Args:
            files_to_copy (list[dict]): List of dictionaries containing source and destination file paths.
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.

    `read_file(self, cloud_path: str, encoding: str = 'utf-8') ‑> str`
    :   Read the content of a file from GCS.
        
        Args:
            cloud_path (str): The GCS path of the file to read.
            encoding (str, optional): The encoding to use. Defaults to 'utf-8'.
        
        Returns:
            bytes: The content of the file as bytes.

    `set_acl_group_owner(self, cloud_path: str, group_email: str) ‑> None`
    :   Set the file in the bucket to grant OWNER permission to a specific group.
        
        Args:
            cloud_path (str): The GCS path of the file.
            group_email (str): The email of the group to grant OWNER permission

    `set_acl_public_read(self, cloud_path: str) ‑> None`
    :   Set the file in the bucket to be publicly readable.
        
        Args:
            cloud_path (str): The GCS path of the file.

    `set_metadata_cache_control(self, cloud_path: str, cache_control: str) ‑> None`
    :   Set Cache-Control metadata for a file.
        
        Args:
            cloud_path (str): The GCS path of the file.
            cache_control (str): The Cache-Control metadata to set.

    `upload_blob(self, destination_path: str, source_file: str) ‑> None`
    :   Upload a file to GCS.
        
        Args:
            destination_path (str): The destination GCS path.
            source_file (str): The source file path.

    `validate_file_pair(self, source_file: str, full_destination_path: str) ‑> dict`
    :   Helper function to validate if source and destination files are identical.
        
        Args:
            source_file (str): The source file path.
            full_destination_path (str): The destination file path.
        
        Returns:
            dict: The file dictionary of the files with a boolean indicating if they are identical.

    `validate_files_are_same(self, src_cloud_path: str, dest_cloud_path: str) ‑> bool`
    :   Validate if two cloud files (source and destination) are identical based on their MD5 hashes.
        
        Args:
            src_cloud_path (str): The source GCS path.
            dest_cloud_path (str): The destination GCS path.
        
        Returns:
            bool: True if the files are identical, False otherwise.

    `write_to_gcs(self, cloud_path: str, content: str) ‑> None`
    :   Write content to a file in GCS.
        
        Args:
            cloud_path (str): The GCS path of the file to write.
            content (str): The content to write.