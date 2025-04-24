import responses
from ops_utils.gcp_utils import GCPCloudFunctions
from unittest.mock import MagicMock, mock_open, patch



class TestGCPUtils:
    gcp_client = GCPCloudFunctions()
    
    @responses.activate
    def test_load_file(self):
        test_gcs_path = "gs://test_bucket/file001.bin"
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/load_blob.yaml")
        test_blob = self.gcp_client.load_blob_from_full_path(test_gcs_path)
        assert test_blob 

    @responses.activate
    def test_check_file_exists(self):
        test_gcs_path = "gs://test_bucket/file001.bin"
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/check_file_exists.yaml")
        test_blob = self.gcp_client.check_file_exists(test_gcs_path)
        assert test_blob

    @responses.activate
    def test_list_bucket(self):
        bucket_name = "test_bucket"
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/list_bucket.yaml")
        list_bucket = self.gcp_client.list_bucket_contents(bucket_name=bucket_name)
        assert len(list_bucket) == 20

    @responses.activate
    def test_copy_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/copy_file.yaml")
        self.gcp_client.copy_cloud_file(src_cloud_path="gs://test_src_path/file001.bin", full_destination_path="gs://dest_bucket/file001.bin")

    @responses.activate
    def test_delete_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/delete_file.yaml")
        self.gcp_client.delete_cloud_file(full_cloud_path="gs://test_bucket/file002.bin")
    
    @responses.activate
    def test_move_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/move_file.yaml")
        self.gcp_client.move_cloud_file(src_cloud_path='gs://test_bucket/file004.bin', full_destination_path='gs://test_bucket/file004_moved.bin')

    @responses.activate
    def test_get_filesize(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/get_filesize.yaml")
        filesize = self.gcp_client.get_filesize(target_path="gs://test_bucket/file001.bin")
        assert filesize == 21788

    @responses.activate
    def test_check_if_files_are_same(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/check_files_same.yaml")
        files_identical = self.gcp_client.validate_files_are_same(src_cloud_path="gs://test_bucket/file001.bin", dest_cloud_path="gs://test_bucket/file002.bin")
        assert not files_identical

    @responses.activate
    def test_read_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/read_file.yaml")
        data = self.gcp_client.read_file(cloud_path='gs://test_bucket/uploaded_test_file.txt')
        assert data == "test\n\ndata\n\nhere"

    @responses.activate
    def test_upload_blob(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.upload_blob()
        pass    

    @responses.activate
    def test_get_md5(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.get_object_md5()
        pass    

    @responses.activate
    def test_copy_to_cloud(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.copy_onprem_to_cloud()
        pass    

    @responses.activate
    def test_set_acl_public(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.set_acl_public_read()
        pass    

    def test_acl_group_owner(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.set_acl_group_owner()
        pass

    def test_metadata_cache_control(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.set_metadata_cache_control
        pass        


    def test_get_most_recent_blob_content(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.get_file_contents_of_most_recent_blob_in_bucket()
        pass        


    def test_write_to_gcp_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.write_to_gcp_file
        pass        

    def test_multithread_validation(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.loop_and_log_validation_files_multithreaded()
        pass

    def test_move_copy_files(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/")
        self.gcp_client.move_or_copy_multiple_files()
        pass    