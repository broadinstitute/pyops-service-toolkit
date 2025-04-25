import responses
import sys
from ops_utils.gcp_utils import GCPCloudFunctions
from unittest.mock import MagicMock, mock_open, patch

sys.modules['google.auth.default'] = MagicMock()

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
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/upload_blob.yaml")
        with patch('google.cloud.storage.blob.open', mock_open(read_data=b"test data here")):
            self.gcp_client.upload_blob(destination_path='gs://test_bucket/uploaded_blob.txt', source_file='source_file.txt')

    @responses.activate
    def test_get_md5(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/get_blob_md5.yaml")
        md5 = self.gcp_client.get_object_md5(file_path='gs://test_bucket/uploaded_test_file.txt')
        assert md5 == "e7c8241f3451ef053f4854f8faa1cf71"

    @responses.activate
    def test_set_acl_public(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/set_blob_acl_public.yaml")
        self.gcp_client.set_acl_public_read(cloud_path="gs://test_bucket/dummy_file.txt")
    
    @responses.activate
    def test_acl_group_owner(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/set_acl_group_owner.yaml")
        self.gcp_client.set_acl_group_owner(cloud_path="gs://test_bucket/uploaded_blob.txt", group_email='test_group@firecloud.org')

    @responses.activate
    def test_metadata_cache_control(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/set_metadata_cache.yaml")
        self.gcp_client.set_metadata_cache_control(cloud_path="gs://test_bucket/uploaded_blob.txt",cache_control="private, max-age=0, no-store")

    @responses.activate
    def test_get_most_recent_blob_content(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/read_newest_blob.yaml")
        data = self.gcp_client.get_file_contents_of_most_recent_blob_in_bucket(bucket_name="test_bucket")
        assert data


    @responses.activate
    def test_write_to_gcp_file(self):
        responses._add_from_file(file_path="ops_utils/tests/data/gcp_util/write_to_blob.yaml")
        self.gcp_client.write_to_gcp_file(cloud_path='gs://test_bucket/blob_to_write.txt', file_contents="test data here")

