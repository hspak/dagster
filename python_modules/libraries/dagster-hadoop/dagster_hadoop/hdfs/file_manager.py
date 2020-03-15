import io
import uuid
from contextlib import contextmanager

from dagster import check, usable_as_dagster_type
from dagster.core.storage.file_manager import (
    FileHandle,
    FileManager,
    TempfileManager,
    check_file_like_obj,
)


@usable_as_dagster_type
class HDFSFileHandle(FileHandle):
    def __init__(self, path, namenode_host=None):
        self._path = check.str_param(path, 'path')
        self.namenode_host = check.opt_str_param(namenode_host, 'namenode_host')

    @property
    def path(self):
        return self._path

    @property
    def path_desc(self):
        return self._path

    @property
    def hdfs_path(self):
        return 'hdfs://{namenode_host}/{path}'.format(
            namenode_host=self.namenode_host or '', path=self._path
        )


class HDFSFileManager(FileManager):
    def __init__(self, hdfs_fs, hdfs_path):
        self._hdfs_path = check.str_param(hdfs_path, 'hdfs_path')
        self._local_handle_cache = {}
        self._temp_file_manager = TempfileManager()

    def copy_handle_to_local_temp(self, file_handle):
        self._download_if_not_cached(file_handle)
        return self._get_local_path(file_handle)

    def _download_if_not_cached(self, file_handle):
        if not self._file_handle_cached(file_handle):
            # instigate download
            temp_file_obj = self._temp_file_manager.tempfile()
            temp_name = temp_file_obj.name
            bucket_obj = self._client.get_bucket(file_handle.gcs_bucket)
            bucket_obj.blob(file_handle.gcs_key).download_to_file(temp_file_obj)
            self._local_handle_cache[file_handle.hdfs_path] = temp_name

        return file_handle

    @contextmanager
    def read(self, file_handle, mode='rb'):
        check.inst_param(file_handle, 'file_handle', HDFSFileHandle)
        check.str_param(mode, 'mode')
        check.param_invariant(mode in {'r', 'rb'}, 'mode')

        self._download_if_not_cached(file_handle)

        with open(self._get_local_path(file_handle), mode) as file_obj:
            yield file_obj

    def _file_handle_cached(self, file_handle):
        return file_handle.hdfs_path in self._local_handle_cache

    def _get_local_path(self, file_handle):
        return self._local_handle_cache[file_handle.hdfs_path]

    def read_data(self, file_handle):
        with self.read(file_handle, mode='rb') as file_obj:
            return file_obj.read()

    def write_data(self, data):
        check.inst_param(data, 'data', bytes)
        return self.write(io.BytesIO(data), mode='wb')

    def write(self, file_obj, mode='wb'):
        check_file_like_obj(file_obj)
        gcs_key = self.get_full_key(str(uuid.uuid4()))
        bucket_obj = self._client.get_bucket(self._gcs_bucket)
        bucket_obj.blob(gcs_key).upload_from_file(file_obj)
        return HDFSFileHandle(self._gcs_bucket, gcs_key)

    def get_full_key(self, file_key):
        return '{base_key}/{file_key}'.format(base_key=self._gcs_base_key, file_key=file_key)

    def delete_local_temp(self):
        self._temp_file_manager.close()
