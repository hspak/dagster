import logging
import subprocess
import sys
from distutils import spawn  # pylint: disable=no-name-in-module
from io import BytesIO, StringIO

import six
from pyarrow.hdfs import HadoopFileSystem

from dagster import check
from dagster.core.definitions.events import ObjectStoreOperation, ObjectStoreOperationType
from dagster.core.storage.object_store import ObjectStore
from dagster.core.types.marshal import SerializationStrategy


class HDFSObjectStore(ObjectStore):
    def __init__(self, hdfs_fs):
        self.hdfs_fs = check.inst_param(hdfs_fs, 'hdfs_fs', HadoopFileSystem)
        super(HDFSObjectStore, self).__init__('hdfs', sep='/')

    def set_object(self, key, obj, serialization_strategy=None):
        check.str_param(key, 'key')

        # cannot check obj since could be arbitrary Python object
        check.inst_param(
            serialization_strategy, 'serialization_strategy', SerializationStrategy
        )  # cannot be none here

        logging.info('Writing HDFS object at: ' + self.uri_for_key(key))

        if self.has_object(key):
            logging.warning('Removing existing GCS key: {key}'.format(key=key))
            self.rm_object(key)

        with (
            BytesIO()
            if serialization_strategy.write_mode == 'wb' or sys.version_info < (3, 0)
            else StringIO()
        ) as file_like:
            serialization_strategy.serialize(obj, file_like)
            file_like.seek(0)
            self.hdfs_fs.upload(key, file_like)

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.SET_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=obj,
            serialization_strategy_name=serialization_strategy.name,
            object_store_name=self.name,
        )

    def get_object(self, key, serialization_strategy=None):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')

        if serialization_strategy.read_mode == 'rb':
            file_obj = BytesIO(self.hdfs_fs.cat(key))
        else:
            file_obj = StringIO(
                six.ensure_str(self.hdfs_fs.cat(key)).decode(serialization_strategy.encoding)
            )

        file_obj.seek(0)

        obj = serialization_strategy.deserialize(file_obj)
        return ObjectStoreOperation(
            op=ObjectStoreOperationType.GET_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=obj,
            serialization_strategy_name=serialization_strategy.name,
            object_store_name=self.name,
        )

    def has_object(self, key):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')
        return self.hdfs_fs.exists(key)

    def rm_object(self, key):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')

        if self.has_object(key):
            self.hdfs_fs.delete(key)

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.RM_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=None,
            serialization_strategy_name=None,
            object_store_name=self.name,
        )

    def cp_object(self, src, dst):
        check.str_param(src, 'src')
        check.str_param(dst, 'dst')

        hdfs_cli = which_('hdfs')
        if hdfs_cli:
            logging.warning('Copy is not implemented by pyarrow, falling back to hdfs CLI')
            subprocess.check_output(['hdfs', 'dfs', '-cp', src, dst])
        else:
            raise NotImplementedError(
                'Copy is not implemented by pyarrow and hdfs CLI not found, giving up'
            )

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.CP_OBJECT,
            key=self.uri_for_key(src),
            dest_key=self.uri_for_key(dst),
            object_store_name=self.name,
        )

    def uri_for_key(self, key, protocol=None):
        check.str_param(key, 'key')
        protocol = check.opt_str_param(protocol, 'protocol', default='hdfs://')
        return '%s/%s' % (protocol, key)


def which_(exe):
    '''Uses distutils to look for an executable, mimicking unix which'''
    # https://github.com/PyCQA/pylint/issues/73
    return spawn.find_executable(exe)
