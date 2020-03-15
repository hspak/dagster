from pyarrow.hdfs import HadoopFileSystem

from dagster import check
from dagster.core.storage.intermediate_store import IntermediateStore
from dagster.core.storage.type_storage import TypeStoragePluginRegistry

from .object_store import HDFSObjectStore


class HDFSIntermediateStore(IntermediateStore):
    def __init__(
        self, run_id, hdfs_fs=None, type_storage_plugin_registry=None, hdfs_prefix='/dagster',
    ):
        check.str_param(hdfs_prefix, 'hdfs_path')
        check.str_param(run_id, 'run_id')
        check.inst_param(hdfs_fs, 'hdfs_fs', HadoopFileSystem)

        object_store = HDFSObjectStore(hdfs_prefix, hdfs_fs=hdfs_fs)

        def root_for_run_id(r_id):
            return object_store.key_for_paths([hdfs_prefix, 'storage', r_id])

        super(HDFSIntermediateStore, self).__init__(
            object_store,
            root_for_run_id=root_for_run_id,
            run_id=run_id,
            type_storage_plugin_registry=check.inst_param(
                type_storage_plugin_registry
                if type_storage_plugin_registry
                else TypeStoragePluginRegistry(types_to_register=[]),
                'type_storage_plugin_registry',
                TypeStoragePluginRegistry,
            ),
        )
