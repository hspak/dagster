from dagster import Field, SystemStorageData, system_storage
from dagster.core.storage.intermediates_manager import IntermediateStoreIntermediatesManager
from dagster.core.storage.system_storage import fs_system_storage, mem_system_storage

from .file_manager import HDFSFileManager
from .intermediate_store import HDFSIntermediateStore


@system_storage(
    name='hdfs',
    is_persistent=True,
    config={'hdfs_prefix': Field(str, is_required=False, default_value='/dagster')},
    required_resource_keys={'hdfs'},
)
def hdfs_system_storage(init_context):
    hdfs_fs = init_context.resources.hdfs.fs
    hdfs_prefix = '{path}/storage/{run_id}/files'.format(
        path=init_context.system_storage_config['hdfs_path'],
        run_id=init_context.pipeline_run.run_id,
    )
    return SystemStorageData(
        file_manager=HDFSFileManager(hdfs_fs=hdfs_fs, hdfs_prefix=hdfs_prefix),
        intermediates_manager=IntermediateStoreIntermediatesManager(
            HDFSIntermediateStore(
                hdfs_fs=hdfs_fs,
                hdfs_prefix=init_context.system_storage_config['hdfs_prefix'],
                run_id=init_context.pipeline_run.run_id,
                type_storage_plugin_registry=init_context.type_storage_plugin_registry,
            )
        ),
    )


hdfs_plus_default_storage_defs = [mem_system_storage, fs_system_storage, hdfs_system_storage]
