import uuid
from io import BytesIO

import pyarrow as pa
from dagster_hadoop.hdfs import HDFSObjectStore

from dagster.core.types.marshal import PickleSerializationStrategy


def test_gcs_object_store():
    fs = pa.hdfs.connect('localhost')
    object_store = HDFSObjectStore(hdfs_prefix='/dagster', hdfs_fs=fs)

    test_str = b'this is a test'
    file_obj = BytesIO(test_str)
    file_obj.seek(0)

    serialization_strategy = PickleSerializationStrategy()

    key = 'test-file-%s' % uuid.uuid4().hex
    object_store.set_object(key, file_obj, serialization_strategy)

    assert object_store.has_object(key)
    assert object_store.get_object(key, serialization_strategy).obj.read() == test_str

    other_key = 'test-file-%s' % uuid.uuid4().hex
    object_store.cp_object(key, other_key)
    assert object_store.has_object(other_key)

    object_store.rm_object(key)
    object_store.rm_object(other_key)
    assert not object_store.has_object(key)
    assert not object_store.has_object(other_key)
