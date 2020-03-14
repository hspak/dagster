from collections import namedtuple

from dagster import check

from .config_types import ConfigSchemaSnapshot, build_config_schema_snapshot
from .dagster_types import DagsterTypeNamespaceSnapshot, build_dagster_type_namespace_snapshot


class PipelineSnapshot(
    namedtuple('_PipelineSnapshot', 'config_schema_snapshot dagster_type_namespace_snapshot')
):
    def __new__(cls, config_schema_snapshot, dagster_type_namespace_snapshot):
        return super(PipelineSnapshot, cls).__new__(
            cls,
            check.inst_param(
                config_schema_snapshot, 'config_schema_snapshot', ConfigSchemaSnapshot
            ),
            check.inst_param(
                dagster_type_namespace_snapshot,
                'dagster_type_namespace_snapshot',
                DagsterTypeNamespaceSnapshot,
            ),
        )

    @staticmethod
    def from_pipeline_def(pipeline_def):
        return PipelineSnapshot(
            build_config_schema_snapshot(pipeline_def),
            build_dagster_type_namespace_snapshot(pipeline_def),
        )
