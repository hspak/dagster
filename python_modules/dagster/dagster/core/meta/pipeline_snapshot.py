from collections import namedtuple

from dagster import check

from .config_types import ConfigSchemaSnapshot, build_config_schema_snapshot
from .dagster_types import DagsterTypeNamespaceSnapshot, build_dagster_type_namespace_snapshot
from .solid import SolidDefinitionsSnapshot, build_solid_definitions_snapshot


class PipelineSnapshot(
    namedtuple(
        '_PipelineSnapshot',
        'config_schema_snapshot dagster_type_namespace_snapshot solid_definitions_snapshot',
    )
):
    def __new__(
        cls, config_schema_snapshot, dagster_type_namespace_snapshot, solid_definitions_snapshot,
    ):
        return super(PipelineSnapshot, cls).__new__(
            cls,
            config_schema_snapshot=check.inst_param(
                config_schema_snapshot, 'config_schema_snapshot', ConfigSchemaSnapshot
            ),
            dagster_type_namespace_snapshot=check.inst_param(
                dagster_type_namespace_snapshot,
                'dagster_type_namespace_snapshot',
                DagsterTypeNamespaceSnapshot,
            ),
            solid_definitions_snapshot=check.inst_param(
                solid_definitions_snapshot, 'solid_definitions_snapshot', SolidDefinitionsSnapshot
            ),
        )

    @staticmethod
    def from_pipeline_def(pipeline_def):
        return PipelineSnapshot(
            config_schema_snapshot=build_config_schema_snapshot(pipeline_def),
            dagster_type_namespace_snapshot=build_dagster_type_namespace_snapshot(pipeline_def),
            solid_definitions_snapshot=build_solid_definitions_snapshot(pipeline_def),
        )

