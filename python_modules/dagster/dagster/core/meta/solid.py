from collections import namedtuple

from dagster import check
from dagster.core.definitions import (
    CompositeSolidDefinition,
    InputDefinition,
    OutputDefinition,
    PipelineDefinition,
    SolidDefinition,
)
from dagster.core.serdes import whitelist_for_serdes

from .config_types import ConfigFieldMeta, meta_from_field


def build_solid_definitions_snapshot(pipeline_def):
    check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)
    return SolidDefinitionsSnapshot(
        solid_defs={
            solid_def.name: build_solid_def_meta(solid_def)
            for solid_def in pipeline_def.all_solid_defs
            if isinstance(solid_def, SolidDefinition)
        },
        composite_solid_defs={
            solid_def.name: build_composite_solid_def_meta(solid_def)
            for solid_def in pipeline_def.all_solid_defs
            if isinstance(solid_def, CompositeSolidDefinition)
        },
    )


class SolidDefinitionsSnapshot(
    namedtuple('_SolidDefinitionsSnapshot', 'solid_defs composite_solid_defs')
):
    pass


def build_input_def_meta(input_def):
    check.inst_param(input_def, 'input_def', InputDefinition)
    return InputDefMeta(
        name=input_def.name,
        dagster_type_key=input_def.dagster_type.key,
        description=input_def.description,
    )


def build_output_def_meta(output_def):
    check.inst_param(output_def, 'output_def', OutputDefinition)
    return OutputDefMeta(
        name=output_def.name,
        dagster_type_key=output_def.dagster_type.key,
        description=output_def.description,
        is_required=output_def.is_required,
    )


def build_composite_solid_def_meta(comp_solid_def):
    check.inst_param(comp_solid_def, 'comp_solid_def', CompositeSolidDefinition)
    return CompositeSolidDefMeta(
        name=comp_solid_def.name,
        input_def_metas=list(map(build_input_def_meta, comp_solid_def.input_defs)),
        output_def_metas=list(map(build_output_def_meta, comp_solid_def.output_defs)),
        description=comp_solid_def.description,
        tags=comp_solid_def.tags,
        positional_inputs=comp_solid_def.positional_inputs,
        original_def=comp_solid_def,
    )


def build_solid_def_meta(solid_def):
    check.inst_param(solid_def, 'solid_def', SolidDefinition)
    return SolidDefMeta(
        name=solid_def.name,
        input_def_metas=list(map(build_input_def_meta, solid_def.input_defs)),
        output_def_metas=list(map(build_output_def_meta, solid_def.output_defs)),
        description=solid_def.description,
        tags=solid_def.tags,
        positional_inputs=solid_def.positional_inputs,
        required_resource_keys=list(solid_def.required_resource_keys),
        config_field_meta=meta_from_field('config', solid_def.config_field)
        if solid_def.config_field
        else None,
    )


# This and _check_solid_def_header_args helps a defacto mixin for
# CompositeSolidDefMeta and SolidDefMeta. Inheritance is quick difficult
# and counterintuitive in namedtuple land, so went with this scheme instead.
SOLID_DEF_HEADER_PROPS = 'name input_def_metas output_def_metas description tags positional_inputs'


def _check_solid_def_header_args(
    name, input_def_metas, output_def_metas, description, tags, positional_inputs,
):
    return dict(
        name=check.str_param(name, 'name'),
        input_def_metas=check.list_param(input_def_metas, 'input_def_metas', InputDefMeta),
        output_def_metas=check.list_param(output_def_metas, 'output_def_metas', OutputDefMeta),
        description=check.opt_str_param(description, 'description'),
        tags=check.dict_param(tags, 'tags'),  # validate using validate_tags?
        positional_inputs=check.list_param(positional_inputs, 'positional_inputs', str),
    )


# Currently only has the properties common to compute solids and composites
# In order to get other properties with temporarily include the original definition
class CompositeSolidDefMeta(
    namedtuple('_CompositeSolidDefMeta', SOLID_DEF_HEADER_PROPS + ' original_def')
):
    def __new__(
        cls,
        name,
        input_def_metas,
        output_def_metas,
        description,
        tags,
        positional_inputs,
        original_def,
    ):
        return super(CompositeSolidDefMeta, cls).__new__(
            cls,
            original_def=original_def,
            **_check_solid_def_header_args(
                name, input_def_metas, output_def_metas, description, tags, positional_inputs
            )
        )


@whitelist_for_serdes
class SolidDefMeta(
    namedtuple(
        '_SolidDefMeta', SOLID_DEF_HEADER_PROPS + ' config_field_meta required_resource_keys'
    )
):
    def __new__(
        cls,
        name,
        input_def_metas,
        output_def_metas,
        description,
        tags,
        positional_inputs,
        config_field_meta,
        required_resource_keys,
    ):
        return super(SolidDefMeta, cls).__new__(
            cls,
            config_field_meta=check.opt_inst_param(
                config_field_meta, 'config_field_meta', ConfigFieldMeta
            ),
            required_resource_keys=check.list_param(
                required_resource_keys, 'required_resource_keys', str
            ),
            **_check_solid_def_header_args(
                name, input_def_metas, output_def_metas, description, tags, positional_inputs
            )
        )

    def get_input_meta(self, name):
        check.str_param(name, 'name')
        for inp in self.input_def_metas:
            if inp.name == name:
                return inp

        check.failed('Could not find input ' + name)

    def get_output_meta(self, name):
        check.str_param(name, 'name')
        for out in self.output_def_metas:
            if out.name == name:
                return out

        check.failed('Could not find output ' + name)


ISolidDefMeta = (CompositeSolidDefMeta, SolidDefMeta)


@whitelist_for_serdes
class InputDefMeta(namedtuple('_InputDefMeta', 'name dagster_type_key description')):
    def __new__(cls, name, dagster_type_key, description):
        return super(InputDefMeta, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type_key=check.str_param(dagster_type_key, 'dagster_type_key'),
            description=check.opt_str_param(description, 'description'),
        )


@whitelist_for_serdes
class OutputDefMeta(namedtuple('_OutputDefMeta', 'name dagster_type_key description is_required')):
    def __new__(cls, name, dagster_type_key, description, is_required):
        return super(OutputDefMeta, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type_key=check.str_param(dagster_type_key, 'dagster_type_key'),
            description=check.opt_str_param(description, 'description'),
            is_required=check.bool_param(is_required, 'is_required'),
        )
