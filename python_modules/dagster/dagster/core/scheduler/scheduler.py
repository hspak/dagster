import abc
from collections import namedtuple
from enum import Enum

import six

from dagster import check
from dagster.core.definitions.schedule import ScheduleDefinition, ScheduleDefinitionData
from dagster.core.serdes import whitelist_for_serdes
from dagster.utils.error import SerializableErrorInfo


@whitelist_for_serdes
class ScheduleStatus(Enum):
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    ENDED = 'ENDED'


def get_schedule_change_set(old_schedules, new_schedule_defs):
    check.list_param(old_schedules, 'old_schedules', Schedule)
    check.list_param(new_schedule_defs, 'new_schedule_defs', ScheduleDefinition)

    new_schedules_defs_dict = {s.name: s for s in new_schedule_defs}
    old_schedules_dict = {s.name: s for s in old_schedules}

    new_schedule_defs_names = set(new_schedules_defs_dict.keys())
    old_schedules_names = set(old_schedules_dict.keys())

    added_schedules = new_schedule_defs_names - old_schedules_names
    changed_schedules = new_schedule_defs_names & old_schedules_names
    removed_schedules = old_schedules_names - new_schedule_defs_names

    changeset = []

    for schedule_name in added_schedules:
        changeset.append(("add", schedule_name, []))

    for schedule_name in changed_schedules:
        changes = []

        old_schedule_def = old_schedules_dict[schedule_name].schedule_definition_data
        new_schedule_def = new_schedules_defs_dict[schedule_name]

        if old_schedule_def.cron_schedule != new_schedule_def.cron_schedule:
            changes.append(
                ("cron_schedule", (old_schedule_def.cron_schedule, new_schedule_def.cron_schedule))
            )

        if len(changes) > 0:
            changeset.append(("change", schedule_name, changes))

    for schedule_name in removed_schedules:
        changeset.append(("remove", schedule_name, []))

    return changeset


class SchedulerHandle(object):
    def __init__(
        self, schedule_defs,
    ):
        check.list_param(schedule_defs, 'schedule_defs', ScheduleDefinition)
        self._schedule_defs = schedule_defs

    def up(self, python_path, repository_path, repository, instance):
        '''SchedulerHandle stores a list of up-to-date ScheduleDefinitions and a reference to a
        ScheduleStorage. When `up` is called, it reconciles the ScheduleDefinitions list and
        ScheduleStorage to ensure there is a 1-1 correlation between ScheduleDefinitions and
        Schedules, where the ScheduleDefinitions list is the source of truth.

        If a new ScheduleDefinition is introduced, a new Schedule is added to storage with status
        ScheduleStatus.STOPPED.

        For every previously existing ScheduleDefinition (where schedule_name is the primary key),
        any changes to the definition are persisted in the corresponding Schedule and the status is
        left unchanged. The schedule is also restarted to make sure the external articfacts (such
        as a cron job) are up to date.

        For every ScheduleDefinitions that is removed, the corresponding Schedule is removed from
        the storage and the corresponding Schedule is ended.
        '''

        schedules_to_restart = []
        for schedule_def in self._schedule_defs:
            # If a schedule already exists for schedule_def, overwrite bash script and
            # metadata file
            existing_schedule = instance.get_schedule_by_name(repository, schedule_def.name)
            if existing_schedule:
                # Keep the status, but replace schedule_def, python_path, and repository_path
                schedule = Schedule(
                    schedule_def.schedule_definition_data,
                    existing_schedule.status,
                    python_path,
                    repository_path,
                )

                instance.update_schedule(repository, schedule)
                schedules_to_restart.append(schedule)
            else:
                schedule = Schedule(
                    schedule_def.schedule_definition_data,
                    ScheduleStatus.STOPPED,
                    python_path,
                    repository_path,
                )

                instance.add_schedule(repository, schedule)

        # Delete all existing schedules that are not in schedule_defs
        schedule_def_names = {s.name for s in self._schedule_defs}
        existing_schedule_names = set([s.name for s in instance.all_schedules(repository)])
        schedule_names_to_delete = existing_schedule_names - schedule_def_names

        for schedule in schedules_to_restart:
            # Restart is only needed if the schedule was previously running
            if schedule.status == ScheduleStatus.RUNNING:
                instance.stop_schedule(repository, schedule.name)
                instance.start_schedule(repository, schedule.name)

        for schedule_name in schedule_names_to_delete:
            instance.end_schedule(repository, schedule_name)

    def get_change_set(self, repository, instance):
        schedule_defs = self.all_schedule_defs()
        schedules = instance.all_schedules(repository)
        return get_schedule_change_set(schedules, schedule_defs)

    def all_schedule_defs(self):
        return self._schedule_defs

    def get_schedule_def_by_name(self, name):
        return next(
            schedule_def for schedule_def in self._schedule_defs if schedule_def.name == name
        )


class Scheduler(six.with_metaclass(abc.ABCMeta)):
    '''Abstract base class for a scheduler. This component is responsible for interfacing with
    an external system such as cron to ensure scheduled repeated execution according.
    '''

    @abc.abstractmethod
    def start_schedule(self, instance, repository, schedule_name):
        '''Resume a pipeline schedule.

        Args:
            schedule_name (string): The schedule to resume
        '''

    @abc.abstractmethod
    def stop_schedule(self, instance, repository, schedule_name):
        '''Stops an existing pipeline schedule

        Args:
            schedule_name (string): The schedule to stop
        '''

    @abc.abstractmethod
    def end_schedule(self, instance, repository, schedule_name):
        '''Resume a pipeline schedule.

        Args:
            schedule_name (string): The schedule to end and delete
        '''

    @abc.abstractmethod
    def get_log_path(self, instance, repository, schedule_name):
        '''Get path to store logs for schedule
        '''


@whitelist_for_serdes
class Schedule(
    namedtuple('Schedule', 'schedule_definition_data status python_path repository_path')
):
    def __new__(cls, schedule_definition_data, status, python_path=None, repository_path=None):

        return super(Schedule, cls).__new__(
            cls,
            check.inst_param(
                schedule_definition_data, 'schedule_definition_data', ScheduleDefinitionData
            ),
            check.inst_param(status, 'status', ScheduleStatus),
            check.opt_str_param(python_path, 'python_path'),
            check.opt_str_param(repository_path, 'repository_path'),
        )

    @property
    def name(self):
        return self.schedule_definition_data.name

    @property
    def cron_schedule(self):
        return self.schedule_definition_data.cron_schedule

    @property
    def environment_vars(self):
        return self.schedule_definition_data.environment_vars

    def with_status(self, status):
        check.inst_param(status, 'status', ScheduleStatus)

        return Schedule(
            self.schedule_definition_data,
            status=status,
            python_path=self.python_path,
            repository_path=self.repository_path,
        )


@whitelist_for_serdes
class ScheduleTickStatus(Enum):
    STARTED = 'STARTED'
    SKIPPED = 'SKIPPED'
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'


@whitelist_for_serdes
class ScheduleTickSuccessData(namedtuple('_ScheduleTickSuccessData', 'run_id')):
    def __new__(cls, run_id):
        return super(ScheduleTickSuccessData, cls).__new__(cls, run_id=check.str_param(run_id, 'run_id'))


@whitelist_for_serdes
class ScheduleTickFailureData(namedtuple('_ScheduleTickFailureData', 'error')):
    def __new__(cls, error):
        return super(ScheduleTickFailureData, cls).__new__(
            cls, error=check.opt_inst_param(error, 'error', SerializableErrorInfo),
        )


def _validate_tick_specific_data(tick_status, tick_specific_data):
    if tick_status == ScheduleTickStatus.SUCCESS:
        check.inst_param(tick_specific_data, 'tick_specific_data', ScheduleTickSuccessData)
    elif tick_status == ScheduleTickStatus.FAILURE:
        check.inst_param(tick_specific_data, 'tick_specific_data', ScheduleTickFailureData)

    return tick_specific_data


@whitelist_for_serdes
class ScheduleTickData(namedtuple('Schedule', 'schedule_name status tick_specific_data')):
    def __new__(cls, schedule_name, status, tick_specific_data=None):

        return super(ScheduleTickData, cls).__new__(
            cls,
            check.str_param(schedule_name, 'schedule_name'),
            check.inst_param(status, 'status', ScheduleTickStatus),
            _validate_tick_specific_data(status, tick_specific_data),
        )

    def with_status(self, status, tick_specific_data=None):
        check.inst_param(status, 'status', ScheduleTickStatus)
        return self._replace(
            status=status,
            tick_specific_data=_validate_tick_specific_data(status, tick_specific_data),
        )


class ScheduleTick(namedtuple('Schedule', 'tick_id schedule_tick_data')):
    def __new__(cls, tick_id, schedule_tick_data):

        return super(ScheduleTick, cls).__new__(
            cls,
            check.int_param(tick_id, 'tick_id'),
            check.inst_param(schedule_tick_data, 'schedule_tick_data', ScheduleTickData),
        )

    def with_status(self, status, tick_specific_data=None):
        check.inst_param(status, 'status', ScheduleTickStatus)
        return self._replace(
            schedule_tick_data=self.schedule_tick_data.with_status(status, tick_specific_data)
        )

    @property
    def schedule_name(self):
        return self.schedule_tick_data.schedule_name

    @property
    def status(self):
        return self.schedule_tick_data.status

    @property
    def tick_specific_data(self):
        return self.schedule_tick_data.tick_specific_data
