import datetime
from collections import defaultdict

from dagster import PartitionSetDefinition, ScheduleExecutionContext
from dagster.core.definitions.pipeline import PipelineRunsFilter
from dagster.core.scheduler import SchedulerHandle
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.utils.partitions import date_partition_range


def backfilling_partition_selector(
    context: ScheduleExecutionContext, partition_set_def: PartitionSetDefinition
):
    partitions = partition_set_def.get_partitions()
    if not partitions:
        return None

    # query runs db for this partition set
    filters = PipelineRunsFilter(tags={'dagster/partition_set': partition_set_def.name,})
    partition_set_runs = context.instance.get_runs(filters)

    runs_by_partition = defaultdict(list)

    for run in partition_set_runs:
        runs_by_partition[run.tags['dagster/partition']].append(run)

    previous_partition_in_flight = False
    for partition in partitions:
        runs = runs_by_partition[partition.name]

        # when we find the first empty partition
        if len(runs) == 0:
            # execute it the previous one is still not active
            if not previous_partition_in_flight:
                return partition
            # other wise skip this tick, wait for previous to finish
            else:
                return None

        # active runs for a given partition may be manual retries or the
        # current tip of the backfill progression
        if any([run.status == PipelineRunStatus.STARTED for run in runs]):
            previous_partition_in_flight = True

        # Other impls would check the runs here and choose to do something if there were only errors for
        # a given partition, for example throw and stop the backfill schedule from progressing at all

    return None


def backfill_test_schedule():
    # create weekly partion set
    partition_set = PartitionSetDefinition(
        name='unreliable_weekly',
        pipeline_name='unreliable_pipeline',
        partition_fn=date_partition_range(
            # first sunday of the year
            start=datetime.datetime(2020, 1, 5),
            delta=datetime.timedelta(weeks=1),
        ),
        environment_dict_fn_for_partition=lambda _: {'storage': {'filesystem': {}}},
    )
    return partition_set.create_schedule_definition(
        schedule_name='backfill_unreliable_weekly',
        cron_schedule="* * * * *",  # tick every minute
        partition_selector=backfilling_partition_selector,
    )


def get_bay_bikes_schedules():
    from dagster_examples.bay_bikes.schedules import (
        daily_weather_ingest_schedule,
        monthly_trip_ingest_schedule,
    )

    return [daily_weather_ingest_schedule, monthly_trip_ingest_schedule]


def get_toys_schedules():
    from dagster import ScheduleDefinition, file_relative_path

    return [
        backfill_test_schedule(),
        ScheduleDefinition(
            name="many_events_every_min",
            cron_schedule="* * * * *",
            pipeline_name='many_events',
            environment_dict_fn=lambda _: {"storage": {"filesystem": {}}},
        ),
        ScheduleDefinition(
            name="pandas_hello_world_hourly",
            cron_schedule="0 * * * *",
            pipeline_name="pandas_hello_world_pipeline",
            environment_dict_fn=lambda _: {
                'solids': {
                    'mult_solid': {
                        'inputs': {
                            'num_df': {
                                'csv': {
                                    'path': file_relative_path(
                                        __file__, "pandas_hello_world/data/num.csv"
                                    )
                                }
                            }
                        }
                    },
                    'sum_solid': {
                        'inputs': {
                            'num_df': {
                                'csv': {
                                    'path': file_relative_path(
                                        __file__, "pandas_hello_world/data/num.csv"
                                    )
                                }
                            }
                        }
                    },
                },
                "storage": {"filesystem": {}},
            },
        ),
    ]


def define_scheduler():
    # Done instead of using schedules to avoid circular dependency issues.
    return SchedulerHandle(schedule_defs=get_bay_bikes_schedules() + get_toys_schedules())


def get_toys_pipelines():
    from dagster_examples.toys.error_monster import error_monster
    from dagster_examples.toys.sleepy import sleepy_pipeline
    from dagster_examples.toys.log_spew import log_spew
    from dagster_examples.toys.stdout_spew import stdout_spew_pipeline
    from dagster_examples.toys.many_events import many_events
    from dagster_examples.toys.composition import composition
    from dagster_examples.toys.pandas_hello_world import (
        pandas_hello_world_pipeline,
        pandas_hello_world_pipeline_with_read_csv,
    )
    from dagster_examples.toys.unreliable import unreliable_pipeline

    return [
        composition,
        error_monster,
        log_spew,
        many_events,
        pandas_hello_world_pipeline,
        pandas_hello_world_pipeline_with_read_csv,
        sleepy_pipeline,
        stdout_spew_pipeline,
        unreliable_pipeline,
    ]


def get_airline_demo_pipelines():
    from dagster_examples.airline_demo.pipelines import (
        airline_demo_ingest_pipeline,
        airline_demo_warehouse_pipeline,
    )

    return [
        airline_demo_ingest_pipeline,
        airline_demo_warehouse_pipeline,
    ]


def get_event_pipelines():
    from dagster_examples.event_pipeline_demo.pipelines import event_ingest_pipeline

    return [event_ingest_pipeline]


def get_pyspark_pipelines():
    from dagster_examples.pyspark_pagerank.pyspark_pagerank_pipeline import pyspark_pagerank

    return [pyspark_pagerank]


def get_jaffle_pipelines():
    from dagster_examples.jaffle_dbt.jaffle import jaffle_pipeline

    return [jaffle_pipeline]


def get_bay_bikes_pipelines():
    from dagster_examples.bay_bikes.pipelines import generate_training_set_and_train_model

    return [generate_training_set_and_train_model]


def define_demo_repo():
    # Lazy import here to prevent deps issues
    from dagster import RepositoryDefinition

    pipeline_defs = (
        get_airline_demo_pipelines()
        + get_bay_bikes_pipelines()
        + get_event_pipelines()
        + get_jaffle_pipelines()
        + get_pyspark_pipelines()
        + get_toys_pipelines()
    )

    return RepositoryDefinition(name='internal-dagit-repository', pipeline_defs=pipeline_defs,)
