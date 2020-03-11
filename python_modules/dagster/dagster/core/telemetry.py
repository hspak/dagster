# As an open source project, we collect usage statistics to inform development priorities.
# For more information, check out the docs at https://docs.dagster.io/latest/install/telemetry/'

import datetime
import os
import uuid

import requests
import yaml

from dagster.core.instance.config import DAGSTER_CONFIG_YAML_FILENAME

TELEMETRY_STR = 'telemetry'
INSTANCE_ID_STR = 'instance_id'
ENABLED_STR = 'enabled'


def telemetry_wrapper(f):
    def wrap(*args, **kwargs):
        start_time = datetime.datetime.now()
        log_action(action=f.__name__ + "_started", client_time=start_time)
        result = f(*args, **kwargs)
        end_time = datetime.datetime.now()
        log_action(
            action=f.__name__ + "_ended",
            client_time=end_time,
            elapsed_time=end_time - start_time,
            metadata={'success': getattr(result, 'success', None)},
        )
        return result

    return wrap


def log_action(action, client_time, elapsed_time=None, metadata=None):
    if os.getenv('DAGSTER_TELEMETRY_ENABLED') == 'False':
        return

    try:
        (instance_id, dagster_telemetry_enabled) = _get_instance_id()

        if dagster_telemetry_enabled is False:
            return

        requests.post(
            'https://telemetry.elementl.dev/actions',
            data={
                'instance_id': instance_id,
                'action': action,
                'client_time': client_time,
                'elapsed_time': elapsed_time,
                'metadata': metadata,
            },
        )
    except Exception:  # pylint: disable=broad-except
        pass


def _dagster_home_if_set():
    dagster_home_path = os.getenv('DAGSTER_HOME')

    if not dagster_home_path:
        return None

    return os.path.expanduser(dagster_home_path)


def _get_instance_id():
    instance_id = None
    dagster_telemetry_enabled = None
    dagster_home_path = _dagster_home_if_set()

    if dagster_home_path is None:
        return (uuid.getnode(), True)

    instance_config_path = os.path.join(dagster_home_path, DAGSTER_CONFIG_YAML_FILENAME)

    if not os.path.exists(instance_config_path):
        with open(instance_config_path, 'w') as f:
            instance_id = str(uuid.getnode())
            dagster_telemetry_enabled = True
            yaml.dump(
                {
                    TELEMETRY_STR: {
                        INSTANCE_ID_STR: instance_id,
                        ENABLED_STR: dagster_telemetry_enabled,
                    }
                },
                f,
            )
    else:
        with open(instance_config_path, 'r') as f:
            instance_profile_json = yaml.load(f, Loader=yaml.FullLoader)
            if instance_profile_json is None:
                instance_profile_json = {}

            if TELEMETRY_STR in instance_profile_json:
                if INSTANCE_ID_STR in instance_profile_json[TELEMETRY_STR]:
                    instance_id = instance_profile_json[TELEMETRY_STR][INSTANCE_ID_STR]
                if ENABLED_STR in instance_profile_json[TELEMETRY_STR]:
                    dagster_telemetry_enabled = instance_profile_json[TELEMETRY_STR][ENABLED_STR]

        if not dagster_telemetry_enabled is False and (
            instance_id is None or dagster_telemetry_enabled is None
        ):
            if instance_id is None:
                instance_id = str(uuid.uuid4())
                instance_profile_json[TELEMETRY_STR][INSTANCE_ID_STR] = instance_id
            if dagster_telemetry_enabled is None:
                dagster_telemetry_enabled = True
                instance_profile_json[TELEMETRY_STR][ENABLED_STR] = dagster_telemetry_enabled

            with open(instance_config_path, 'w') as f:
                yaml.dump(instance_profile_json, f)

    return (instance_id, dagster_telemetry_enabled)


def execute_reset_telemetry_profile():
    dagster_home_path = _dagster_home_if_set()
    if not dagster_home_path:
        print('Must set $DAGSTER_HOME environment variable to reset profile')
        return

    instance_config_path = os.path.join(dagster_home_path, DAGSTER_CONFIG_YAML_FILENAME)
    if not os.path.exists(instance_config_path):
        with open(instance_config_path, 'w') as f:
            yaml.dump({TELEMETRY_STR: {INSTANCE_ID_STR: str(uuid.uuid4())}}, f)

    else:
        with open(instance_config_path, 'r') as f:
            instance_profile_json = yaml.load(f, Loader=yaml.FullLoader)
            if TELEMETRY_STR in instance_profile_json:
                instance_profile_json[TELEMETRY_STR][INSTANCE_ID_STR] = str(uuid.uuid4())
            else:
                instance_profile_json[TELEMETRY_STR] = {INSTANCE_ID_STR: str(uuid.uuid4())}

            with open(instance_config_path, 'w') as f:
                yaml.dump(instance_profile_json, f)


def execute_disable_telemetry():
    _toggle_telemetry(False)


def execute_enable_telemetry():
    _toggle_telemetry(True)


def _toggle_telemetry(enable_telemetry):
    dagster_home_path = _dagster_home_if_set()
    if not dagster_home_path:
        print(
            "Must set $DAGSTER_HOME environment variable to {enable_telemetry} telemetry".format(
                enable_telemetry="enable" if enable_telemetry else "disable"
            )
        )
        return

    instance_config_path = os.path.join(dagster_home_path, DAGSTER_CONFIG_YAML_FILENAME)

    if not os.path.exists(instance_config_path):
        with open(instance_config_path, 'w') as f:
            yaml.dump({TELEMETRY_STR: {ENABLED_STR: enable_telemetry}}, f)

    else:
        with open(instance_config_path, 'r') as f:
            instance_profile_json = yaml.load(f, Loader=yaml.FullLoader)
            if TELEMETRY_STR in instance_profile_json:
                instance_profile_json[TELEMETRY_STR][ENABLED_STR] = enable_telemetry
            else:
                instance_profile_json[TELEMETRY_STR] = {ENABLED_STR: enable_telemetry}

            with open(instance_config_path, 'w') as f:
                yaml.dump(instance_profile_json, f)
