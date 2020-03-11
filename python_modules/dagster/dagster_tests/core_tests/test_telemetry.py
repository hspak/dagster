import datetime
import time

from dagster import seven
from dagster.core.telemetry import (
    _get_instance_id,
    execute_disable_telemetry,
    execute_enable_telemetry,
    execute_reset_telemetry_profile,
    log_action,
)
from dagster.core.test_utils import environ
from dagster.seven import mock


def mock_uuid():
    return 'some_random_uuid'


@mock.patch(seven.builtin_print())
@mock.patch('uuid.uuid4', mock_uuid)
def test_reset_telemetry_profile(mocked_print):
    with environ({'DAGSTER_TELEMETRY_ENABLED': 'True', 'DAGSTER_HOME': ''}):
        open_mock = mock.mock_open()
        with mock.patch('dagster.core.telemetry.open', open_mock, create=True):
            execute_reset_telemetry_profile()
            open_mock.assert_not_called()

            assert mocked_print.mock_calls == seven.print_single_line_str(
                'Must set $DAGSTER_HOME environment variable to reset profile'
            )

            with environ({'DAGSTER_HOME': '/dagster/home/path/'}):
                execute_reset_telemetry_profile()
                open_mock.assert_called_with('/dagster/home/path/dagster.yaml', 'w')

                open_mock.return_value.write.assert_has_calls(
                    [
                        mock.call('telemetry'),
                        mock.call(':'),
                        mock.call('\n'),
                        mock.call('  '),
                        mock.call('instance_id'),
                        mock.call(':'),
                        mock.call(' '),
                        mock.call('some_random_uuid'),
                        mock.call('\n'),
                    ]
                )


@mock.patch(seven.builtin_print())
def test_enable_telemetry(mocked_print):
    with environ({'DAGSTER_TELEMETRY_ENABLED': 'True', 'DAGSTER_HOME': ''}):
        open_mock = mock.mock_open()
        with mock.patch('dagster.core.telemetry.open', open_mock, create=True):
            execute_enable_telemetry()
            open_mock.assert_not_called()
            assert mocked_print.mock_calls == seven.print_single_line_str(
                'Must set $DAGSTER_HOME environment variable to enable telemetry'
            )
            with environ({'DAGSTER_HOME': '/dagster/home/path/'}):
                execute_enable_telemetry()
                open_mock.assert_called_with('/dagster/home/path/dagster.yaml', 'w')
                open_mock.return_value.write.assert_has_calls(
                    [
                        mock.call('telemetry'),
                        mock.call(':'),
                        mock.call('\n'),
                        mock.call('  '),
                        mock.call('enabled'),
                        mock.call(':'),
                        mock.call(' '),
                        mock.call('true'),
                        mock.call('\n'),
                    ]
                )


@mock.patch(seven.builtin_print())
def test_disable_telemetry(mocked_print):
    with environ({'DAGSTER_TELEMETRY_ENABLED': 'True'}):
        open_mock = mock.mock_open()
        with mock.patch('dagster.core.telemetry.open', open_mock, create=True):
            execute_disable_telemetry()
            open_mock.assert_not_called()
            assert mocked_print.mock_calls == seven.print_single_line_str(
                'Must set $DAGSTER_HOME environment variable to disable telemetry'
            )

            with environ({'DAGSTER_HOME': '/dagster/home/path/'}):
                execute_disable_telemetry()
                open_mock.assert_called_with('/dagster/home/path/dagster.yaml', 'w')
                open_mock.return_value.write.assert_has_calls(
                    [
                        mock.call('telemetry'),
                        mock.call(':'),
                        mock.call('\n'),
                        mock.call('  '),
                        mock.call('enabled'),
                        mock.call(':'),
                        mock.call(' '),
                        mock.call('false'),
                        mock.call('\n'),
                    ]
                )


@mock.patch('requests.post')
def test_telemetry_disabled(requests_mock):
    with environ({'DAGSTER_TELEMETRY_ENABLED': 'True'}):
        with seven.TemporaryDirectory() as tmpdir_path:
            with environ({'DAGSTER_HOME': tmpdir_path}):
                execute_disable_telemetry()
                log_action(action='did something', client_time=datetime.datetime.now(), metadata={})
                time.sleep(0.5)
                requests_mock.assert_not_called()


@mock.patch('requests.post')
def test_dagster_telemetry_enabled(requests_mock):
    with environ({'DAGSTER_TELEMETRY_ENABLED': 'True'}):
        with seven.TemporaryDirectory() as tmpdir_path:
            with environ({'DAGSTER_HOME': tmpdir_path}):
                execute_enable_telemetry()
                (instance_id, dagster_telemetry_enabled) = _get_instance_id()
                assert dagster_telemetry_enabled == True
                log_action(action='did something', client_time=datetime.datetime.now(), metadata={})
                time.sleep(0.5)
                _, kwargs = requests_mock.call_args_list[0]
                requests_mock.assert_called_once_with(
                    'https://telemetry.elementl.dev/actions',
                    data={
                        'instance_id': instance_id,
                        'action': 'did something',
                        'client_time': kwargs['data']['client_time'],  # Ignore client_time in test
                        'elapsed_time': None,
                        'metadata': {},
                    },
                )
