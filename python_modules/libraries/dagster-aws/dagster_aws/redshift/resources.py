import abc
import sys
from contextlib import contextmanager

import psycopg2
import psycopg2.extensions
import six

from dagster import Field, StringSource, check, resource


class RedshiftError(Exception):
    pass


class BaseRedshiftResource(six.with_metaclass(abc.ABCMeta)):
    def __init__(self, context):  # pylint: disable=too-many-locals
        # Extract parameters from resource config
        self.conn_args = {
            k: context.resource_config.get(k)
            for k in (
                'host',
                'port',
                'user',
                'password',
                'database',
                'schema',
                'connect_timeout',
                'sslmode',
            )
            if context.resource_config.get(k) is not None
        }

        self.autocommit = context.resource_config.get('autocommit', False)
        self.iam_role = context.resource_config.get('iam_role', None)
        self.staging_bucket = context.resource_config.get('staging_bucket', None)
        self.log = context.log_manager

    @abc.abstractmethod
    def execute_query(self, query, fetch_results=False, cursor_factory=None):
        pass

    @abc.abstractmethod
    def execute_queries(self, queries, fetch_results=False, cursor_factory=None):
        pass


class RedshiftResource(BaseRedshiftResource):
    def execute_query(self, query, fetch_results=False, cursor_factory=None):
        '''Synchronously execute a single query against Redshift. Will return a list of rows, where
        each row is a tuple of values, e.g. SELECT 1 will return [(1,)].

        Args:
            query (str): The query to execute.
            fetch_results (bool, optional): Whether to return the results of executing the query.
                Defaults to False, in which case the query will be executed without retrieving the
                results.
            cursor_factory (psycopg2.extensions.cursor, optional): An alternative cursor_factory;
                defaults to None. Will be used when constructing the cursor. See the psycopg2
                documentation for more details:
                    https://www.psycopg.org/docs/extras.html?highlight=range#psycopg2.extras.DictCursor

        Returns:
            List[Tuple[*]], optional: Results of the query, as a list of tuples, when fetch_results
                is set. Otherwise return None.
        '''
        check.str_param(query, 'query')
        check.bool_param(fetch_results, 'fetch_results')
        check.opt_subclass_param(cursor_factory, 'cursor_factory', psycopg2.extensions.cursor)

        with self._get_cursor(cursor_factory=cursor_factory) as cursor:
            if sys.version_info[0] < 3:
                query = query.encode('utf-8')

            self.log.info('[redshift] Executing query \'{query}\''.format(query=query))
            cursor.execute(query)

            if fetch_results and cursor.rowcount > 0:
                return cursor.fetchall()
            else:
                self.log.info('[redshift] Empty result from query')

    def execute_queries(self, queries, fetch_results=False, cursor_factory=None):
        '''Synchronously execute a list of queries against Redshift. Will return a list of list of
        rows, where each row is a tuple of values, e.g. ['SELECT 1', 'SELECT 1'] will return
        [[(1,)], [(1,)]].

        Args:
            queries (List[str]): The queries to execute.
            fetch_results (bool, optional): Whether to return the results of executing the query.
                Defaults to False, in which case the query will be executed without retrieving the
                results.
            cursor_factory (psycopg2.extensions.cursor, optional): An alternative cursor_factory;
                defaults to None. Will be used when constructing the cursor. See the psycopg2
                documentation for more details:
                    https://www.psycopg.org/docs/extras.html?highlight=range#psycopg2.extras.DictCursor

        Returns:
            List[List[Tuple[*]]], optional: Results of the query, as a list of list of tuples, when
                fetch_results is set. Otherwise return None.
        '''
        check.list_param(queries, 'queries', of_type=str)
        check.bool_param(fetch_results, 'fetch_results')
        check.opt_subclass_param(cursor_factory, 'cursor_factory', psycopg2.extensions.cursor)

        results = []
        with self._get_cursor(cursor_factory=cursor_factory) as cursor:
            for query in queries:
                if sys.version_info[0] < 3:
                    query = query.encode('utf-8')

                self.log.info('[redshift] Executing query \'{query}\''.format(query=query))
                cursor.execute(query)

                if fetch_results and cursor.rowcount > 0:
                    results.append(cursor.fetchall())
                else:
                    self.log.info('[redshift] Empty result from query')

        if fetch_results:
            return results

    @contextmanager
    def _get_cursor(self, cursor_factory=None):
        check.opt_subclass_param(cursor_factory, 'cursor_factory', psycopg2.extensions.cursor)

        conn = psycopg2.connect(**self.conn_args)
        if self.autocommit:
            conn.autocommit = True
        with conn:
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor
            if not self.autocommit:
                conn.commit()
        conn.close()


class FakeRedshiftResource(BaseRedshiftResource):
    QUERY_RESULT = [(1,)]

    def execute_query(self, query, fetch_results=False, cursor_factory=None):
        check.str_param(query, 'query')
        check.bool_param(fetch_results, 'fetch_results')

        self.log.info('[fake-redshift] Executing query \'{query}\''.format(query=query))
        if fetch_results:
            return self.QUERY_RESULT

    def execute_queries(self, queries, fetch_results=False, cursor_factory=None):
        check.list_param(queries, 'queries', of_type=str)
        check.bool_param(fetch_results, 'fetch_results')

        for query in queries:
            self.log.info('[fake-redshift] Executing query \'{query}\''.format(query=query))
        if fetch_results:
            return [self.QUERY_RESULT] * 3


def define_redshift_config():
    '''Redshift configuration. See the Redshift documentation for reference:

    https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-to-cluster.html
    '''

    return {
        'host': Field(StringSource, description='Redshift host', is_required=True),
        'port': Field(int, description='Redshift port', is_required=False, default_value=5439),
        'user': Field(
            StringSource, description='Username for Redshift connection', is_required=False,
        ),
        'password': Field(
            StringSource, description='Password for Redshift connection', is_required=False,
        ),
        'database': Field(
            StringSource,
            description='Name of the default database to use. After login, you can use USE DATABASE'
            ' to change the database.',
            is_required=False,
        ),
        'schema': Field(
            str,
            description='Name of the default schema to use. After login, you can use USE SCHEMA to '
            'change the schema.',
            is_required=False,
        ),
        'autocommit': Field(
            bool,
            description='None by default, which honors the Redshift parameter AUTOCOMMIT. Set to '
            'True or False to enable or disable autocommit mode in the session, respectively.',
            is_required=False,
        ),
        'connect_timeout': Field(
            int,
            description='Connection timeout in seconds. 60 seconds by default',
            is_required=False,
            default_value=5,
        ),
        'sslmode': Field(
            str, description='SSL mode to use', is_required=False, default_value='require',
        ),
    }


@resource(
    config=define_redshift_config(),
    description='Resource for connecting to the Redshift data warehouse',
)
def redshift_resource(context):
    '''This resource enables connecting to a Redshift cluster and issuing queries against that
    cluster.

    Example:
        from dagster import ModeDefinition, execute_solid, solid
        from dagster_aws.redshift import redshift_resource

        @solid(required_resource_keys={'redshift'})
        def example_redshift_solid(context):
            return context.resources.redshift.execute_query('SELECT 1', fetch_results=True)

        result = execute_solid(
            example_redshift_solid,
            environment_dict={
                'resources': {
                    'redshift': {
                        'config': {
                            'host': 'my-redshift-cluster.us-east-1.redshift.amazonaws.com',
                            'port': 5439,
                            'user': 'dagster',
                            'password': 'dagster',
                            'database': 'dev',
                        }
                    }
                }
            },
            mode_def=ModeDefinition(resource_defs={'redshift': redshift_resource}),
        )
        assert result.output_value() == [(1,)]

    '''
    return RedshiftResource(context)


@resource(
    config=define_redshift_config(),
    description='Fake resource for connecting to the Redshift data warehouse. Usage is identical '
    'to the real redshift_resource. Will always return [(1,)] for the single query case and '
    '[[(1,)], [(1,)], [(1,)]] for the multi query case.',
)
def fake_redshift_resource(context):
    return FakeRedshiftResource(context)
