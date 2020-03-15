import pyarrow as pa

from dagster import Field, Noneable, Permissive, resource


class HDFSResource(object):
    def __init__(self, host, port, user, kerb_ticket, driver, extra_conf):
        self.fs = pa.hdfs.connect(
            host=host,
            port=port,
            user=user,
            kerb_ticket=kerb_ticket,
            driver=driver,
            extra_conf=extra_conf,
        )


@resource(
    {
        'host': Field(
            str,
            is_required=False,
            default_value='default',
            description='NameNode. Set to "default" for fs.defaultFS from core-site.xml.',
        ),
        'port': Field(
            int,
            is_required=False,
            default_value=0,
            description='NameNode\'s port. Set to 0 for default or logical (HA) nodes.',
        ),
        'user': Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description='Username when connecting to HDFS; None implies login user.',
        ),
        'kerb_ticket': Field(
            Noneable(str),
            is_required=False,
            default_value=None,
            description='Path to Kerberos ticket cache.',
        ),
        'driver': Field(
            str,
            is_required=False,
            default_value='libhdfs',
            description='''{'libhdfs', 'libhdfs3'}, default 'libhdfs'
    Connect using libhdfs (JNI-based) or libhdfs3 (3rd-party C++ library from Apache HAWQ
    (incubating)).
      ''',
        ),
        'extra_conf': Field(
            Noneable(Permissive()),
            is_required=False,
            default_value=None,
            description='dict, default None. extra Key/Value pairs for config; Will override any '
            'hdfs-site.xml properties.',
        ),
    }
)
def hdfs_resource(init_context):
    '''Connect to an HDFS cluster. All parameters are optional and should only be set if the
    defaults need to be overridden.

    Authentication should be automatic if the HDFS cluster uses Kerberos. However, if a username is
    specified, then the ticket cache will likely be required.
    '''
    return HDFSResource(**init_context.resource_config)
