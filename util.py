from getopt import getopt,GetoptError
import MySQLdb
import yaml
import subprocess
import itertools

def usage():
    print('''Slicer v0.1:

    -h, --help            - shows help information
    -r, --read ...        - specify name of read connection
    -w, --write ...       - specify name of write connection
    -c, --continue        - don't empty Redis db and don't truncate tables
    -t, --tables ...      - comma separated list of tables
        --copy-schema     - copies schema from source database
                            (target database will be deleted and recreated)
    ''')

def resolve_settings(argv):
    read_connection = None
    write_connection = None
    cleanup = True
    copy_schema = False
    table_list = []

    try:
        opts, args = getopt(
            argv,
            'hr:w:ct:',
            ['help', 'read=', 'write=', 'continue', 'tables=', 'copy-schema']
        )
    except GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-r", "--read"):
            read_connection = arg
        elif opt in ("-w", "--write"):
            write_connection = arg
        elif opt in ("-c", "--continue"):
            cleanup = False
        elif opt in ("-t", "--tables"):
            table_list = arg.split(",")
        elif opt in ("--copy-schema",):
            copy_schema = True

    if not read_connection:
        raise RuntimeError("Read connection name is not specified")
    if not write_connection:
        raise RuntimeError("Write connection name is not specified")

    return {
        "read": read_connection,
        "write": write_connection,
        "cleanup": cleanup,
        "tables": table_list,
        "copy_schema": copy_schema,
    }

def get_connection_factory(*parameter_sets):
    _conns = itertools.cycle([{
        'host': parameters["host"],
        'port': parameters["port"],
        'user': parameters["user"],
        'passwd': parameters["password"] if parameters["password"] else "",
        'db': parameters["database"],
        'use_unicode': True,
        'charset': "utf8"
    } for parameters in parameter_sets])

    return lambda: MySQLdb.connect(**next(_conns))

def mysql_cmd_string(params: dict, cmd = 'mysql', select_db = False):
    return cmd + \
        ' -u"%s"' % (params['user'],) + \
        (' -p"%s"' % (params['password'].replace('"', '\\"').replace('`', '\\`'),) if params['password'] else '') + \
        ' -h"%s"' % (params['host'],) + \
        ' -P"%s"' % (str(params['port']) if params['port'] else '3306',) + \
        ' ' + (params['database'] if select_db else '')

def copy_database_schema(data_registry, write_connection):
    write_connection.query('SET FOREIGN_KEY_CHECKS=0')

    for table in data_registry.tables:
        create_sql = data_registry.get_create_table(table)
        write_connection.query('DROP TABLE IF EXISTS `%s`' % (table,))
        write_connection.query(create_sql)

    for routine in data_registry.routines:
        if routine.type == 'PROCEDURE':
            write_connection.query('DROP PROCEDURE IF EXISTS `%s`' % (routine.name,))
        elif routine.type == 'FUNCTION':
            write_connection.query('DROP FUNCTION IF EXISTS `%s`' % (routine.name,))
        else:
            raise RuntimeError('Unknown routine type: %s' % (routine.type,))
        write_connection.query(routine.create_sql)
