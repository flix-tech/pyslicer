from getopt import getopt,GetoptError
import MySQLdb
import yaml
import subprocess

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

def get_connection_factory(parameters):
    _conn = {
        'host': parameters["host"],
        'port': parameters["port"],
        'user': parameters["user"],
        'passwd': parameters["password"] if parameters["password"] else "",
        'db': parameters["database"],
        'use_unicode': True,
        'charset': "utf8"
    }

    return lambda: MySQLdb.connect(**_conn)

def mysql_cmd_string(params: dict, cmd = 'mysql', select_db = False):
    return cmd + \
        ' -u"%s"' % (params['user'],) + \
        (' -p"%s"' % (params['password'],) if params['password'] else '') + \
        ' -h"%s"' % (params['host'],) + \
        ' -P"%s"' % (str(params['port']) if params['port'] else '3306',) + \
        ' ' + (params['database'] if select_db else '')

def copy_database_schema(read_connection_params, write_connection_params):
    mysqldump_cmd = mysql_cmd_string(read_connection_params, 'mysqldump', True) + \
        ' --skip-triggers' + \
        ' --routines' + \
        ' --no-data' + \
        ' --quick'
    mysql_drop_cmd = mysql_cmd_string(write_connection_params) + \
        '-e "DROP DATABASE IF EXISTS %s"' % (write_connection_params['database'],)
    mysql_create_cmd = mysql_cmd_string(write_connection_params) + \
        '-e "CREATE DATABASE %s COLLATE utf8_unicode_ci"' % (write_connection_params['database'],)
    mysql_copy_cmd = mysqldump_cmd + \
        ' | ' + mysql_cmd_string(write_connection_params, select_db = True)

    commands = [
        mysql_drop_cmd,
        mysql_create_cmd,
        mysql_copy_cmd,
    ]

    for cmd in commands:
        run = subprocess.Popen(cmd, shell=True, stdout=None)
        run.wait()
