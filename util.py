from getopt import getopt,GetoptError
import MySQLdb
import yaml

def usage():
    print('''Slicer v0.1:

    -h, --help            - shows help information
    -r, --read ...        - specify name of read connection
    -w, --write ...       - specify name of write connection
    -c, --connections     - list available connections
    -s, --start-over      - clean up mysql and redis before start
    -t, --tables ...      - comma separated list of tables
    ''')

def resolve_settings(argv):
    read_connection = None
    write_connection = None
    cleanup = True
    table_list = []

    try:
        opts, args = getopt(
            argv,
            'hr:w:ct:',
            ['help', 'read=', 'write=', 'connections', 'tables=']
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

    if not read_connection:
        raise RuntimeError("Read connection name is not specified")
    if not write_connection:
        raise RuntimeError("Write connection name is not specified")

    return {
        "read": read_connection,
        "write": write_connection,
        "cleanup": cleanup,
        "tables": table_list,
    }

def get_connection_parameters(connection_name):
    with open('config.yml') as config_file:
        config = yaml.load(config_file)

    assert connection_name in config["connection"], "Read connection settings not found"

    return config["connection"][connection_name]

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
