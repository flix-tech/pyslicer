import sys
import datetime
from concurrent.futures import ThreadPoolExecutor
from random import randint
from config import Configuration
from datasource import DataRegistry
from slicer import SlicingMachine,cleanup
from cupboard import RedisCupboard
from util import resolve_settings, \
                 get_connection_factory, \
                 copy_database_schema

if __name__ == '__main__':
    settings = resolve_settings(sys.argv[1:])
    configuration = Configuration()

    mysql_params = [configuration.get_mysql_parameters(connection_name) for connection_name in settings['read'].split(',')];
    read_connection_params = mysql_params[0];
    read_connection_creator = get_connection_factory(*mysql_params)

    write_connection_params = configuration.get_mysql_parameters(settings['write'])
    write_connection_creator = get_connection_factory(write_connection_params)

    read_connection = read_connection_creator()
    data_registry = DataRegistry(
        read_connection_params['database'],
        read_connection
    )
    slicing_machine = SlicingMachine(
        data_registry,
        read_connection_creator,
        write_connection_creator,
        RedisCupboard(settings['cleanup'], **configuration.get_redis_parameters())
    )

    if len(settings['tables']):
        diff = set(settings['tables']) - data_registry.tables
        assert len(diff) == 0, 'Unknow tables provided: %s' % (', '.join(diff))
        table_list = settings['tables']
    else:
        table_list = data_registry.tables

    write_connection = write_connection_creator()

    # copying schema imply recreating write database
    if settings['copy_schema']:
        copy_database_schema(data_registry, write_connection)
    # in case schema is not copied existing tables will be truncated
    # TODO check whether ALL tables should be trucated when '--tables' specified
    elif settings['cleanup']:
        cleanup(write_connection, data_registry.tables)

    write_connection.close()
    read_connection.close()

    started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    with ThreadPoolExecutor(max_workers=configuration.get_max_workers()) as executor:
        executor.map(slicing_machine.slice_table, table_list)

    slicing_machine.persist_references()

    print('started copying data at', started_at)
    print('completed at', datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
