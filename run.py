import sys
from concurrent.futures import ThreadPoolExecutor
from random import randint
from datasource import DataRegistry
from slicer import SlicingMachine,cleanup
from cupboard import RedisCupboard
from util import resolve_settings,get_connection_factory,get_connection_parameters

if __name__ == '__main__':
    settings = resolve_settings(sys.argv[1:])
    read_connection_params = get_connection_parameters(settings['read']);
    read_connection_creator = get_connection_factory(read_connection_params)

    write_connection_params = get_connection_parameters(settings['write'])
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
        RedisCupboard(settings['cleanup'])
    )

    if len(settings['tables']):
        diff = set(settings['tables']) - data_registry.tables
        assert len(diff) == 0, 'Unknow tables provided: %s' % (', '.join(diff))
        table_list = settings['tables']
    else:
        table_list = data_registry.tables

    if settings['cleanup']:
        write_connection = write_connection_creator()
        cleanup(write_connection, data_registry.tables)

    read_connection.close()
    write_connection.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(slicing_machine.slice_table, table_list)

    slicing_machine.persist_references()
