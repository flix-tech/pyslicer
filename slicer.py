import _mysql
import sys

class SlicingMachine:
    registry = None
    read_connector = None
    write_connector = None
    cupboard = None
    __CHUNK_SIZE = 20000

    def __init__(self, registry, read_connector, write_connector, cupboard):
        self.registry = registry
        self.read_connector = read_connector
        self.write_connector = write_connector
        self.cupboard = cupboard

    def slice_table(self, table):
        print('Start table:', table)
        read_connection = self.read_connector()
        write_connection = self.write_connector()
        reader = self.registry.get_table_reader(table)
        writer = self.__create_table_writer(table, write_connection)

        try:
            reader.set_connection(read_connection)
            references = set()
            for records in reader.fetch_data(self.__CHUNK_SIZE):
                record_keys = list()
                for record in records:
                    record_keys.append(record['primary_key'])
                    for ref_table, primary_key in record['references'].items():
                        if primary_key:
                            references.add((ref_table, primary_key,))
                self.cupboard.put_on_shelf(table, *record_keys)
                writer.persist(*records)
            print('Commit table:', table)
            writer.commit()
            self.cupboard.put_on_reference_shelf(references)
        except:
            print('Error when copying table "%s"' % (table,))
            print(sys.exc_info()[1])
            if read_connection.errno():
                print(read_connection.error())
            self.cupboard.clear_shelf(table)
            writer.rollback()

        read_connection.close()
        write_connection.close()

    def persist_references(self):
        read_connection = self.read_connector()
        write_connection = self.write_connector()
        references = set()

        while self.cupboard.has_pending_references():
            print('Iteration over references')
            for table, record_keys in self.cupboard.get_all_references():
                offset = 0;
                chunk_size = 5000
                table = table.decode('utf-8')
                reader = self.registry.get_table_reader(table)
                writer = self.__create_table_writer(table, write_connection)
                record_keys = list(record_keys)
                reader.set_connection(read_connection)

                try:
                    while offset < len(record_keys):
                        keys = record_keys[offset:offset+chunk_size]
                        records = reader.get_records(*keys)
                        offset += chunk_size
                        new_record_keys = list()
                        for record in records:
                            new_record_keys.append(record['primary_key'])
                            for ref_table, primary_key in record['references'].items():
                                if primary_key:
                                    references.add((ref_table, primary_key,))
                        self.cupboard.put_on_shelf(table, *new_record_keys)
                        writer.persist(*records, ignore_duplicates = True)
                    print('References for table "' + table + '":', len(record_keys))
                    writer.commit()
                    self.cupboard.put_on_reference_shelf(references)
                except:
                    print('Error when copying references to "%s"' % (table,))
                    print(sys.exc_info()[1])
                    if read_connection.errno():
                        print(read_connection.error())
                    self.cupboard.clear_shelf(table)
                    writer.rollback()

        read_connection.close()
        write_connection.close()

    def __create_table_writer(self, table, connection):
        fields = self.registry.metadata[table]['fields']['__order__']
        return TableWriter(table, fields, connection)

class TableWriter:
    def __init__(self, table, fields, connection):
        self.table = table
        self.fields = fields
        self.connection = connection
        cursor = connection.cursor()
        cursor.execute('SET FOREIGN_KEY_CHECKS=0')
        cursor.close()
        connection.autocommit(False)

    def persist(self, *records, **opts):
        fields = ['`%s`' % (field,) for field in self.fields]
        if 'ignore_duplicates' in opts and opts['ignore_duplicates']:
            sql = 'INSERT IGNORE INTO `%s` (%s) VALUES' % (self.table, ', '.join(fields),)
        else:
            sql = 'INSERT INTO `%s` (%s) VALUES' % (self.table, ', '.join(fields),)
        offset = 0
        chunk_size = 500
        while offset < len(records):
            complete_sql = sql
            for record in records[offset:offset+chunk_size]:
                complete_sql += '\n' + record['record'] + ','
            offset += chunk_size
            self.connection.query(complete_sql[:-1])

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

def cleanup(connection, tables: set):
    assert len(tables) > 0, 'Empty table list'

    cursor = connection.cursor()
    cursor.execute('SET FOREIGN_KEY_CHECKS=0')

    for table in tables:
        if cursor.execute('SHOW TABLES LIKE %s', (_mysql.escape_string(table),)):
            cursor.execute('TRUNCATE TABLE `%s`' % (table,))

    cursor.close()
