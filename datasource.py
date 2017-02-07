import MySQLdb
import yaml
import re
from copy import deepcopy
from os.path import isfile

class DataRegistry:
    tables = None
    metadata = None
    routines = None

    def __init__(self, db_name, schema_file, connection):
        self.tables = self.__load_table_list(connection)
        self.metadata = dict(zip(self.tables, [
            {
                'fields': self.__load_fields(connection, db_name, table),
                'refs': self.__load_references(connection, db_name, table),
                'create_sql': self.__table_create_sql(connection, table),
            } for table in self.tables
        ]))
        self.routines = self.__load_routines(connection, db_name)
        self.__schema_configuration = self.__load_schema_configuration(schema_file)
        self.__table_readers = dict()

    def __load_schema_configuration(self, filename):
        assert isfile(filename), 'Schema file \'%s\' is missing.' % (filename,)

        with open(filename) as schema_data:
            configuration = yaml.load(schema_data)

        assert 'tables' in configuration, 'Table list is missing in schema configuration'

        return configuration

    def get_create_table(self, table):
        assert table in self.metadata, 'Unknown table'

        return self.metadata[table]['create_sql']

    def __table_create_sql(self, connection, table):
        cursor = connection.cursor()
        result = cursor.execute('SHOW CREATE TABLE `%s`' % (table,))

        if result:
            create_sql = cursor.fetchone()[1]
        else:
            raise RuntimeError(connection.error())

        cursor.close()

        return create_sql

    def get_table_reader(self, table):
        if table in self.__table_readers:
            return self.__table_readers[table]

        tables = self.__schema_configuration['tables']
        rules = self.__schema_configuration['rules']

        assert 'rule' in tables[table], 'No rule provided for table %s' % (table,)

        rule = tables[table]['rule']
        mask = tables[table]['mask'] if 'mask' in tables[table] else {}

        assert isinstance(mask, dict), 'Invalid type of "mask" parameter'

        if rule == 'upon_request':
            reader = GenericReader(
                table,
                self.metadata[table]['fields'],
                self.metadata[table]['refs'],
                mask
            )
        elif rule == 'join':
            assert tables[table]['table'] in self.metadata, \
                'Can\'t join table %s with unknown table %s' % (table, tables[table]['table'],)
            reader = JoinReader(
                table,
                self.metadata[table]['fields'],
                self.metadata[table]['refs'],
                mask,
                tables[table]['reference'],
                self.get_table_reader(tables[table]['table']).sql_query(),
                self.metadata[tables[table]['table']]['fields']
            )
        else:
            assert rule in rules, 'Unkown rule %s' % (rule,)
            reader = ConditionReader(
                table,
                self.metadata[table]['fields'],
                self.metadata[table]['refs'],
                mask,
                rules[rule]['where'].replace('%table_name%', '`' + table + '`')
            )

        self.__table_readers[table] = reader

        return reader

    def __load_routines(self, connection, database):
        query = """
            SELECT ROUTINE_NAME, ROUTINE_TYPE
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = %s
        """
        cursor = connection.cursor()
        result = cursor.execute(query, (database,))
        routines = list()

        for routine_row in cursor.fetchall():
            routine_name, routine_type = routine_row
            cursor.execute('SHOW CREATE %s `%s`' % (routine_type, routine_name))
            create_sql = cursor.fetchone()[2]
            create_sql = re.sub('DEFINER=[^\s]+\s', '', create_sql)
            routines.append(Routine(routine_name, routine_type, create_sql))

        cursor.close()

        return routines


    def __load_table_list(self, connection):
        query = 'SHOW TABLES'
        cursor = connection.cursor()
        result = cursor.execute(query)
        tables = set()

        for row in cursor:
            tables.add(row[0])

        cursor.close()

        return tables

    def __load_references(self, connection, database, table):
            query = """
                SELECT t.REFERENCED_TABLE_NAME, t.REFERENCED_COLUMN_NAME, t.COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE t
                WHERE t.TABLE_SCHEMA = %s
                    AND t.TABLE_NAME = %s
                    AND t.REFERENCED_TABLE_NAME IS NOT NULL
            """
            cursor = connection.cursor()
            result = cursor.execute(query, (database, table,))
            data = cursor.fetchall()
            cursor.close()
            references = dict()

            if (result):
                for (ref_table_name, ref_table_field, foreign_key) in data:
                    references[foreign_key] = (ref_table_name, ref_table_field,)

            return references

    def __load_fields(self, connection, database, table):
        query = """
            SELECT c.COLUMN_NAME colName, c.COLUMN_KEY keyType, c.DATA_TYPE dataType, c.IS_NULLABLE nullable
            FROM information_schema.COLUMNS c
            WHERE c.TABLE_SCHEMA = %s
            AND c.TABLE_NAME = %s
            ORDER BY c.`ORDINAL_POSITION`
        """
        cursor = connection.cursor()
        result = cursor.execute(query, (database, table,))
        data = cursor.fetchall()
        cursor.close()

        if (result):
            fields = dict()
            fields['__primary__'] = ''
            fields['__order__'] = list()
            for (field_name, field_index, field_type, field_null) in data:
                if field_index == 'PRI':
                    fields['__primary__'] = field_name
                fields['__order__'].append(field_name)
                fields[field_name] = {
                    'name': field_name,
                    'index': field_index,
                    'type': field_type,
                    'null': field_null,
                }
            return fields
        else:
            raise ValueError("Table '{}' has no fields".format(table_name))

class Routine:
    def __init__(self, name, type, create_sql):
        self.name = name
        self.type = type
        self.create_sql = create_sql

class BaseReader:
    def __init__(self, table, fields, references, mask = None):
        self.table = table
        self.fields = fields
        self.references = references
        self.offset = 0
        self.mask = mask

    def set_connection(self, connection):
        self.connection = connection

    def fetch_data(self, chunk_size):
        assert self.connection, 'Cannot read table data without database connection'
        cursor = self.connection.cursor()
        sql = self.sql_query()
        sql += ' LIMIT %s, %s'
        offset = 0
        while cursor.execute(sql, (offset, chunk_size,)):
            offset += chunk_size
            yield [self.populate_record(record_data) for record_data in cursor.fetchall()]
        cursor.close()

    def get_records(self, *keys):
        assert self.connection, 'Cannot read table data without database connection'

        cursor = self.connection.cursor()
        fields = self.__combine_fields()
        sql = 'SELECT %s FROM `%s` WHERE `%s` IN (%s)' % \
            (fields, self.table, self.fields['__primary__'], ','.join([key.decode('utf-8') for key in keys]))

        cursor.execute(sql)
        result = [self.populate_record(record_data) for record_data in cursor.fetchall()]
        cursor.close()
        return result

    def sql_query(self):
        fields = self.__combine_fields()
        return 'SELECT %s FROM `%s`' % (fields, self.table,)

    def __combine_fields(self):
        if self.mask and len(self.mask):
            fields = []
            for field in self.fields['__order__']:
                if field in self.mask:
                    fields.append('%s as "%s"' % (self.mask[field], field,))
                else:
                    fields.append('`%s`' % field)
            fields = ','.join(fields)
        else:
            fields = ','.join(['`%s`' % field for field in self.fields['__order__']])
        return fields

    def populate_record(self, record_data):
        def __cast_number(value):
            return int(value)
        def __cast_decimal(value):
            return float(value)
        def __cast_string(value):
            return '"{}"'.format(str(value).replace('\\', '\\\\').replace('"', '\\"'))
        def __cast_datetime(value):
            return __cast_string(value) if value else '"0000-00-00 00:00:00"'
        def __cast_date(value):
            return __cast_string(value) if value else '"0000-00-00"'
        def __cast_time(value):
            return __cast_string(value) if value else '"00:00:00"'

        cast_map = {
            'int': __cast_number,
            'bigint': __cast_number,
            'smallint': __cast_number,
            'tinyint': __cast_number,
            'decimal': __cast_decimal,
            'double': __cast_decimal,
            'double': __cast_decimal,
            'char': __cast_string,
            'varchar': __cast_string,
            'text': __cast_string,
            'longtext': __cast_string,
            'date': __cast_date,
            'time': __cast_time,
            'datetime': __cast_datetime,
            'enum': __cast_string,
        }
        primary_key = ''
        record_key = self.table
        record = list()
        references = dict()

        for field, value in zip(self.fields['__order__'], record_data):
            if field == self.fields['__primary__']:
                primary_key = value
                record_key += ':' + str(value)
            if value == None and self.fields[field]['null'] == 'YES':
                quoted_value = 'NULL'
            else:
                cast_type = cast_map.get(self.fields[field]['type'], __cast_string)
                quoted_value = cast_type(value)
            record.append(str(quoted_value))
            if field in self.references:
                references[self.references[field][0]] = value

        return {
            'primary_key': primary_key,
            'record_key': record_key,
            'record': '(%s)' % (','.join(record),),
            'references': references,
        }

class GenericReader(BaseReader):
    def fetch_data(self, chunk_size):
        return []

class ConditionReader(BaseReader):
    def __init__(self, table, fields, references, mask, where):
        super().__init__(table, fields, references, mask)
        self.where = where

    def sql_query(self):
        sql = super().sql_query() + ' WHERE ' + self.where
        return sql

class JoinReader(BaseReader):
    def __init__(self, table, fields, references, mask, foreign_key, reference_sql, join_fields):
        super().__init__(table, fields, references, mask)
        self.reference_sql = reference_sql
        self.foreign_key = foreign_key
        self.join_fields = join_fields

    def __referenced_column(self):
        if self.foreign_key in self.references:
            return self.references[self.foreign_key][1]
        elif 'id' in self.join_fields:
            return 'id'
        else:
            return self.join_fields['__primary__']

    def fetch_data(self, chunk_size):
        assert self.connection, 'Cannot read table data without database connection'
        ref_cursor = self.connection.cursor()
        ref_sql = re.sub(
            'SELECT\s+.+\sFROM',
            'SELECT `%s` FROM' % (self.__referenced_column(),),
            self.reference_sql + ' LIMIT %s, %s'
        )
        ref_offset = 0
        ref_chunk_size = 20000
        cursor = self.connection.cursor()
        while ref_cursor.execute(ref_sql, (ref_offset, ref_chunk_size,)):
            ref_offset += ref_chunk_size
            ref_keys = [str(join_data[0]) for join_data in ref_cursor.fetchall()]
            sql = self.sql_query()
            sql += ' WHERE `%s`.`%s` IN (%s)' % (self.table, self.foreign_key, ','.join(ref_keys),)
            sql += ' LIMIT %s, %s'
            offset = 0
            while cursor.execute(sql, (offset, chunk_size,)):
                offset += chunk_size
                yield [self.populate_record(record_data) for record_data in cursor.fetchall()]
        cursor.close()
        ref_cursor.close()
