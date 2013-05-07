import re
import math
from datetime import datetime as DateTime

import dbw
from . import Column, GenericAdapter


class PostgreSqlAdapter(GenericAdapter):
    """Adapter for PostgreSql databases.
    """
    scheme = 'postgresql'

    def _connect(self, url, **kwargs):
        import psycopg2
        self.driver = psycopg2

        match = re.match('^(?P<user>[^:@]+)(:(?P<password>[^@]*))?@(?P<host>[^:/]+)'
                         '(:(?P<port>[0-9]+))?/(?P<db>[^?]+)$', url)
        assert match, "Invalid database URL format: %s" % self.url
        kwargs['user'] = match.group('user')
        assert kwargs['user'], 'User required'
        kwargs['password'] = match.group('password') or ''
        kwargs['host'] = match.group('host')
        assert kwargs['host'], 'Host name required'
        kwargs['database'] = match.group('db')
        assert kwargs['database'], 'Database name required'
        kwargs['port'] = int(match.group('port') or 5432)
        self.driver_args = kwargs

        connection = psycopg2.connect(**self.driver_args)
        connection.set_client_encoding('UTF8')
#        connection.execute('SET FOREIGN_KEY_CHECKS=1;')
#        connection.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")
        return connection

    def _declare_INT(self, column, int_map=((2, 'SMALLINT'), (4, 'INTEGER'), (8, 'BIGINT'))):
        """Render declaration of INT column type.
        """
        max_int = int('9' * column.precision)
        bytes_count = math.ceil((max_int.bit_length() - 1) / 8)  # add one bit for sign
        for _bytes_count, _column_type in int_map:
            if bytes_count <= _bytes_count:
                break
        else:
            raise Exception('Too big precision specified.')
        column_str = _column_type

        if column.autoincrement:
            if column_str == 'BIGINT':
                column_str = 'BIGSERIAL'
            else:
                column_str = 'SERIAL'
        else:
            if not column.nullable:
                column_str += ' NOT'
            column_str += ' NULL'
            if column.default is not dbw.Nil:
                column_str += ' DEFAULT ' + self._render(column.default, column)

        return column_str

    def _declare_BOOL(self, column):
        """Render declaration of BOOLEAN column type.
        """
        column_str = 'BOOLEAN'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        return column_str

    def _encode_BOOL(self, value, column):
        """Encode a value for insertion in a column of INT type.
        """
        return bool(int(value))

    def _decode_BOOL(self, value, column):
        """Decode a value from the DB to a value good for the corresponding model field.
        """
        return bool(int(value))

    def _declare_DATETIME(self, column):
        column_str = 'timestamp (6) without time zone'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        return column_str

    def _encode_DATETIME(self, value, column):
        if isinstance(value, str):
            return DateTime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        if isinstance(value, DateTime):
            return value.strftime("'%Y-%m-%d %H:%M:%S.%f'")
        raise SyntaxError('Expected datetime.datetime')

    def _decode_DATETIME(self, value, column):
        return value

    def _get_create_table_indexes(self, model):
        assert dbw.is_model(model)
        indexes = []
        for index in model._meta.db_indexes:
            if index.type.lower() != 'primary':  # only primary index in the CREATE TABLE query
                continue
            index_type = 'PRIMARY KEY'
            columns = []
            for index_field in index.index_fields:
                column = index_field.field.column.name
                prefix_length = index_field.prefix_length
                if prefix_length:
                    column += '(%i)' % prefix_length
#                sort_order = index_field.sort_order
#                column += ' %s' % sort_order.upper()
                columns.append(column)

            indexes.append('%s (%s)' % (index_type, ', '.join(columns)))

        return indexes

    def _get_create_table_other(self, model):
        assert dbw.is_model(model)
        queries = []
        for index in model._meta.db_indexes:
            if index.type.lower() == 'primary':  # primary index is in the CREATE TABLE query
                continue
            elif index.type.lower() == 'unique':
                index_type = 'UNIQUE INDEX'
            else:
                index_type = 'INDEX'
            columns = []
            for index_field in index.index_fields:
                column = index_field.field.column.name
#                prefix_length = index.prefix_lengths[i]
#                if prefix_length:
#                    column += '(%i)' % prefix_length
                sort_order = index_field.sort_order
                column += ' %s' % sort_order.upper()
                columns.append(column)
                # all fields are checked to have the same table, so take the first one
            model = index.index_fields[0].field.model
            queries.append('CREATE %s %s ON %s (%s)'
                           % (index_type, index.name, model, ', '.join(columns)))

        for field in model._meta.fields.values():
            column = field.column
            if column is not None and column.comment:
                queries.append(
                    "COMMENT ON COLUMN %s.%s IS %s"
                    % (model._meta.db_name, column.name, self.escape(column.comment)))

        return queries

    def get_tables(self):
        """Get list of tables (names) in this DB.
        """
        cursor = self.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table_name):
        """Get columns of a table
        """
        rows = self.select(
            'column_name', 'data_type', 'column_default', 'is_nullable', 'character_maximum_length',
            'numeric_precision', 'numeric_scale',
            from_='information_schema.columns',
            where={'table_schema': 'public', 'table_name': table_name},
        )

        columns = {}
        for row in rows.dictresult():
            type_name = row['data_type'].lower()
            if 'int' in type_name:
                type_name = 'INT'
            elif type_name == 'boolean':
                type_name = 'BOOL'
            elif 'char' in type_name:
                type_name = 'CHAR'
            elif type_name == 'timestamp without time zone':
                type_name = 'DATETIME'
            elif type_name == 'numeric':
                type_name = 'DECIMAL'
            elif type_name not in ('text', 'date'):
                raise Exception('Unexpected data type: `%s`' % type_name)
            precision = row['character_maximum_length'] or row['numeric_precision']
            nullable = row['is_nullable'].lower() == 'yes'
            default = row['column_default']
            if isinstance(default, str) and default.lower().startswith('nextval('):
                autoincrement = True
            else:
                autoincrement = False
            # TODO: retrieve column comment
            column = Column(type=type_name, name=row['column_name'],
                            default=default,
                            precision=precision, scale=row['numeric_scale'],
                            nullable=nullable, autoincrement=autoincrement)
            columns[column.name] = column
        return columns

    def insert(self, *fields):
        """Overriden to add `RETURNING id`.
        """
        query = self._insert(*fields) + ' RETURNING id'
        cursor = self.execute(query)
        return cursor.fetchone()[0]

    def _drop_table(self, table_name):
        """Return query for dropping a table.
        @param table_name: table name or a model describing the table
        """
        if dbw.is_model(table_name):
            table_name = table_name._meta.db_name
        if not isinstance(table_name, str):
            raise AssertionError('Expecting a str or a Model')
        return 'DROP TABLE IF EXISTS %s' % table_name
