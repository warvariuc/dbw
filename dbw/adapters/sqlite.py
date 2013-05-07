import os
import re
import math
from datetime import date as Date, datetime as DateTime, timedelta as TimeDelta
from decimal import Decimal

import dbw
from . import Column, GenericAdapter


FORMAT_QMARK_REGEX = re.compile(r'(?<!%)%s')


class SqliteAdapter(GenericAdapter):
    """Adapter for Sqlite databases.
    """
    scheme = 'sqlite'

    def _connect(self, db_path, **kwargs):
        import sqlite3

        self.driver = sqlite3
        # path_encoding = sys.getfilesystemencoding() or locale.getdefaultlocale()[1] or 'utf8'
        if db_path != ':memory:':
            if not os.path.isabs(db_path):
                # convert relative path to be absolute
                db_path = os.path.abspath(os.path.join(os.getcwd(), db_path))
            if not os.path.isfile(db_path):
                raise dbw.DbConnectionError(
                    '"%s" is not a file.\nFor a new database create an empty file.' % db_path)
            self.url = db_path

        return sqlite3.connect(db_path, **kwargs)

    def execute(self, query, *args):
        # convert '%s' to '?' as sqlite client expects
        query = FORMAT_QMARK_REGEX.sub('?', query).replace('%%', '%')
        return super().execute(query, *args)

    def _truncate(self, model, mode=''):
        assert dbw.is_model(model)
        table_name = str(model)
        return ['DELETE FROM %s;' % table_name,
                "DELETE FROM sqlite_sequence WHERE name='%s';" % table_name]

    def _get_create_table_indexes(self, model):
        assert dbw.is_model(model)
        indexes = []
        for index in model._meta.db_indexes:
            if index.type != 'primary':  # Sqlite has only primary indexes in the CREATE TABLE query
                continue
            index_type = 'PRIMARY KEY'
            columns = []
            for index_field in index.index_fields:
                column = index_field.field.column.name
                prefix_length = index_field.prefix_length
                if prefix_length:
                    column += '(%i)' % prefix_length
                sort_order = index_field.sort_order
                column += ' %s' % sort_order.upper()
                columns.append(column)

            indexes.append('%s (%s)' % (index_type, ', '.join(columns)))

        return indexes

    def _get_create_table_other(self, model):
        assert dbw.is_model(model)
        indexes = []
        for index in model._meta.db_indexes:
            if index.type == 'primary':  # Sqlite has only primary indexes in the CREATE TABLE query
                continue
            elif index.type == 'unique':
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
                # al fields are checked to have the same table, so take the first one
            model = index.index_fields[0].field.model
            indexes.append('CREATE %s "%s" ON "%s" (%s)'
                           % (index_type, index.name, model, ', '.join(columns)))

        return indexes

    def _declare_CHAR(self, column):
        column_str = 'TEXT'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        if column.comment:
            column_str += ' /* %s */' % column.comment
        return column_str

    def _declare_INT(self, column):
        """INTEGER column type for Sqlite.
        """
        max_int = int('9' * column.precision)
        bytes_count = math.ceil((max_int.bit_length() - 1) / 8)  # add one bit for sign
        if bytes_count > 8:
            raise Exception('Too many digits specified.')

        column_str = 'INTEGER'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        if column.comment:
            column_str += ' /* %s */' % column.comment
        return column_str

    def _declare_DATE(self, column):
        """Sqlite db does have native DATE data type.
        We store dates in it as integer number of days since the Epoch.
        """
        column_str = 'INTEGER'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        if column.comment:
            column_str += ' /* %s */' % column.comment
        return column_str

    def _encode_DATE(self, value, column):
        if isinstance(value, str):
            value = DateTime.strptime(value, '%Y-%m-%d').date()
        if isinstance(value, Date):
            return (value - self._epoch).days
        raise SyntaxError('Expected "YYYY-MM-DD" or datetime.date.')

    def _decode_DATE(self, value, column):
        return self._epoch + TimeDelta(days=value)

    def _declare_DECIMAL(self, column):
        """In Sqlite there is a special DECIMAL, which we won't use.
        We will store decimals as integers."""
        column_str = 'INTEGER'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not dbw.Nil:
            column_str += ' DEFAULT ' + self._render(column.default, column)
        if column.comment:
            column_str += ' /* %s */' % column.comment
        return column_str

    def _encode_DECIMAL(self, value, column):
        return int(Decimal(value) * (10 ** column.scale))

    def _decode_DECIMAL(self, value, column):
        return Decimal(value) / (10 ** column.scale)

    def get_tables(self):
        """Get list of tables (names) in this DB."""
        self.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in self.cursor.fetchall()]

    def get_columns(self, table_name):
        """Get columns of a table"""
        self.execute("PRAGMA table_info('%s')" % table_name)  # name, type, notnull, dflt_value, pk
        columns = {}
        for row in self.cursor.fetchall():
            dbw.logger.debug('Found table column: %s, %s', table_name, row)
            type_name = row[2].lower()
            # INTEGER PRIMARY KEY fields are auto-generated in sqlite
            # INT PRIMARY KEY is not the same as INTEGER PRIMARY KEY!
            autoincrement = bool(type_name == 'integer' and row[5])
            if 'int' in type_name or 'bool' in type_name:  # booleans are sotred as ints in sqlite
                type_name = 'int'
            elif type_name not in ('blob', 'text'):
                raise TypeError('Unexpected data type: %s' % type_name)
            column = Column(type=type_name, name=row[1], default=row[4],
                            precision=19, nullable=(not row[3]), autoincrement=autoincrement)
            columns[column.name] = column
            dbw.logger.debug('Reproduced table column: %s, %s', table_name, column)
        return columns


# alternative store format - using strings
#    def _DATE(self, **kwargs):
#        return 'TEXT'
#
#    def _encode_DATE(self, value, **kwargs):
#        if isinstance(value, str):
#            value = self.decodeDATE(value)
#        if isinstance(value, Date):
#            return value.strftime("'%Y-%m-%d'")
#        raise SyntaxError('Expected "YYYY-MM-DD" or datetime.date.')
#
#    def _decode_DATE(self, value, **kwargs):
#        return DateTime.strptime(value, '%Y-%m-%d').date()
#
#    def _DATETIME(self, **kwargs):
#        return 'TEXT'
#
#    def _encode_DATETIME(self, value, **kwargs):
#        if isinstance(value, DateTime):
#            return value.strftime("'%Y-%m-%d %H:%M:%S.%f'")
#        raise SyntaxError('Expected datetime.datetime.')
#
#    def _decode_DATETIME(self, value, **kwargs):
#        return DateTime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
#
#    def _DECIMAL(self, **kwargs):
#        return 'TEXT'
#
#    def _encode_DECIMAL(self, value, max_digits, fractionDigits, **kwargs):
#        _format = "'%% %i.%if'" % (max_digits + 1, fractionDigits)
#        return _format % Decimal(value)
#
#    def _decode_DECIMAL(self, value, **kwargs):
#        return Decimal(str(value))
