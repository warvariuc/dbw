import re

import dbw
from . import Column, GenericAdapter


class MysqlAdapter(GenericAdapter):
    """Adapter for MySql databases.
    """
    scheme = 'mysql'

    def _connect(self, url, **kwargs):
        import pymysql
        self.driver = pymysql

        match = re.match('^(?P<user>[^:@]+)(:(?P<password>[^@]*))?@(?P<host>[^:/]+)'
                         '(:(?P<port>[0-9]+))?/(?P<db>[^?]+)$', url)
        if not match:
            raise dbw.AdapterError("Wrong database URL format: %s" % url)
        kwargs['user'] = match.group('user')
        assert kwargs['user'], 'User required'
        kwargs['passwd'] = match.group('password') or ''
        kwargs['host'] = match.group('host')
        assert kwargs['host'], 'Host name required'
        kwargs['db'] = match.group('db')
        assert kwargs['db'], 'Database name required'
        kwargs['port'] = int(match.group('port') or 3306)
        kwargs['charset'] = 'utf8'
        self.driver_args = kwargs

        connection = pymysql.connect(**kwargs)
        connection.execute('SET FOREIGN_KEY_CHECKS=1;')
        connection.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")
        return connection

    def _get_create_table_other(self, model):
        return ["ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='%s'" % model.__doc__]

    def _RANDOM(self):
        return 'RAND()'

    def _CONCAT(self, expressions):  # CONCAT(str1,str2,...)
        "Concatenate two or more expressions."
        rendered_expressions = []
        for expression in expressions:
            rendered_expressions.append('(' + self.render(expression) + ')')
        return 'CONCAT(' + ', '.join(rendered_expressions) + ')'

    def get_tables(self):
        """Get list of tables (names) in this DB."""
        self.execute("SHOW TABLES")
        return [row[0] for row in self.cursor.fetchall()]

    def get_columns(self, table_name):
        """Get columns of a table"""
        self.execute("SELECT column_name, data_type, column_default, is_nullable,"
                     "       character_maximum_length, numeric_precision, numeric_scale,"
                     "       column_type, extra, column_comment "
                     "FROM information_schema.columns "
                     "WHERE table_schema = '%s' AND table_name = '%s'"
                     % (self.driver_args['db'], table_name))
        columns = {}
        for row in self.cursor.fetchall():
            type_name = row[1].lower()
            if 'int' in type_name:
                type_name = 'int'
            elif 'char' in type_name:
                type_name = 'char'
            elif type_name not in ('text', 'datetime', 'date'):
                raise Exception('Unexpected data type: %s' % type_name)
            precision = row[4] or row[5]
            nullable = row[3].lower() == 'yes'
            autoincrement = 'auto_increment' in row[8].lower()
            unsigned = row[7].lower().endswith('unsigned')
            column = Column(type=type_name, field=None, name=row[0], default=row[2],
                            precision=precision, scale=row[6], unsigned=unsigned,
                            nullable=nullable, autoincrement=autoincrement, comment=row[9])
            columns[column.name] = column
        return columns
