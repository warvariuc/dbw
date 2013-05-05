"""
This module contains database adapters, which incapsulate all operations specific to a certain
database. All other ORM modules should be database agnostic.
"""
__author__ = "Victor Varvariuc <victor.varvariuc@gmail.com>"

import os
import base64
import time
import re
import math
from datetime import date as Date, datetime as DateTime, timedelta as TimeDelta
from decimal import Decimal
import pprint

import dbw
from dbw import Nil, logger, sql_logger


class Column():
    """Information about database table column.
    """
    def __init__(self, type, name, default=Nil, precision=None, scale=None, unsigned=None,
                 nullable=True, autoincrement=False, comment=''):
        self.name = name  # db table column name
        self.type = type  # string with the name of data type (decimal, varchar, bigint...)
        self.default = default  # column default value
        self.precision = precision  # char max length or decimal/int max digits
        self.scale = scale  # for decimals
        self.unsigned = unsigned  # for decimals, integers
#        assert nullable or default is not None or autoincrement, \
#            'Column `%s` is not nullable, but has no default value.' % self.name
        self.nullable = nullable  # can contain NULL values?
        self.autoincrement = autoincrement  # for primary integer
        self.comment = comment

    def __str__(self, db=None):
        db = db or dbw.generic_adapter
        assert isinstance(db, GenericAdapter), 'Expected a GenericAdapter instance'
        col_func = getattr(db, '_declare_' + self.type.upper())
        column_type = col_func(self)
        return '%s %s' % (self.name, column_type)

    def str(self):
        attrs = self.__dict__.copy()
        name = attrs.pop('name')
        return '%s(%s)' % (name, ', '.join('%s= %s' % attr for attr in attrs.items()))


# --------------------------------------------------------------------------------------------------

class GenericAdapter():
    """ A generic database adapter.
    """
    scheme = 'generic'
    driver = None
    # from this date number of days will be counted when storing DATE values in the DB
    _epoch = Date(1970, 1, 1)
    _MAX_QUERIES = 20  # how many queries to keep in log

    def __str__(self):
        return "'%s://%s'" % (self.scheme, self.url)

    def __init__(self, url='', *args, **kwargs):
        """
        @param url: database location without scheme
        """
        self.connection = None
        self.cursor = None
        if url:
            self.connect(url, *args, **kwargs)

    def connect(self, url, autocommit=True, *args, **kwargs):
        """Connect to the DB.
        """
        self.url = url
        logger.debug('Creating adapter for `%s`' % self)
        self._queries = []  # [(query_start_time, query_str, query_execution_duration),]
        self.autocommit = autocommit
        self.connection = self._connect(url, *args, **kwargs)
        self.cursor = self.connection.cursor()

    def _connect(self, url, *args, **kwargs):
        """Connect to the DB. To be overridden in subclasses.
        @return: database connection
        """
        raise NotImplementedError()

    def disconnect(self):
        self.connection.close()
        self.connection = None
        self.cursor = None

    def commit(self):
        self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def execute(self, query, *args):
        """Execute a query.
        """
        sql_logger.debug('DB query: %s' % query)
        if not self.cursor:
            raise dbw.AdapterError('No connection has been set yet.')
        start_time = time.time()
        try:
            result = self.cursor.execute(query, *args)
            if self.autocommit:
                self.commit()
        except Exception:
            logger.warning(query)
            raise
        finish_time = time.time()
        self._queries.append((start_time, query, finish_time - start_time))
        self._queries = self._queries[-self._MAX_QUERIES:]
        return result

    def get_last_query(self):
        return self._queries[-1] if self._queries else (0, '', 0)

    def _MODELFIELD(self, field):
        """Render a table column name."""
        # db = db or dbw.GenericAdapter # we do not use adapter here
        assert isinstance(field, dbw.ModelField)
        return '%s.%s' % (field.model, field.column.name)

    def _AND(self, left, right):
        """Render the AND clause."""
        return '(%s AND %s)' % (self.render(left), self.render(right, left))

    def _OR(self, left, right):
        """Render the OR clause."""
        return '(%s OR %s)' % (self.render(left), self.render(right, left))

    def _EQ(self, left, right):
        if right is None:
            return '(%s IS NULL)' % self.render(left)
        return '(%s = %s)' % (self.render(left), self.render(right, left))

    def _NE(self, left, right):
        if right is None:
            return '(%s IS NOT NULL)' % self.render(left)
        return '(%s <> %s)' % (self.render(left), self.render(right, left))

    def _GT(self, left, right):
        return '(%s > %s)' % (self.render(left), self.render(right, left))

    def _GE(self, left, right):
        return '(%s >= %s)' % (self.render(left), self.render(right, left))

    def _LT(self, left, right):
        return '(%s < %s)' % (self.render(left), self.render(right, left))

    def _LE(self, left, right):
        return '(%s <= %s)' % (self.render(left), self.render(right, left))

    def _ADD(self, left, right):
        return '(%s + %s)' % (self.render(left), self.render(right, left))

    def _LIKE(self, expression, pattern):
        "The LIKE Operator."
        return "(%s LIKE '%s')" % (self.render(expression), pattern)

    def _CONCAT(self, expressions):  # ((expression1) || (expression2) || ...)
        "Concatenate two or more expressions."
        rendered_expressions = []
        for expression in expressions:
            rendered_expressions.append('(' + self.render(expression) + ')')
        return '(' + ' || '.join(rendered_expressions) + ')'

    def _IN(self, first, second):
        if isinstance(second, str):
            return '(%s IN (%s))' % (self.render(first), second[:-1])
        items = ', '.join(self.render(item, first) for item in second)
        return '(%s IN (%s))' % (self.render(first), items)

    def _COUNT(self, expression):
        if expression is None:
            return 'COUNT(*)'
        assert isinstance(expression, dbw.Expression)
        distinct = getattr(expression, 'distinct', False)
        expression = self.render(expression)
        if distinct:
            return 'COUNT(DISTINCT %s)' % expression
        else:
            return 'COUNT(%s)' % expression

    def _MAX(self, expression):
        return 'MAX(%s)' % self.render(expression)

    def _MIN(self, expression):
        return 'MIN(%s)' % self.render(expression)

    def _LOWER(self, expression):
        return 'LOWER(%s)' % self.render(expression)

    def _UPPER(self, expression):
        return 'UPPER(%s)' % self.render(expression)

    def _NULL(self):
        return 'NULL'

    def _RANDOM(self):
        return 'RANDOM()'

    def _LIMIT(self, limit=None):
        if not limit:
            return ''
        elif isinstance(limit, int):
            return ' LIMIT %i' % limit
        elif isinstance(limit, (tuple, list)) and len(limit) == 2:
            return ' LIMIT %i OFFSET %i' % (limit[1], limit[0])
        else:
            raise dbw.QueryError('`limit` must be an integer or tuple/list of two elements. '
                                 'Got `%s`' % limit)

    def render(self, value, cast_field=None):
        """Render of a value (Expression, Field or simple (scalar?) value) in a format suitable for
        operations with cast_field in the DB.
        @param value:
        @param cast_field:
        """
        if isinstance(value, dbw.Expression):  # it's an Expression or Field
            if isinstance(value, dbw.DateTimeField):
                pass  # db-api 2.0 supports python datetime as is, no need to stringify
            return value.__str__(self)  # render sub-expression
        else:  # it's a value for a DB column
            if value is not None and cast_field is not None:
                if isinstance(cast_field, dbw.Expression):
                    cast_field = cast_field.type  # expression right operand type
                assert isinstance(cast_field, dbw.ModelField)
#                assert isinstance(cast_field, dbw.Expression), 'Cast field must be an Expression.'
#                if cast_field.__class__ is dbw.Expression:  # Field - subclass of Expression
#                    cast_field = cast_field.type  # expression right operand type
                value = cast_field._cast(value)
                try:
                    return self._render(value, cast_field.column)
                except Exception:
                    logger.warning('Check %r._cast().' % cast_field)
                    raise
            return self._render(value)

    def _render(self, value, column=None):
        """Render a simple value to the format needed for the given column.
        For example, _render a datetime to the format acceptable for datetime columns in this kind
        of DB.
        If there is no column - present the value as string.
        Values are always passed to queries as quoted strings. I.e. even integers like 123 are put
        like '123'.
        """
        if value is None:
            return self._NULL()
        if column:
            assert isinstance(column, Column), 'It must be a Column instance.'
            encode_func_name = '_encode_' + column.type.upper()
            encode_func = getattr(self, encode_func_name, None)
            if callable(encode_func):
                value = encode_func(value, column)
                assert isinstance(value, (str, int, Decimal)), \
                    'Encode `%s.%s` function did not return a string, integer or decimal' \
                    % (self.__class__.__name__, '_encode_' + column.type.upper())
                return str(value)
        return self.escape(value)

    def escape(self, value):
        """Convert a value to string, escape single quotes and enclose it in single quotes.
        """
        return "'%s'" % str(value).replace("'", "''")  # escaping single quotes

    def IntegrityError(self):
        return self.driver.IntegrityError

    def OperationalError(self):
        return self.driver.OperationalError

    def _get_create_table_columns(self, model):
        """Get columns declarations for CREATE TABLE statement.
        """
        columns = []
        for field in model._meta.fields.values():
            column = field.column
            if column is not None:
                columns.append(column.__str__(self))
        return columns

    def _get_create_table_indexes(self, model):
        """Get indexes declarations for CREATE TABLE statement.
        """
        assert dbw.is_model(model)
        indexes = []
        for index in model._indexes:
            if index.type == 'primary':
                index_type = 'PRIMARY KEY'
            elif index.type == 'unique':
                index_type = 'UNIQUE KEY'
            else:
                index_type = 'KEY'
            columns = []
            for index_field in index.index_fields:
                column = index_field.field.name
                if index_field.prefix_length:
                    column += '(%i)' % index_field.prefix_length
                column += ' %s' % index_field.sort_order.upper()
                columns.append(column)

            indexes.append('%s %s (%s)' % (index_type, index.name, ', '.join(columns)))

        return indexes

    def _get_create_table_other(self, model):
        """Additional statements for creating a table.
        """
        return []

    def get_create_table_query(self, model):
        """Get CREATE TABLE statement for the given model in this DB.
        """
        assert dbw.is_model(model), 'Provide a Table subclass.'
        columns = self._get_create_table_columns(model)
        indexes = self._get_create_table_indexes(model)
        query = 'CREATE TABLE %s (' % str(model)
        query += '\n  ' + ',\n  '.join(columns)
        query += ',\n  ' + ',\n  '.join(indexes) + '\n) '
        queries = [query]
        queries.extend(self._get_create_table_other(model))
        return queries

    def _declare_INT(self, column, int_map=((1, 'TINYINT'), (2, 'SMALLINT'), (3, 'MEDIUMINT'),
                                            (4, 'INT'), (8, 'BIGINT'))):
        """Render declaration of INT column type.
        `store_rating_sum` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'Item\'s store rating'
        """
        max_int = int('9' * column.precision)
        # TODO: check for column.unsigned
        bytes_count = math.ceil((max_int.bit_length() - 1) / 8)  # add one bit for sign
        for _bytes_count, _column_type in int_map:
            if bytes_count <= _bytes_count:
                break
        else:
            raise Exception('Too big precision specified.')
        column_str = '%s(%s)' % (_column_type, column.precision)
        if column.unsigned:
            column_str += ' UNSIGNED'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not Nil:
            column_str += ' DEFAULT ' + self._render(column.default, None)
        if column.autoincrement:
            column_str += ' AUTO_INCREMENT'
        if column.comment:
            column_str += ' COMMENT ' + self._render(column.comment, None)
        return column_str

    def _encode_INT(self, value, column):
        """Encode a value for insertion in a column of INT type.
        """
        return str(int(value))

    def _declare_BOOL(self, column):
        """Render declaration of BOOLEAN column type. Store boolean as 0/1 integer.
        """
        column_str = 'TINYINT'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not Nil:
            column_str += ' DEFAULT ' + self._render(column.default, None)
        if column.comment:
            column_str += ' COMMENT ' + self._render(column.comment, None)
        return column_str

    def _encode_BOOL(self, value, column):
        """Encode a value for insertion in a column of INT type.
        """
        return str(int(value))

    def _decode_BOOL(self, value, column):
        """Decode a value from the DB to a value good for the corresponding model field.
        """
        return bool(int(value))

    def _declare_CHAR(self, column):
        """CHAR, VARCHAR
        """
        return 'VARCHAR (%i)' % column.precision

    def _decode_CHAR(self, value, column):
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _declare_DECIMAL(self, column):
        """The declaration syntax for a DECIMAL column is DECIMAL(M,D).
        The ranges of values for the arguments in MySQL 5.1 are as follows:
        M is the maximum number of digits (the precision). It has a range of 1 to 65.
        D is the number of digits to the right of the decimal point (the scale).
        It has a range of 0 to 30 and must be no larger than M.
        """
        return 'DECIMAL(%s, %s)' % (column.precision, column.scale)

    def _declare_DATE(self, column):
        return 'DATE'

    def _declare_DATETIME(self, column):
        return 'INTEGER'

    def _encode_DATETIME(self, value, column):
        """Not all DBs have microsecond precision in DATETIME columns.
        So, generic implementation stores datetimes as integer number of microseconds since the
        Epoch.
        """
        if isinstance(value, str):
            value = DateTime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        if isinstance(value, DateTime):
            # in microseconds since the UNIX epoch
            return int(time.mktime(value.timetuple()) * 1000000) + value.microsecond
        raise dbw.QueryError('Expected datetime.datetime.')

    def _decode_DATETIME(self, value, column):
        return DateTime.fromtimestamp(value / 1000000)

    def _declare_TEXT(self, column):
        return 'TEXT'

    def _declare_BLOB(self, column):
        return 'BLOB'

    def _encode_BLOB(self, value, column):
        return "'%s'" % base64.b64encode(value)

    def _decode_BLOB(self, value, column):
        return base64.b64decode(value)

#    def _getExpressionTables(self, expression):
#        """Get tables involved in WHERE expression.
#        """
#        tables = set()
#        if dbw.is_model(expression):
#            tables.add(expression)
#        elif isinstance(expression, dbw.Field):
#            tables.add(expression.table)
#        elif isinstance(expression, dbw.Expression):
#            tables |= self._getExpressionTables(expression.left)
#            tables |= self._getExpressionTables(expression.right)
#        return tables

    def last_insert_id(self):
        """Get last insert ID."""
        return self.cursor.lastrowid

    def _insert(self, *_fields):
        """Create INSERT query.
        INSERT INTO table_name [ ( col_name1, col_name2, ... ) ]
          VALUES ( expression1_1, expression1_2, ... ),
            ( expression2_1, expression2_2, ... ), ...
        """
        fields = []
        model = None
        for item in _fields:
            assert isinstance(item, (list, tuple)) and len(item) == 2, \
                'Pass tuples with 2 items: (field, value).'
            field = item[0]
            assert isinstance(field, dbw.ModelField), 'First item must be a Field.'
            _model = field.model
            model = model or _model
            assert model is _model, 'Pass fields from the same table'
            if not field.column.autoincrement:
                fields.append(item)
        keys = ', '.join(field.column.name for field, _ in fields)
        values = ', '.join(self.render(value, field) for field, value in fields)
        return 'INSERT INTO %s (%s) VALUES (%s)' % (model, keys, values)

    def insert(self, *fields):
        """Insert records in the db.
        @param *args: tuples in form (Field, value)
        """
        query = self._insert(*fields)
        self.execute(query)
        return self.last_insert_id()

    def _update(self, *fields, where=None, limit=None):
        """UPDATE table_name SET col_name1 = expression1, col_name2 = expression2, ...
          [ WHERE expression ] [ LIMIT limit_amount ]
          """
        model = None
        for item in fields:
            assert isinstance(item, (list, tuple)) and len(item) == 2, \
                'Pass tuples with 2 items: (field, value).'
            field, value = item
            assert isinstance(field, dbw.ModelField), 'First item in the tuple must be a Field.'
            _model = field.model
            if model is None:
                model = _model
            assert model is _model, 'Pass fields from the same model'
        sql_w = (' WHERE ' + self.render(where)) if where else ''
        sql_v = ', '.join(['%s= %s' % (field.column.name, self.render(value, field))
                           for (field, value) in fields])
        return 'UPDATE %s SET %s%s' % (model, sql_v, sql_w)

    def update(self, *fields, where=None, limit=None):
        """Update records
        @param *args: tuples in form (ModelField, value)
        @param where: an Expression or string for WHERE part of the DELETE query
        @param limit: a tuple in form (start, end) which specifies the range dor deletion.
        """
        sql = self._update(*fields, where=where)
        self.execute(sql)
        return self.cursor.rowcount

    def _delete(self, model, where, limit=None):
        """DELETE FROM table_name [ WHERE expression ] [ LIMIT limit_amount ]"""
        assert dbw.is_model(model)
        sql_w = ' WHERE ' + self.render(where) if where else ''
        return 'DELETE FROM %s%s' % (model, sql_w)

    def delete(self, model, where, limit=None):
        """Delete records from table with the given condition and limit.
        @param talbe: a Model subclass, whose records to delete
        @param where: an Expression or string for WHERE part of the DELETE query
        @param limit: a tuple in form (start, end) which specifies the range dor deletion.
        """
        sql = self._delete(model, where)
        self.execute(sql)
        return self.cursor.rowcount

    def _select(self, *fields, from_=None, where=None, orderby=False, limit=False,
                distinct=False, groupby=False, having=''):
        """SELECT [ DISTINCT ] column_expression1, column_expression2, ...
          [ FROM from_clause ]
          [ JOIN table_name ON (join_condition) ]
          [ WHERE where_expression ]
          [ GROUP BY expression1, expression2, ...
              [ HAVING having_expression ] ]
          [ ORDER BY order_column_expr1, order_column_expr2, ... ]
        """
        if not fields:
            raise dbw.QueryError('Specify at least on field to select.')

        if not from_:
            from_ = []
            for field in fields:
                if isinstance(field, dbw.Expression):
                    # some expressions might have `model` attribute
                    model = getattr(field, 'model', None)
                    if model is not None and model not in from_:
                        from_.append(field.model)

        if not from_:
            raise dbw.QueryError('Specify at least one model in `from_` argument or at least on '
                                 'Field to select')

        _fields = []
        for field in fields:
            if isinstance(field, dbw.Expression):
                field = self.render(field)
            elif not isinstance(field, str):
                raise dbw.QueryError('Field must an Expression/Field/str instance. Got `%s`'
                                     % dbw.get_object_path(field))
            _fields.append(field)
        sql_fields = ', '.join(_fields)

        tables = []
        joins = []
        texts = []

        for arg in dbw.listify(from_):
            if dbw.is_model(arg):
                tables.append(str(arg))
            elif isinstance(arg, dbw.Join):
                joins.append('%s JOIN %s ON %s' % (arg.type.upper(), arg.model,
                                                   self.render(arg.on)))
            elif isinstance(arg, str):
                texts.append(arg)
            else:
                raise dbw.QueryError('`from_` argument should contain only Models, Joins or '
                                     'strings, but got a `%s`' % dbw.get_object_path(arg))

        sql_from = ''
        if tables:
            sql_from += ' ' + ', '.join(tables)
        if texts:
            sql_from += ' ' + ' '.join(texts)
        if joins:
            sql_from += ' ' + ' '.join(joins)

        if not where:
            sql_where = ''
        elif isinstance(where, dict):
            items = []
            for key, value in where.items():
                items.append('(%s = %s)' % (key, self.render(value)))
            sql_where = ' WHERE ' + ' AND '.join(items)

        elif isinstance(where, str):
            sql_where = ' WHERE ' + where
        elif isinstance(where, dbw.Expression):
            sql_where = ' WHERE ' + self.render(where)
        else:
            raise dbw.exceptions.QueryError('Where argument should be a dict, str or Expression')

        sql_select = ''
        if distinct is True:
            sql_select += 'DISTINCT'
        elif distinct:
            sql_select += 'DISTINCT ON (%s)' % distinct

        sql_other = ''
        if groupby:
            _groupby = []
            for _expr in dbw.listify(groupby):
                if isinstance(_expr, dbw.Expression):
                    _expr = self.render(_expr)
                elif not isinstance(_expr, str):
                    raise dbw.QueryError('Groupby should be Field, Expression or str.')
                _groupby.append(_expr)
            sql_other += ' GROUP BY %s' % ', '.join(_groupby)
            if having:
                if isinstance(having, dbw.Expression):
                    _having = self.render(having)
                elif isinstance(having, str):
                    _having = having
                else:
                    raise dbw.QueryError('Groupby should be Field, Expression or str.')
                sql_other += ' HAVING %s' % _having

        if orderby:
            orderby = dbw.listify(orderby)
            _orderby = []
            for _expr in orderby:
                if isinstance(_expr, dbw.Expression):
                    _expr = self.render(_expr) + ' ' + _expr.sort
                elif isinstance(_expr, str):
                    if _expr.lower() == '<random>':
                        _expr = self._RANDOM()
                else:
                    raise dbw.QueryError('Orderby should be Field, Expression or str.')
                _orderby.append(_expr)
            sql_other += ' ORDER BY %s' % ', '.join(_orderby)

# When using LIMIT, it is a good idea to use an ORDER BY clause that constrains the result rows into
# a unique order. Otherwise you will get an unpredictable subset of the query's rows -- you may be
# asking for the tenth through twentieth rows, but tenth through twentieth in what ordering?
# The ordering is unknown, unless you specified ORDER BY.
#        if limit:
#            if not orderby and tables:
#                sql_other += ' ORDER BY %s' % ', '.join(map(str, (table.id for table in tables)))

        sql_other += self._LIMIT(limit)
        sql = 'SELECT %s %s FROM %s%s%s' % (sql_select, sql_fields, sql_from, sql_where, sql_other)

        return fields, sql

    def select(self, *fields, from_=None, where=None, **attributes):
        """Create and return SELECT query.
        @param fields: tables, fields or joins;
        @param from_: tables and joined tables to select from.
            None -tables will be automatically extracted from provided fields
            A single model or string
            A list of models or strings
        @param where: expression for where;
        @param limit: an integer (LIMIT) or tuple/list of two elements (OFFSET, LIMIT)
        @param orderby:
        @param groupby:
        tables are taken from fields and `where` expression;
        """
        fields, query = self._select(*fields, from_=from_, where=where, **attributes)
        self.execute(query)
        rows = list(self.cursor.fetchall())
        return self._parse_response(fields, rows)

    def _parse_response(self, fields, rows):
        """Post process results fetched from the DB. Decode values to model fields representation.
        Return results in Rows object.
        """
        for i, row in enumerate(rows):
            new_row = []
            for j, field in enumerate(fields):
                value = row[j]
                if value is not None and isinstance(field, dbw.FieldExpression):
                    column = field.left.column
                    if isinstance(column, dbw.Column):
                        decode_func = getattr(self, '_decode_' + column.type.upper(), None)
                        if decode_func:
                            value = decode_func(value, column)
                new_row.append(value)
            rows[i] = new_row

        return Rows(self, fields, rows)


####################################################################################################

class Rows():
    """Keeps results of a SELECT and provides methods for convenient access.
    """
    def __init__(self, db, fields, rows):
        """
        @param fields: list of queried fields
        @param rows: list of tuples with query result
        """
        self.db = db
        self.fields = tuple(fields)
        self.rows = rows
        self._fields_str = tuple(str(field) for field in fields)
        # {field_str: field_order}
        self._fields_order = dict((field_str, i) for i, field_str in enumerate(self._fields_str))

    def value(self, row_no, field):
        """Get a value.
        @param rowNo: row number
        @param field: field instance or column number
        """
        if isinstance(field, int):
            column_no = field
        else:
            column_no = self._fields_order[str(field)]
        return self.rows[row_no][column_no]

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]

    def __iter__(self):
        """Iterator over records."""
        return iter(self.rows)

    def __str__(self):
        return pprint.pformat(self.rows)

    def dictresult(self):
        """Iterator of the result which return row by row in form
        {'field1_name': field1_value, 'field2_name': field2_value, ...}
        """
        for row in self.rows:
            yield {self._fields_str[i]: value for i, value in enumerate(row)}


#---------------------------------------------------------------------------------------------------

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
        return 'TEXT'

    def _declare_INT(self, column):
        """INTEGER column type for Sqlite.
        """
        max_int = int('9' * column.precision)
        bytes_count = math.ceil((max_int.bit_length() - 1) / 8)  # add one bit for sign
        if bytes_count > 8:
            raise Exception('Too many digits specified.')
        return 'INTEGER'

    def _declare_DATE(self, column):
        """Sqlite db does have native DATE data type.
        We store dates in it as integer number of days since the Epoch.
        """
        return 'INTEGER'

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
        return 'INTEGER'

    def _encode_DECIMAL(self, value, column):
        return Decimal(value) * (10 ** column.scale)

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
            logger.debug('Found table column: %s, %s' % (table_name, row))
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
            logger.debug('Reproduced table column: %s, %s' % (table_name, column))
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


#---------------------------------------------------------------------------------------------------


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

#---------------------------------------------------------------------------------------------------


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
            if column.default is not Nil:
                column_str += ' DEFAULT ' + self._render(column.default, None)

        return column_str

    def _declare_BOOL(self, column):
        """Render declaration of BOOLEAN column type.
        """
        column_str = 'BOOLEAN'
        if not column.nullable:
            column_str += ' NOT'
        column_str += ' NULL'
        if column.default is not Nil:
            column_str += ' DEFAULT ' + self._render(column.default, None)
        if column.comment:
            column_str += ' COMMENT ' + self._render(column.comment, None)
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
                    "COMMENT ON COLUMN %s.%s IS %s" % (model._meta.db_name, column.name,
                                                       self.escape(column.comment))
                )

        return queries

    def get_tables(self):
        """Get list of tables (names) in this DB.
        """
        self.execute("SELECT table_name "
                     "FROM information_schema.tables "
                     "WHERE table_schema = 'public'")
        return [row[0] for row in self.cursor.fetchall()]

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
        self.execute(query)
        return self.cursor.fetchone()[0]

    def _drop_table(self, table_name):
        """Return query for dropping a table.
        @param table_name: table name or a model describing the table
        """
        if dbw.is_model(table_name):
            table_name = table_name._meta.db_name
        if not isinstance(table_name, str):
            raise AssertionError('Expecting a str or a Model')
        return 'DROP TABLE IF EXISTS %s' % table_name
