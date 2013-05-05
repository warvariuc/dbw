import time
import math
import base64
from datetime import date as Date, datetime as DateTime
from decimal import Decimal

import dbw
from . import Column, Rows


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
        dbw.logger.debug('Creating adapter for `%s`' % self)
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
        dbw.sql_logger.debug('DB query: %s' % query)
        if not self.cursor:
            raise dbw.AdapterError('No connection has been set yet.')
        start_time = time.time()
        try:
            result = self.cursor.execute(query, *args)
            if self.autocommit:
                self.commit()
        except Exception:
            dbw.logger.warning(query)
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
                    dbw.logger.warning('Check %r._cast().' % cast_field)
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
        if column.default is not dbw.Nil:
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
        if column.default is not dbw.Nil:
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

    def _select(self, *fields, from_='', where='', orderby='', limit=None,
                distinct='', groupby='', having=''):
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

        return Rows(self, sql, fields)

    def select(self, *fields, from_='', where='', orderby='', limit=None,
               distinct='', groupby='', having=''):
        """Create and return SELECT query.
        @param fields: tables, fields or joins;
        @param from_: tables and joined tables to select from.
            None -tables will be automatically extracted from provided fields
            A single model or string
            A list of models or strings
        @param where: expression for where;
        @param limit: an integer (LIMIT) or tuple/list of two elements (OFFSET, LIMIT)
        @param orderby: list of expressions to sort by
        @param groupby: list of expressions to group by
        @param having: list of condition expressions to apply within group by
        tables are taken from fields and `where` expression;
        """
        rows = self._select(*fields, from_=from_, where=where, orderby=orderby,
                             limit=limit, distinct=distinct, groupby=groupby, having=having)
        assert isinstance(rows, Rows)
        rows._execute()
        return rows
