"""
This module contains database adapters, which incapsulate all operations specific to a certain
database. All other ORM modules should be database agnostic.
"""
__author__ = "Victor Varvariuc <victor.varvariuc@gmail.com>"

import pprint

import dbw


class Column():
    """Information about database table column.
    """
    def __init__(self, type, name, default=dbw.Nil, precision=None, scale=None, unsigned=None,
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


class Rows():
    """The object keeps results of a SELECT and provides methods for convenient access.
    """
    def __init__(self, db, query, fields):
        """
        @param db: adapter thrught which the query was made
        @param query: the SELECT sql query performed
        @param fields: list of queried fields
        """
        self.db = db
        self.query = query
        self.values = []
        self.fields = tuple(fields)
        self._fields_str = tuple(str(field) for field in fields)
        # {field_str: field_order}
        self._fields_order = dict((field_str, i) for i, field_str in enumerate(self._fields_str))

    def execute_query(self):
        """Execute the SELECT query and process results fetched from the DB.
        Decode values to model fields representation.
        """
        self.db.execute(self.query)
        rows = []
        for row in self.db.cursor.fetchall():
            new_row = []
            for field_no, field in enumerate(self.fields):
                value = row[field_no]
                if value is not None and isinstance(field, dbw.FieldExpression):
                    column = field.left.column
                    if isinstance(column, Column):
                        decode_func = getattr(self.db, '_decode_' + column.type.upper(), None)
                        if decode_func:
                            value = decode_func(value, column)
                new_row.append(value)
            rows.append(new_row)
        self.values = rows

    def value(self, row_no, field):
        """Get a value.
        @param rowNo: row number
        @param field: field instance or column number
        """
        field_no = field if isinstance(field, int) else self._fields_order[str(field)]
        return self.values[row_no][field_no]

    def __len__(self):
        return len(self.values)

    def __getitem__(self, row_no):
        return self.values[row_no]

    def __iter__(self):
        """Iterator over records."""
        return iter(self.values)

    def __repr__(self):
        return pprint.pformat(self.values)

    def dictresult(self):
        """Iterator of the result which return row by row in form
        {'field1_name': field1_value, 'field2_name': field2_value, ...}
        """
        for row in self.values:
            yield {self._fields_str[i]: value for i, value in enumerate(row)}


from .generic import *
from .sqlite import *
from .postgresql import *
from .mysql import *
