"""DBW Exceptions
"""


class DbwError(Exception):
    """Base exception for ORM errors.
    """


class AdapterError(DbwError):
    """
    """


class AdapterNotFound(AdapterError):
    """Suitable db adapter no found for the specified protocol.
    """


class ModelError(DbwError):
    """A problem with a model.
    """


class ModelFieldError(DbwError):
    """A problem with a model field.
    """


class RecordError(ModelError):
    """A problem with a model instance.
    """


class DbError(DbwError):
    """Base exception on operations with DB.
    """


class DbConnectionError(DbError):
    """
    """


class RecordNotFound(DbError):
    """A requested record was not found in the DB table.
    Each model has its own subclass of this exception.
    """


class MultipleRecordsFound(DbError):
    """Got too many records (usually where one was expected - got more than one).
    Each model has its own subclass of this exception.
    """


class RecordSaveError(RecordError):
    """Record save error.
    """


class RecordValueError(ModelError):
    """A problem with a model instance value.
    """


class QueryError(DbwError):
    """Bad parameters to a query.
    """


class TableError(DbError):
    """A problem with a db table structure.
    """


class TableMissing(TableError):

    def __init__(self, db, model):
        """A corresponding table for a model was not found in the db.
        @param db: which db [adapter]
        @param model: which model
        """
        self.db = db
        self.model = model

    def __str__(self):
        return 'Table `%(model)s` does not exist in database `%(db)s`' % self.__dict__


class ColumnError(TableError):
    """A problem with a db table column structure.
    """


class ColumnMissing(TableError):
    """A column for a model is missing in the corresponding db table.
    """
