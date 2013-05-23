__author__ = "Victor Varvariuc <victor.varvariuc@gmail.com>"

import inspect
from datetime import datetime as DateTime, date as Date
from decimal import Decimal
from collections import OrderedDict

import dbw


class ModelAttrInfo():
    """Information about an attribute of a Model class.
    """
    def __init__(self, model, name):
        """
        @param model: model class to which the attribute belongs
        @param name: name of the attribute in the model class
        """
        if model is not None:
            assert dbw.is_model(model)
            assert isinstance(name, str) and name
#            assert hasattr(model, name), 'Model %s does not have an attribute with name %s' \
#                % (dbw.get_object_path(model), name)
        self.model = model
        self.name = name


class ProxyInit():
    """A descriptor to hook accesss to `__init__` of a Model attribute, which needs postponed
    initialization only when the model is fully initialized.
    """
    def __init__(self, cls):
        """
        @param cls: model attribute class
        """
        if not isinstance(cls.__init__, ProxyInit):
            self.orig_init = cls.__init__  # original __init__
            cls.__init__ = self

    def __get__(self, obj, cls):

        if obj is None:  # called as class attribute
            return self

        orig_init = self.orig_init

        def proxy_init(self, *args, model_attr_info=None, **kwargs):
            """This will replace `__init__` method of a Model attribute, will remember
            initialization arguments and will call the original `__init__` when information
            about the model attribute is passed.
            """
#                print('ModelAttrStubMixin.__init__', self.__class__.__name__, args, kwargs)
            if model_attr_info is not None:
                self._model_attr_info = model_attr_info
            else:
                obj._init_args = args
                obj._init_kwargs = kwargs
            if self._model_attr_info.model is not None:
                orig_init(self, *self._init_args, **self._init_kwargs)

        # functions are descriptors, to be able to work as bound methods
        return proxy_init.__get__(obj, cls)



class ModelAttr():
    """Base class for Model attributes, replaces original `__init__` method with a wrapper around
    it. The wrapper remembers initialization arguments and calls the real `__init__` only after the
    model is completely defined, e.g. the Model metaclass passes model/attribute info.
    """
    __creation_ounter = 0  # will be used to track the definition order of the attributes in models
    _model_attr_info = ModelAttrInfo(None, None)  # model attribute information, set by `_init_`

    def __new__(cls, *args, **kwargs):
        """Create the object, but prevent calling its `__init__` method, monkey patching it with a
        stub, remembering the initizalization arguments. The real `__init__` can be called later.
        """
        # create the object normally
        self = super().__new__(cls)
        ModelAttr.__creation_ounter += 1
        self._creation_order = ModelAttr.__creation_ounter
#        print('ModelAttrMixin.__new__', cls, repr(self))
        ProxyInit(cls)  # monkey patching `__init__` with our version
        return self


from . import model_fields, exceptions, model_options, query_manager


class ModelType(type):
    """Metaclass for all models.
    It gives names to all fields and makes instances for fields for each of the models.
    It has some class methods for models.
    """
    def __new__(cls, name, bases, attrs):

        NewModel = super().__new__(cls, name, bases, attrs)
        parent_models = [base for base in bases if isinstance(base, ModelType)]

        try:

            dbw.logger.debug('Finishing initialization of model `%r`', NewModel)

            model_attrs = OrderedDict()
            for attr_name, attr in inspect.getmembers(NewModel):
                if isinstance(attr, ModelAttr):
                    model_attrs[attr_name] = attr

            _meta = model_attrs.pop('_meta', None)
            assert isinstance(_meta, model_options.ModelOptions), \
                '`_meta` attribute should be instance of ModelOptions'
            # sort by definition order - for the correct recreation order
            model_attrs = sorted(model_attrs.items(), key=lambda i: i[1]._creation_order)

            for attr_name, attr in model_attrs:
                if attr._model_attr_info.model:  # inherited field
                    # make its copy for the new model
                    _attr = attr.__class__(*attr._init_args, **attr._init_kwargs)
                    _attr._creation_order = attr._creation_order
                    attr = _attr
                try:
                    attr.__init__(model_attr_info=ModelAttrInfo(NewModel, attr_name))
                except Exception:
                    dbw.logger.debug(
                        'Failed to init a model attribute: %r.%s', NewModel, attr_name)
                    raise
                setattr(NewModel, attr_name, attr)

            # process _meta here, when all fields should have been initialized
            if _meta._model_attr_info.model is not None:  # inherited
                dbw.logger.debug('Model `%r` does not have `_meta` attribute', NewModel)
                _meta = model_options.ModelOptions()  # override
            _meta.__init__(model_attr_info=ModelAttrInfo(NewModel, '_meta'))
            NewModel._meta = _meta

            if parent_models:
                # make per model exceptions
                # exceptions have the same name and are inherited from parent models exceptions
                NewModel.RecordNotFound = type(
                    'RecordNotFound',
                    tuple(parent_model.RecordNotFound
                          for parent_model in parent_models),
                    {'__module__': NewModel.__module__}
                )
                NewModel.MultipleRecordsFound = type(
                    'MultipleRecordsFound',
                    tuple(parent_model.MultipleRecordsFound
                          for parent_model in parent_models),
                    {'__module__': NewModel.__module__}
                )

        except Exception as exc:
            raise exceptions.ModelError(str(exc))

        return NewModel

    def __getitem__(self, field_name):
        """Get a Model field by name - Model['field_name'].
        """
        if not isinstance(field_name, str):
            raise exceptions.ModelError('Pass a field name as key.')
        if field_name in self._meta.fields:
            return getattr(self, field_name)  # to ensure descriptor behavior
        raise exceptions.ModelFieldError('Model `%s` does not have field `%s`.'
                                         % (dbw.get_object_path(self), field_name))

    def __iter__(self):
        """Get Table fields.
        """
        for field_name in self._meta.fields:
            yield getattr(self, field_name)  # to ensure descriptor behavior

    def __len__(self):
        return len(self._meta.fields)

    def __str__(self):
        return self._meta.db_name

    def __repr__(self):
        return dbw.get_object_path(self)


class Model(metaclass=ModelType):
    """Base class for all models. Class attributes - the fields.
    Instance attributes - the values for the corresponding model fields.
    """
    objects = query_manager.QueryManager()
    _meta = model_options.ModelOptions(abstract=True)

    RecordNotFound = exceptions.RecordNotFound
    MultipleRecordsFound = exceptions.MultipleRecordsFound

    # default fields
    # row id. This field is present in all model
    id = model_fields.IdField()
    # version of the record - datetime (with milliseconds) of the last update of this record
    timestamp = model_fields.DateTimeField()

    def __init__(self, db, *args, **kwargs):
        """Create a model instance - a record.
        @param db: db adapter in which to save the table record or from which it was fetched
        @param *args: tuples (ModelField or field_name, value)
        @param **kwargs: {field_name: fieldValue}
        """
        if not (isinstance(db, adapters.GenericAdapter) or db is None):
            raise exceptions.RecordError('`db` should be a GenericAdapter instance')
        self._db = db

        model = None
        for arg in args:
            assert isinstance(arg, (list, tuple)) and len(arg) == 2, \
                'Pass tuples with 2 items: (field, value).'
            field, value = arg
            if isinstance(field, str):
                field = self.__class__[field]
            assert isinstance(field, model_fields.FieldExpression), 'First arg must be a Field.'
            field = field.left
            _model = field.model
            model = model or _model
            assert model is _model, 'Pass fields from the same model'
            kwargs[field.name] = value

        # make values for fields
        for field_name, field in self._meta.fields.items():
            # is this a field name?
            field_value = kwargs.pop(field_name, dbw.Nil)
            if field_value is dbw.Nil and isinstance(field, model_fields.RelatedRecordField):
                # a related record id?
                field_value = kwargs.pop(field._name, dbw.Nil)
                if field_value is not dbw.Nil:
                    field_name = field._name

            if field_value is dbw.Nil:
                field_value = field.default

            setattr(self, field_name, field_value)

        if kwargs:
            raise exceptions.ModelError('Got unknown field names: %s' % ', '.join(kwargs))

    def __getitem__(self, field):
        """Get a Record Field value by key.
        key: either a Field instance or name of the field.
        """
        model = self.__class__
        if isinstance(field, model_fields.FieldExpression):
            field = field.left
        if isinstance(field, model_fields.ModelField):
            assert field.model is model, 'This field is from another model.'
            attr_name = field.name
        elif isinstance(field, str):
            field = model[field]
            attr_name = field.name
        else:
            raise TypeError('Pass either a Field or its name.')
        return getattr(self, attr_name)

    def delete(self):
        """Delete this record. Issues a SQL DELETE for the object. This only deletes the object in
        the database; the Python instance will still exist and will still have data in its fields.
        `record.id` will be set to None.
        """
        db = self._db
        model = self.__class__
        model.objects.check_table(db)
        signals.pre_delete.send(sender=model, record=self)
        db.delete(model, where=(model.id == self.id))
        db.commit()
        signals.post_delete.send(sender=model, record=self)
        self.id = None

    def save(self):
        db = self._db
        model = self.__class__
        model.objects.check_table(db)
        self.timestamp = DateTime.now()
        values = []  # list of tuples (Field, value)
        for field in model._meta.fields.values():
            value = dbw.Nil
            if isinstance(field, model_fields.RelatedRecordField):
                value = getattr(self, field._name)
            else:
                value = self[field]

            values.append(field(value))

        signals.pre_save.send(sender=model, record=self)

        is_new = not self.id
        if is_new:  # new record
            self.id = db.insert(*values)
        else:  # existing record
            rows_count = db.update(*values, where=(model.id == self.id))
            if not rows_count:
                raise dbw.exceptions.RecordSaveError('Looks like the record was deleted: table=`%s`'
                                                     ', id=%s' % (model, self.id))
        db.commit()

        signals.post_save.send(sender=model, record=self, is_new=is_new)

    def __repr__(self):
        """Human readable presentation of the record.
        """
        values = []
        for field_name, field in self._meta.fields.items():
            if isinstance(field, model_fields.RelatedRecordField):
                field_name = field._name
            field_value = getattr(self, field_name)
            if isinstance(field_value, (Date, DateTime, Decimal)):
                field_value = str(field_value)
            values.append("%s= %r" % (field_name, field_value))
        return '%s(%s)' % (dbw.get_object_path(self), ', '.join(values))

    @classmethod
    def count(cls):
        """Get COUNT expression for this table.
        """
        return dbw.Expression('_COUNT', None, model=cls)


class Join():
    """Object holding parameters for a join.
    """
    def __init__(self, model, on, type=''):
        """
        @param model: table to join
        @param on: join condition
        @param type: join type. if empty - INNER JOIN
        """
        assert dbw.is_model(model), 'Pass a model class.'
        assert isinstance(on, dbw.Expression), 'WHERE should be an Expression.'
        self.model = model  # table to join
        self.on = on  # expression defining join condition
        self.type = type  # join type. if empty - INNER JOIN


class LeftJoin(Join):
    """Left join parameters.
    """
    def __init__(self, table, on):
        super().__init__(table, on, 'left')


from . import adapters, signals
