"""
Tests for DBW
"""
__author__ = 'Victor Varvariuc <victor.varvariuc@gmail.com>'

import unittest

import dbw


class TestModelAttr(unittest.TestCase):

    def test_model_attr(self):

        class ModelAttribute(dbw.ModelAttr):
            sequence = 0

            def __init__(self):
                self.sequence = self.sequence + 1

        class ModelAttributeSubclass(ModelAttribute):
            def __init__(self):
                self.sequence = self.sequence + 1
                assert ModelAttribute is not super(ModelAttributeSubclass)
                super().__init__()

        class TestModel(dbw.Model):
            attr1 = ModelAttribute()
            self.assertEqual(attr1.sequence, 0)
            attr2 = ModelAttributeSubclass()
            self.assertEqual(attr2.sequence, 0)

        self.assertEqual(TestModel.attr1.sequence, 1)
        self.assertEqual(TestModel.attr2.sequence, 2)


class TestModels(unittest.TestCase):

    def test_model_options(self):

        ok = False
        try:
            class TestModel(dbw.Model):
                _meta = object()
        except dbw.ModelError:
            ok = True
        self.assertTrue(ok, '`_meta` should be only instance of ModelOptions.')

        class TestModel1(dbw.Model):
            field1 = dbw.IntegerField()
            _meta = dbw.ModelOptions(
                db_name='test1234',
            )

        self.assertIsInstance(TestModel1._meta, dbw.ModelOptions)
        self.assertEqual(TestModel1._meta.db_name, 'test1234')
        # Model._meta.fields should be a {fieldName: Field, ...}
        self.assertIsInstance(TestModel1._meta.fields, dict)
        for fieldName, field in TestModel1._meta.fields.items():
            self.assertIsInstance(fieldName, str)
            self.assertIsInstance(field, dbw.ModelField)

        class TestModel2(TestModel1):
            pass

        # _meta should be per Model, as each model contains its own fields, name, etc.
        self.assertIsNot(TestModel1._meta, TestModel2._meta)
        # as db_name was not given, it is calculated from Model name
        self.assertEqual(TestModel2._meta.db_name, 'test_model2')

        class TestModel3(dbw.Model):
            last_name = dbw.CharField(max_length=100)
            first_name = dbw.CharField(max_length=100)
            # you can specify name of the fields in indexes
            _meta = dbw.ModelOptions(
                db_indexes=dbw.DbUnique('last_name', 'first_name'),
            )

        # test indexes in _meta
        self.assertIsInstance(TestModel3._meta.db_indexes, list)
        # primary index for id and our compound index
        self.assertEqual(len(TestModel3._meta.db_indexes), 2)
        for index in TestModel3._meta.db_indexes:
            self.assertIsInstance(index, dbw.DbIndex)
            self.assertIsInstance(index.index_fields, list)

        self.assertEqual(len(TestModel3._meta.db_indexes[0].index_fields), 2)
        self.assertIsInstance(TestModel3._meta.db_indexes[0].index_fields[0].field, dbw.CharField)
        self.assertIsInstance(TestModel3._meta.db_indexes[0].index_fields[1].field, dbw.CharField)
        self.assertEqual(TestModel3._meta.db_indexes[0].type, 'unique')

        self.assertEqual(len(TestModel3._meta.db_indexes[1].index_fields), 1)
        self.assertIsInstance(TestModel3._meta.db_indexes[1].index_fields[0].field, dbw.IdField)
        self.assertEqual(TestModel3._meta.db_indexes[1].type, 'primary')

        # you can specify fields in indexes
        class TestModel4(dbw.Model):
            last_name = dbw.CharField(max_length=100)
            first_name = dbw.CharField(max_length=100)
            _meta = dbw.ModelOptions(
                db_indexes=dbw.DbUnique(last_name, first_name)
            )

        # you can specify more sophisticated indexes
        class TestModel5(dbw.Model):
            name = dbw.CharField(max_length=100)
            description = dbw.TextField()
            birth_date = dbw.DateField()
            _meta = dbw.ModelOptions(
                db_indexes=(dbw.DbIndex(dbw.DbIndexField(name, 'desc'),
                            dbw.DbIndexField(description, prefix_length=30)),
                            dbw.DbIndex(birth_date))
            )

    def test_model_inheritance(self):

        class TestModel1(dbw.Model):
            field1 = dbw.CharField(max_length=100)

        class TestModel2(TestModel1):
            field1 = dbw.IntegerField()
            field2 = dbw.CharField(max_length=100)

        self.assertIsNot(TestModel2.field1, TestModel1.field1)
        self.assertIsInstance(TestModel1.field1.left, dbw.CharField)
        self.assertIsInstance(TestModel2.field1.left, dbw.IntegerField)
        self.assertIs(TestModel1.field1.left.model, TestModel1)
        self.assertIs(TestModel2.field1.left.model, TestModel2)


class TestModelFields(unittest.TestCase):

    def test_model_field_name(self):

        ok = False
        try:
            class TestModel1(dbw.Model):
                _field = dbw.IntegerField()
        except dbw.ModelError:
            ok = True
        self.assertTrue(ok, 'Models should not accept fields with names starting with `_`')

        class TestModel2(dbw.Model):
            integer_field = dbw.IntegerField()

        # Model.field returns Expression, not Field
        self.assertIsInstance(TestModel2.integer_field, dbw.FieldExpression)
        self.assertEqual(TestModel2.integer_field.left.name, 'integer_field')
        self.assertIsInstance(TestModel2.integer_field, dbw.FieldExpression)

    def test_record_values(self):

        class TestModel2(dbw.Model):
            integer_field = dbw.IntegerField()
            record_field = dbw.RelatedRecordField('self')

        class TestModel3(dbw.Model):
            pass

        # create a record from the model
        record = TestModel2(None)
        record1 = TestModel2(None, id=101)
        record2 = TestModel2(None)
        record3 = TestModel3(None)

        # the assignment should fail, only integers should be accepted
        self.assertRaises(dbw.RecordValueError, setattr, record, 'integer_field', '1')
        # try to assign a record of another model
        self.assertRaises(dbw.RecordValueError, setattr, record, 'record_field', record3)

        record2.record_field = record
        self.assertEqual(record2.record_field, record)
        self.assertEqual(record2.record_field_id, None)

        record2.record_field_id = record1.id
        # no adapter was given, when the record was created
        self.assertRaises(dbw.AdapterError, getattr, record2, 'record_field')
        self.assertEqual(record2.record_field_id, record1.id)

        record2.record_field = None
        self.assertEqual(record2.record_field, None)
        self.assertEqual(record2.record_field_id, None)

        record2.record_field_id = None
        self.assertEqual(record2.record_field, None)
        self.assertEqual(record2.record_field_id, None)


class TestExpressions(unittest.TestCase):

    def test_expressions(self):

        class TestModel1(dbw.Model):
            field1 = dbw.IntegerField()
            field2 = dbw.CharField(max_length=100)

        class TestModel2(dbw.Model):
            field3 = dbw.RelatedRecordField(TestModel1)

        self.assertEqual(str(TestModel1.id == 1), '(test_model1.id = 1)')
        self.assertEqual(str(TestModel1.field1 == 1), '(test_model1.field1 = 1)')
        self.assertEqual(str(TestModel1.field2 == 2), "(test_model1.field2 = '2')")
        self.assertEqual(str(TestModel2.field3 == 3), "(test_model2.field3_id = 3)")
        self.assertEqual(str(TestModel2.field3.upper()), "UPPER(test_model2.field3_id)")
        self.assertEqual(str(TestModel2.field3.lower()), "LOWER(test_model2.field3_id)")
        self.assertEqual(str(TestModel2.field3.min()), "MIN(test_model2.field3_id)")
        self.assertEqual(str(TestModel2.field3.max()), "MAX(test_model2.field3_id)")
        self.assertEqual(str(TestModel2.field3.in_(1, 2)), "(test_model2.field3_id IN (1, 2))")
        self.assertEqual(str(TestModel2.field3.count()), "COUNT(test_model2.field3_id)")
        self.assertEqual(str(TestModel2.field3.like('%ed')), "(test_model2.field3_id LIKE '%ed')")
