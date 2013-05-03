__author__ = 'Victor Varvariuc <victor.varvariuc@gmail.com>'

import unittest
import os
from datetime import date as Date
from decimal import Decimal

import dbw

from . import test_postgres_adapter


class SqliteAdapterTest(test_postgres_adapter.PostgresqlAdapterTest):

    @classmethod
    def setUpClass(cls):
        db_path = os.environ.get('SQLITE_TEST_DB')
        if not db_path:
            raise unittest.SkipTest('No path of test db file is defined.')
        with open(db_path, 'w'):
            pass  # create/truncate the file
        cls.db = dbw.connect('sqlite://' + db_path)

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()
        os.remove(cls.db.db_path)

    def _get_book_field_info(self):
        """Info about how Book record fields are stored and retrieved.
        """
        return (
            ('id', int, int),
            ('name', str, str),
            ('author_id', int, int),
            ('price', Decimal, int),
            ('publication_date', Date, int),
            ('is_favorite', bool, int),
        )
