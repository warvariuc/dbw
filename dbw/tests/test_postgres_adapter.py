__author__ = 'Victor Varvariuc <victor.varvariuc@gmail.com>'

import unittest
import os
from datetime import date as Date, datetime as DateTime
from decimal import Decimal

import dbw


class PostgresqlAdapterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db_url = os.environ.get('POSTGRESQL_TEST_DB')
        if not db_url:
            raise unittest.SkipTest('No URL of test db is defined.')
        cls.db = dbw.connect('postgresql://' + db_url)

        # Drops all tables from the test database
        cls.db.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type != 'VIEW' AND table_name NOT LIKE 'pg_ts_%%'
        """)
        for (table_name,) in cls.db.cursor.fetchall():
            cls.db.execute('DROP TABLE %s CASCADE' % table_name)

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()

    def _get_book_field_info(self):
        """Info about how Book record fields are stored and retrieved.
        """
        return (
            ('id', int, int),
            ('name', str, str),
            ('author_id', int, int),
            ('price', Decimal, Decimal),
            ('publication_date', Date, Date),
            ('is_popular', bool, bool),
        )

    def test_adapter(self):

        class Author(dbw.Model):
            """Authors catalog.
            """
            # `id` and `timestamp` fields already present
            last_name = dbw.CharField(max_length=100, comment='Author\'s last name')
            first_name = dbw.CharField(max_length=100, comment='Author\'s first name')
            created_at = dbw.DateTimeField()

            _meta = dbw.ModelOptions(
                db_name='authors',
                db_indexes=dbw.DbUnique(last_name, first_name),
            )

        class Book(dbw.Model):
            """Books catalog.
            """
            name = dbw.CharField(max_length=100, default='A very good book!!!')
            price = dbw.DecimalField(max_digits=10, decimal_places=2, default='0.00',
                                     db_index=True)  # 2 decimal places
            author = dbw.RelatedRecordField(Author, db_index=True)
            publication_date = dbw.DateField()
            is_popular = dbw.BooleanField()

        db = self.db
        for model in (Author, Book):
            for query in db.get_create_table_query(model):
                db.execute(query)
        db.commit()

        # several ways creating new records
        # Model(db, (Model.field1, field1_value), (Model.field2, field2_value), ...)
        # Model(db, Model.field1(field1_value), Model.field2(field2_value), ...)
        # Model(db, (field1_name, field1_value), (field2_name, field2_value), ...)
        # Model(db, field1_name=field1_value, field2_name=field2_value, ...)

        author_data = (
            ('first_name', 'last_name'),
            ('Sam', 'Williams'),
            ('Steven', 'Levy'),
            ('Richard', 'Stallman'),
        )
        authors = []
        for data in author_data[1:]:
            data = dict(zip(author_data[0], data))
            author = Author.objects.create(db=db, **data)
            authors.append(author)

        book_data = (
            (Book.name, 'author', 'price', 'publication_date', 'is_popular'),
            ("Free as in Freedom: Richard Stallman's Crusade for Free Software",
             authors[0], Decimal('9.55'), Date(2002, 3, 8), False),
            ("Hackers: Heroes of the Computer Revolution - 25th Anniversary Edition",
             authors[1], Decimal('14.95'), Date(2010, 3, 27), True),
            ("In The Plex: How Google Thinks, Works, and Shapes Our Lives",
             authors[1], Decimal('13.98'), Date(2011, 4, 12), False),
            ("Crypto: How the Code Rebels Beat the Government Saving Privacy in the Digital Age",
             authors[1], Decimal('23.00'), Date(2002, 1, 15), False),
            ("Книга с русским названием",
             None, Decimal('0.00'), Date(2000, 2, 29), True),
        )
        books = []
        for data in book_data[1:]:
            data = zip(book_data[0], data)
            book = Book.objects.create(db, *data)
            books.append(book)

        db.execute("SELECT COUNT(*) FROM book")
        raw = db.cursor.fetchall()
        self.assertEqual(raw[0][0], 5)

        field_info = self._get_book_field_info()
        for book_id in range(1, 4):
            db.execute("""
                SELECT id, name, author_id, price, publication_date, is_popular
                FROM book
                WHERE id = %s
            """, (book_id,))
            raw = db.cursor.fetchall()
            rows = db.select(*Book, where=(Book.id == book_id))
            book = Book.objects.get_one(db, id=book_id)
            for i, (field_name, value_type, db_type) in enumerate(field_info):
                field = getattr(Book, field_name)
                if isinstance(field, dbw.model_fields._RecordId):
                    field = getattr(Book, field._record_field.name)
                # check value type
                # value in the DB
                self.assertIsInstance(raw[0][i], db_type)
                # value in Rows instance
                self.assertIsInstance(rows.value(0, field), value_type)
                # value in the record
                self.assertIsInstance(getattr(book, field_name), value_type)
                # check value equality
                if value_type is not db_type:
                    continue
                if i == 0:
                    original_value = i  # id
                else:
                    original_value = book_data[book_id][i - 1]
                    if isinstance(original_value, dbw.Model):
                        original_value = original_value.id
                    self.assertEqual(original_value, raw[0][i])
                self.assertEqual(raw[0][i], rows.value(0, field))
                self.assertEqual(raw[0][i], getattr(book, field_name))

        # `where` in form of `(14 < Book.price < '15.00')` does not work as expected
        # as it is transformed by Python into `(14 < Book.price) and (Book.price < '15.00')`
        # resulting in `where = (Book.price < '15.00')`
        self.assertEqual(str((15 > Book.price > '14.00')), "(book.price > '14.00')")
        # SELECT query
        db.select(Book.id, where=(Book.price > '15'), limit=10)
        book = Book.objects.get_one(db, where=(Book.price > 15))

        # UPDATE query
        old_price = book.price
        new_title = 'A new title with raised price'
        last_query = db.get_last_query()
        db.update(
            Book.name(new_title),
            Book.price(Book.price + 1),
            where=(Book.id == book.id)
        )
        self.assertEqual(db._queries[-2], last_query)

        # there are still 5 books in the db - it was an update
        db.execute("SELECT COUNT(*) FROM book")
        raw = db.cursor.fetchall()
        self.assertEqual(raw[0][0], 5)

        # verify that the data was changed
        db.execute("SELECT name FROM book WHERE id = %s" % book.id)
        raw = db.cursor.fetchall()
        self.assertEqual(raw, [(new_title,)])

        book = Book.objects.get_one(db, where=(Book.id == book.id))
        self.assertEqual(db._queries[-5], last_query)
        self.assertEqual(book.price, old_price + 1)
        self.assertEqual(book.name, new_title)

        # Authors count
        list(db.select(Author.count()).dictresult())
        list(db.select(Author.first_name, Author.last_name).dictresult())

        # Selecting all fields for book with id=1
        rows = db.select(
            *(list(Book) + list(Author)),
            from_=[Book, dbw.Join(Author, Book.author == Author.id)],
            where=(Book.id == 1)
        )
        self.assertIsInstance(rows.value(0, Book.id), int)

        assert issubclass(Author.RecordNotFound, dbw.RecordNotFound)
        self.assertIsNot(Author.RecordNotFound, dbw.RecordNotFound)
        # try to get non-existent record
        self.assertRaises(Author.RecordNotFound, Author.objects.get_one, db, id=12345)

        # New saved book with wrong author
        book = Book(db, ('name', "Just for Fun."), ('author', authors[0]), ('price', '11.20'),
                    ('publication_date', '2002-12-01'))
        book.author = Author.objects.get_one(db, id=3)  # Richard Stallman (?)
        book.save()

        # Created a new author, but did not save it
        author = Author(db, **dict(first_name='Linus', last_name='Torvalds'))
        self.assertIsNone(author.id)

        book.author = author  # No! It's Linus Torvalds author of this book!
        # Assigned the book this new unsaved author
        # `book.author_id` should be None as the new author was not saved yet
        self.assertIsNone(book.author_id)
        # But `book.author` should be the one we assigned
        self.assertEqual(book.author, author)

        # Saved the new author. It should have now an id and a timestamp
        author.save()
        self.assertIsInstance(author.id, int)
        self.assertIsInstance(author.timestamp, DateTime)

        # After saving the new author `book.author_id` should have changed
        self.assertEqual(book.author_id, author.id)

        # Retreving book with id 1
        book = Book.objects.get_one(db, id=1)
        # Accessing `book.author` should automatically retrieve the author from the db
        last_query = db.get_last_query()
        book.author
        self.assertEqual(db._queries[-2], last_query)

        # Retreving book with id 1
        book = Book.objects.get_one(db, id=1, select_related=True)
        last_query = db.get_last_query()
        # Accessing `book.author` should NOT make a query to the db, as `select_related` was used
        book.author
        self.assertEqual(db.get_last_query(), last_query)

        count = Book.id.count()
        rows = db.select(Book.author, count, where=(Book.author != None),
                         groupby=Book.author, having=(count > 1), orderby=[-count, Book.author])
        self.assertEqual(rows.values, [[2, 3]])
