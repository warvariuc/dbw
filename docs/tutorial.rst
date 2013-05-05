========
Tutorial
========

--------------
Installing DBW
--------------

Installing from ``pypi`` using ``pip``::

	vic@vic-X202E ~/projects » mkvirtualenv test-dbw
	Using base prefix '/usr'
	New python executable in test-dbw/bin/python3.3
	Also creating executable in test-dbw/bin/python
	Installing distribute.........done.
	Installing pip................done.

	[test-dbw] vic@vic-X202E ~/projects » pip install dbw
	Downloading/unpacking dbw
	...
	Successfully installed dbw nose
	Cleaning up...

	[test-dbw] vic@vic-X202E ~/projects » python
	Python 3.3.1 (default, Apr 17 2013, 22:30:32) 
	[GCC 4.7.3] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import dbw
	>>> dbw.__version__
	'0.1.2'
	>>>

Installing directly from ``github`` using ``pip``::

	vic@vic-X202E ~/projects » mkvirtualenv test-dbw
	Using base prefix '/usr'
	New python executable in test-dbw/bin/python3.3
	Also creating executable in test-dbw/bin/python
	Installing distribute.........done.
	Installing pip................done.
	
	[test-dbw] vic@vic-X202E ~/projects » pip install -e git://github.com/warvariuc/dbw.git#egg=dbw
	Obtaining dbw from git+git://github.com/warvariuc/dbw.git#egg=dbw
	...
	Installing collected packages: dbw, nose
	Successfully installed dbw nose
	Cleaning up...
	
	[test-dbw] vic@vic-X202E ~/projects » python
	Python 3.3.1 (default, Apr 17 2013, 22:30:32) 
	[GCC 4.7.3] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import dbw
	>>> dbw.__version__
	'0.1.2'
	>>>

Installing from source::

	vic@vic-X202E ~/projects/dbw (master) » mkvirtualenv test-dbw
	Using base prefix '/usr'
	New python executable in test-dbw/bin/python3.3
	Also creating executable in test-dbw/bin/python
	Installing distribute.........done.
	Installing pip................done.

	[test-dbw] vic@vic-X202E ~/projects/dbw (master) » python3 setup.py install
	...
	Installed /home/vic/projects/.envs/test-dbw/lib/python3.3/site-packages/dbw-0.1.2-py3.3.egg
	...
	Installed /home/vic/projects/.envs/test-dbw/lib/python3.3/site-packages/nose-1.3.0-py3.3.egg
	Finished processing dependencies for dbw==0.1.2

	[test-dbw] vic@vic-X202E ~/projects/dbw (master *) » cd ..

	[test-dbw] vic@vic-X202E ~/projects » python
	Python 3.3.1 (default, Apr 17 2013, 22:30:32) 
	[GCC 4.7.3] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import dbw
	>>> dbw.__version__
	'0.1.2'
	>>> dbw.__file__
	'/home/vic/projects/.envs/test-dbw/lib/python3.3/site-packages/dbw-0.1.2-py3.3.egg/dbw/__init__.py'
	>>>

-------------
Running tests
-------------

To run tests:

1. Put the sources somewhere, have ``nose`` and ``psycopg2`` installed.
2. Set up a test PostgreSql database.
3. Ensure that ``run-tests.py`` has the right URL for test databases.
4. Run tests

::

	vic@vic-X202E ~/projects/.envs » cd /tmp
 
	vic@vic-X202E /tmp » git clone git://github.com/warvariuc/dbw.git
	Cloning into 'dbw'...
	remote: Counting objects: 120, done.
	remote: Compressing objects: 100% (80/80), done.
	remote: Total 120 (delta 67), reused 83 (delta 36)
	Receiving objects: 100% (120/120), 61.36 KiB, done.
	Resolving deltas: 100% (67/67), done.

	vic@vic-X202E /tmp » cd dbw
 
	vic@vic-X202E /tmp/dbw (master) » ls
	CHANGES.rst  dbw  docs  LICENSE.rst  MANIFEST.in  README.rst  setup.py

	vic@vic-X202E /tmp/dbw (master) » cd dbw
 
	vic@vic-X202E /tmp/dbw/dbw (master) » ./run-tests.py 
	test_expressions (dbw.tests.test_orm.TestExpressions) ... ok
	test_model_attr (dbw.tests.test_orm.TestModelAttr) ... ok
	test_model_field_name (dbw.tests.test_orm.TestModelFields) ... ok
	test_record_values (dbw.tests.test_orm.TestModelFields) ... ok
	test_model_inheritance (dbw.tests.test_orm.TestModels) ... ok
	test_model_options (dbw.tests.test_orm.TestModels) ... ok
	runTest (dbw.tests.test_postgres_adapter.PostgresqlAdapterTest) ... ok
	runTest (dbw.tests.test_sqlite_adapter.SqliteAdapterTest) ... ok
	
	----------------------------------------------------------------------
	Ran 8 tests in 3.886s
	
	OK
 	