#!/usr/bin/env python3

import unittest
import os

# sudo -u postgres psql
# CREATE USER test WITH PASSWORD 'test';
# CREATE DATABASE test;
# GRANT ALL PRIVILEGES ON DATABASE test TO test;
os.environ['POSTGRESQL_TEST_DB_URL'] = 'postgresql://test:test@localhost/test'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ['SQLITE_TEST_DB_PATH'] = os.path.join(BASE_DIR, 'test.sqlite')
# os.environ['MYSQL_TEST_DB_URL'] = 'mysql://test:test@localhost/test'

all_tests = unittest.TestLoader().discover('dbw.tests')
unittest.TextTestRunner(verbosity=2).run(all_tests)
