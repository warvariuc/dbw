#!/usr/bin/env python3

import os
import sys

import nose


# sudo -u postgres psql
# CREATE USER test WITH PASSWORD 'test';
# CREATE DATABASE test OWNER test;
POSTGRESQL_TEST_DB = 'test:test@localhost/test'
SQLITE_TEST_DB = '/tmp/dbw-test.sqlite'
MYSQL_TEST_DB = ''

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_tests(verbosity=1, interactive=False):

#     import dbw
#     dbw.logger.setLevel(dbw.logging.DEBUG)
#     dbw.sql_logger.setLevel(dbw.logging.DEBUG)

    if POSTGRESQL_TEST_DB:
        os.environ.setdefault('POSTGRESQL_TEST_DB', POSTGRESQL_TEST_DB)
    if SQLITE_TEST_DB:
        os.environ.setdefault('SQLITE_TEST_DB', os.path.join(BASE_DIR, SQLITE_TEST_DB))
    if MYSQL_TEST_DB:
        os.environ.setdefault('MYSQL_TEST_DB', MYSQL_TEST_DB)

    nose_args = sys.argv.copy()
    nose_args.extend(['-v'])

    nose.run(argv=nose_args)


if __name__ == "__main__":
    run_tests()
