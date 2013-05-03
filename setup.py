#!/usr/bin/env python3

version = '0.1.0'
shortdesc = 'DataBase Wrapper: DAL + ORM'
longdesc = open('README.rst').read()
longdesc += open('CHANGES.rst').read()
longdesc += open('LICENSE.rst').read()


import sys


PYTHON_REQUIRED_VERSION = '3.3'
if sys.version < PYTHON_REQUIRED_VERSION:
    sys.exit('Python %s or newer required (you are using: %s).'
             % (PYTHON_REQUIRED_VERSION, sys.version))


import setuptools


setuptools.setup(name='dbw',
    version=version,
    description=shortdesc,
    long_description=longdesc,
    keywords='DAL Database Abstraction Layer ORM Object Relational Manager wrapper',
    author='Victor Varvariuc',
    author_email='victor.varvariuc@gmail.com',
    url='https://github.com/warvariuc/dbw',
    packages=('dbw', 'dbw.dispatch', 'dbw.tests'),
    license='BSD',
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Topic :: Database',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.3',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
    install_requires=(
        'nose',
    ),
)
