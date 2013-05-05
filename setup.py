#!/usr/bin/env python3

import sys


PYTHON_REQUIRED_VERSION = '3.3'
if sys.version < PYTHON_REQUIRED_VERSION:
    sys.exit('Python %s or newer required (you are using: %s).'
             % (PYTHON_REQUIRED_VERSION, sys.version))


import re
import os
import setuptools


longdesc = open('README.rst').read()
longdesc += '\n\n' + open('CHANGES.rst').read()
longdesc += '\n\n' + open('LICENSE.rst').read()

v_file = open(os.path.join(os.path.dirname(__file__), 'dbw', '__init__.py')).read()
version = re.search(r"^__version__ = '(.*?)'$", v_file, re.MULTILINE).group(1)

setuptools.setup(name='dbw',
    version=version,
    description='DataBase Wrapper: DAL + ORM',
    long_description=longdesc,
    keywords='DAL Database Abstraction Layer ORM Object Relational Manager wrapper',
    author='Victor Varvariuc',
    author_email='victor.varvariuc@gmail.com',
    url='https://github.com/warvariuc/dbw',
    packages=setuptools.find_packages(),
    include_package_data=True,  # install as package data files mentioned in MANIFEST.in
    zip_safe=False,
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
