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

	vic@vic-X202E ~/projects » mkvirtualenv test-dbw
	Using base prefix '/usr'
	New python executable in test-dbw/bin/python3.3
	Also creating executable in test-dbw/bin/python
	Installing distribute.........done.
	Installing pip................done.

	[test-dbw] vic@vic-X202E ~/projects » cd dbw 

	[test-dbw] vic@vic-X202E ~/projects/dbw (master) » ./setup.py install
	running install
	...	
	Installed /home/vic/projects/.envs/test-dbw/lib/python3.3/site-packages/dbw-0.1.2-py3.3.egg
	...	
	Installed /home/vic/projects/.envs/test-dbw/lib/python3.3/site-packages/nose-1.3.0-py3.3.egg
	Finished processing dependencies for dbw==0.1.2

	[test-dbw] vic@vic-X202E ~/projects/dbw (master) » python
	Python 3.3.1 (default, Apr 17 2013, 22:30:32) 
	[GCC 4.7.3] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import dbw
	>>> dbw.__version__
	'0.1.2'
	>>> 
	