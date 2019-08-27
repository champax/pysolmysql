pysolmysql
============

Welcome to pysol

Copyright (C) 2013/2017 Laurent Labatut / Laurent Champagnac

pysolmysql is a set of simple MYSQL client Apis

They are gevent based.
They rely on pymysql.

Usage
===============

```
d_conf = {
    "host": "localhost",
    "port": 3306,
    "database": None,
    "user": "root",
    "password": "root",
    "autocommit": True,
}
        
ar = MysqlApi.exec_n(d_conf, "select user, host from mysql.user;")

for d_record in ar:
    logger.info("user=%s, host=%s", d_record["user"], d_record["host"])
```

Pool
===============

Now backed by a basic pool implementation, which support underlying backend clusters (mariadb galera for instance)

This basic pool implementation is forked and adapted from :
- https://github.com/laurentL/django-mysql-geventpool-27
- https://github.com/shunsukeaihara/django-mysql-geventpool

Pool max size
===============

Pool max size (default 10) can be specified using
```
d_conf = {
    "pool_max_size": 10,
    ...
}
```

Possible backward compatibility issue:
- If the pool is maxed, an exception will be raised

Multiple hosts
===============

Multiple hosts can be addressed in an active/active manner.

Several hosts can be specified using :
- "hosts" list (preferred)
```
d_conf = {
    "hosts": ["localhost", "127.0.0.1"],
    ...
}
```

- "host" comma separated list
```
d_conf = {
    "host": "localhost,127.0.0.1",
    ...
}
```

- "host" single entry (backward compatible mode)
```
d_conf = {
    "host": "localhost",
    ...
}
```

Source code
===============

- We are pep8 compliant (as far as we can, with some exemptions)
- We use a right margin of 360 characters (please don't talk me about 80 chars)
- All unittest files must begin with `test_` or `Test`, should implement setUp and tearDown methods
- All tests must adapt to any running directory
- The whole project is backed by gevent (http://www.gevent.org/)
- We use docstring (:return, :rtype, :param, :type etc..), they are mandatory
- We use PyCharm "noinspection", feel free to use them

Requirements
===============

- Debian 8 Jessie or greater, x64, Python 2.7

Unittests
===============

To run unittests, you will need:

- mysql installed and running, with root/root credentials

License
===============

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA


