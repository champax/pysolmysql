pysolmysql
============

Welcome to pysol

Copyright (C) 2013/2025 Laurent Labatut / Laurent Champagnac

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

