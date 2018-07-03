# -*- coding: utf-8 -*-
"""
# ===============================================================================
#
# Copyright (C) 2013/2017 Laurent Labatut / Laurent Champagnac
#
#
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
# ===============================================================================
"""

# Imports
import logging
import unittest

from pysolbase.SolBase import SolBase

from pysolmysql.Mysql.MysqlApi import MysqlApi

logger = logging.getLogger(__name__)
SolBase.voodoo_init()
SolBase.logging_init(log_level='DEBUG', force_reset=True)


# noinspection PyBroadException
class TestMysqlApi(unittest.TestCase):
    """
    Test description
    """

    AR_CREATE_TABLES = [
        """CREATE TABLE `t1` (
          `server_id` VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """,
        """CREATE TABLE `t2` (
          `server_id` VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """,
    ]

    # noinspection PyPep8Naming
    def setUp(self):
        """
        Setup
        """

        d_conf_root = {
            "host": "localhost",
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        # exec_n
        try:
            ar = MysqlApi.exec_n(d_conf_root, "DROP DATABASE IF EXISTS pysolmysql_test;")
            logger.info("ar=%s", ar)
        except Exception as e:
            logger.debug("Ex=%s", SolBase.extostr(e))
        MysqlApi.exec_n(d_conf_root, "CREATE DATABASE IF NOT EXISTS pysolmysql_test;")

    # noinspection PyPep8Naming
    def tearDown(self):
        """
        Setup (called on destroy)
        """

        pass

    def test_multi(self):
        """
        Test
        """

        d_conf = {
            "host": "localhost",
            "port": 3306,
            "database": "pysolmysql_test",
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        ar = MysqlApi.multi_n(d_conf, TestMysqlApi.AR_CREATE_TABLES)
        logger.info("ar=%s", ar)

        ar = MysqlApi.exec_n(d_conf, "SELECT * FROM t1;")
        logger.info("ar=%s", ar)

        ar = MysqlApi.exec_n(d_conf, "SELECT * FROM t2;")
        logger.info("ar=%s", ar)

    def test_mysql_api_with_db(self):
        """
        Test
        """

        d_conf = {
            "host": "localhost",
            "port": 3306,
            "database": "mysql",
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        # exec_1
        d = MysqlApi.exec_1(d_conf, "SELECT user, host FROM user LIMIT 1;")
        logger.info("d=%s", d)
        self.assertIsInstance(d, dict)
        self.assertIn("user", d)
        self.assertIn("host", d)

        # exec_1 (must fail, bad db)
        d_conf["database"] = "zzz"
        try:
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM user LIMIT 1;")
            self.fail("Must fail, bad db")
        except Exception:
            pass

        # exec_1 (must fail, bad db even with explicit)
        d_conf["database"] = "zzz"
        try:
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")
            self.fail("Must fail, bad db with explicit")
        except Exception:
            pass

    def test_mysql_api_with_utf8(self):
        """
        Test
        """

        d_conf = {
            "host": "localhost",
            "port": 3306,
            "database": "pysolmysql_test",
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        # exec_0
        table_statement = """CREATE TABLE `ut_t1` (
            `string` VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """
        drop_table = """DROP TABLE IF EXISTS ut_t1"""

        d = MysqlApi.exec_n(d_conf, drop_table)
        logger.info("d=%s", d)
        d = MysqlApi.exec_n(d_conf, table_statement)
        logger.info("d=%s", d)

        for v in ['tamer', u'string_utf8_Ř']:
            d = MysqlApi.exec_0(d_conf, "INSERT INTO ut_t1 set string = '%s'" % v)
            logger.info("d=%s", d)
            d = MysqlApi.exec_1(d_conf, """select * from ut_t1 where string = '%s'""" % v, fix_types=True)
            logger.info("Got d=%s", d)            
            self.assertEqual(v, d['string'])

    def test_mysql_api(self):
        """
        Test
        :return:
        :rtype:
        """

        d_conf = {
            "host": "localhost",
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        # exec_n
        ar = MysqlApi.exec_n(d_conf, "show global status;")
        logger.info("ar=%s", ar)
        self.assertIsInstance(ar, list)
        for d in ar:
            self.assertIsInstance(d, dict)
            self.assertIn("Value", d)
            self.assertIn("Variable_name", d)

        # exec_n, no return (should be ok)
        ar = MysqlApi.exec_n(d_conf, "SELECT user, host FROM mysql.user WHERE user='zzz';")
        logger.info("ar=%s", ar)
        self.assertIsInstance(ar, tuple)
        self.assertEqual(len(ar), 0)

        # exec_n
        ar = MysqlApi.exec_n(d_conf, "SELECT user, host FROM mysql.user;")
        logger.info("ar=%s", ar)
        self.assertIsInstance(ar, list)
        for d in ar:
            self.assertIsInstance(d, dict)
            self.assertIn("user", d)
            self.assertIn("host", d)

        # exec_n, one record
        ar = MysqlApi.exec_n(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")
        logger.info("ar=%s", ar)
        self.assertIsInstance(ar, list)
        for d in ar:
            self.assertIsInstance(d, dict)
            self.assertIn("user", d)
            self.assertIn("host", d)

        # exec_1
        d = MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")
        logger.info("d=%s", d)
        self.assertIsInstance(d, dict)
        self.assertIn("user", d)
        self.assertIn("host", d)

        # exec_1, 2 records (must fail)
        try:
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 2;")
            self.fail("Must raise")
        except Exception:
            pass

        # exec_1, 0 records (must fail)
        try:
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user WHERE user='zzz';")
            self.fail("Must raise")
        except Exception:
            pass

        # exec_01
        d = MysqlApi.exec_01(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")
        logger.info("d=%s", d)
        self.assertIsInstance(d, dict)
        self.assertIn("user", d)
        self.assertIn("host", d)

        # exec_01, 2 records (must fail)
        try:
            MysqlApi.exec_01(d_conf, "SELECT user, host FROM mysql.user LIMIT 2;")
            self.fail("Must raise")
        except Exception:
            pass

        # exec_01, 0 records (must be ok)
        d = MysqlApi.exec_01(d_conf, "SELECT user, host FROM mysql.user WHERE user='zzz';")
        logger.info("d=%s", d)
        self.assertIsNone(d)

        # exec_0
        d = MysqlApi.exec_0(d_conf, "SELECT user, host FROM mysql.user;")
        logger.info("d=%s", d)
        self.assertIsNone(d)

        # multi_n
        d = MysqlApi.multi_n(d_conf, ["SELECT DISTINCT(user) FROM mysql.user;", "SELECT DISTINCT(host) FROM mysql.user;"])
        logger.info("d=%s", d)
        self.assertIsNone(d)
