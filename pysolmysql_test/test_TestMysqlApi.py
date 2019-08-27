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

from gevent import Greenlet
from gevent.event import Event
from pymysql import ProgrammingError
from pysolbase.SolBase import SolBase
from pysolmeters.AtomicInt import AtomicIntSafe
from pysolmeters.Meters import Meters

from pysolmysql.Mysql.MysqlApi import MysqlApi

logger = logging.getLogger(__name__)
SolBase.voodoo_init()
SolBase.logging_init(log_level='INFO', force_reset=True)


# noinspection PyBroadException,SqlNoDataSourceInspection,SqlResolve
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

        MysqlApi.reset_pools()
        Meters.reset()

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

        # Full reset
        MysqlApi.reset_pools()
        Meters.reset()

    # noinspection PyPep8Naming
    def tearDown(self):
        """
        Setup (called on destroy)
        """

        SolBase.logging_init(log_level='DEBUG', force_reset=True)
        Meters.write_to_logger()
        SolBase.logging_init(log_level='INFO', force_reset=True)

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
            # noinspection PyUnresolvedReferences
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

    def test_pool_basic(self):
        """
        Test pool, basic
        """

        MysqlApi.reset_pools()
        Meters.reset()

        d_conf = {
            "hosts": ["localhost", "127.0.0.1"],
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        for _ in range(0, 10):
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")

        # Check it
        self.assertEquals(Meters.aig("k.db_pool_hash.cur"), 1)

        self.assertEquals(Meters.aig("k.db_pool_base.cur_size"), 1)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), 10)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_release"), 10)

        self.assertEquals(Meters.aig("k.db_pool_mysql.call.__init"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_create"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._get_connection"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_ping"), 10)

    def test_pool_basic_err(self):
        """
        Test pool, basic
        """

        MysqlApi.reset_pools()
        Meters.reset()

        d_conf = {
            "hosts": ["localhost", "127.0.0.1"],
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        for _ in range(0, 10):
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")

        # Error
        try:
            MysqlApi.exec_1(d_conf, "SELECT zzz FROM mysql.no_table;")
        except ProgrammingError as e:
            logger.debug("Expected ex=%s", SolBase.extostr(e))

        # Check it
        self.assertEquals(Meters.aig("k.db_pool_hash.cur"), 1)

        self.assertEquals(Meters.aig("k.db_pool_base.cur_size"), 1)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), 11)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_release"), 11)

        self.assertEquals(Meters.aig("k.db_pool_mysql.call.__init"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_create"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._get_connection"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_ping"), 11)

    def test_pool_basic_bad_db(self):
        """
        Test pool, basic
        """

        MysqlApi.reset_pools()
        Meters.reset()

        d_conf = {
            "hosts": ["localhost", "127.0.0.1"],
            "port": 3306,
            "database": "no_db",
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        try:
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM no_db.user LIMIT 1;")
        except Exception as e:
            logger.debug("Expected ex=%s", SolBase.extostr(e))

        # Check it
        self.assertEquals(Meters.aig("k.db_pool_hash.cur"), 1)

        self.assertEquals(Meters.aig("k.db_pool_base.cur_size"), 0)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), 1)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_release"), 1)

        self.assertEquals(Meters.aig("k.db_pool_mysql.call.__init"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_create"), 1)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._get_connection"), 2)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_close"), 2)

        self.assertEquals(Meters.aig("k.db_pool_mysql.hosts.deactivate_one"), 2)
        self.assertEquals(Meters.aig("k.db_pool_mysql.hosts.all_down"), 1)

    def test_pool_basic_x2(self):
        """
        Test pool, basic
        """

        MysqlApi.reset_pools()
        Meters.reset()

        d_conf = {
            "hosts": ["localhost", "127.0.0.1"],
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        for _ in range(0, 10):
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")

        d_conf = {
            "hosts": ["localhost", "localhost"],
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
        }

        for _ in range(0, 10):
            MysqlApi.exec_1(d_conf, "SELECT user, host FROM mysql.user LIMIT 1;")

        # Check it
        self.assertEquals(Meters.aig("k.db_pool_hash.cur"), 1 * 2)

        self.assertEquals(Meters.aig("k.db_pool_base.cur_size"), 1 * 2)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), 10 * 2)
        self.assertEquals(Meters.aig("k.db_pool_base.call.connection_release"), 10 * 2)

        self.assertEquals(Meters.aig("k.db_pool_mysql.call.__init"), 1 * 2)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_create"), 1 * 2)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._get_connection"), 1 * 2)
        self.assertEquals(Meters.aig("k.db_pool_mysql.call._connection_ping"), 10 * 2)

    # ============================
    # BENCH
    # ============================

    def _run_mysql_bench(self, event):
        """
        Run
        """

        d_conf = {
            "hosts": ["localhost", "127.0.0.1"],
            "port": 3306,
            "database": None,
            "user": "root",
            "password": "root",
            "autocommit": True,
            "pool_max_size": self.pool_max,
        }

        # Wait
        self.gorun_event.wait()

        # Go
        cur_count = 0
        logger.debug("Entering now")
        self.thread_running.increment()
        self.thread_running_ok.increment()
        try:
            while not self.run_event.isSet():
                cur_count += 1
                try:
                    MysqlApi.exec_1(d_conf, self.pool_sql)
                except Exception as e:
                    self.exception_raised += 1
                    logger.warning("Ex=%s", SolBase.extostr(e))
                    self.thread_running_ok.increment(-1)
                    return
                finally:
                    pass
        finally:
            self.assertGreater(cur_count, 0)
            logger.debug("Exiting")
            event.set()
            self.thread_running.increment(-1)

    def _go_greenlet(self, greenlet_count, pool_max, sql="SELECT user, host FROM mysql.user LIMIT 1;", check_exception=True):
        """
        Doc
        :param greenlet_count: greenlet_count
        :type greenlet_count: int
        :param pool_max: Pool max size
        :type pool_max: int
        :param sql: str
        :type sql: str
        :param check_exception: bool
        :type check_exception: bool
        """

        MysqlApi.reset_pools()
        Meters.reset()

        g_event = None
        g_array = None
        try:
            # Settings
            g_count = greenlet_count
            g_ms = 5000

            # Go
            self.pool_max = pool_max
            self.run_event = Event()
            self.exception_raised = 0
            self.pool_sql = sql
            self.thread_running = AtomicIntSafe()
            self.thread_running_ok = AtomicIntSafe()

            # Signal
            self.gorun_event = Event()

            # Alloc greenlet
            g_array = list()
            g_event = list()
            for _ in range(0, g_count):
                greenlet = Greenlet()
                g_array.append(greenlet)
                g_event.append(Event())

            # Run them
            for idx in range(0, len(g_array)):
                greenlet = g_array[idx]
                event = g_event[idx]
                greenlet.spawn(self._run_mysql_bench, event)
                SolBase.sleep(0)

            # Signal
            self.gorun_event.set()

            # Wait a bit
            dt = SolBase.mscurrent()
            while SolBase.msdiff(dt) < g_ms:
                SolBase.sleep(1000)
                # Stat
                ms = SolBase.msdiff(dt)
                sec = float(ms / 1000.0)
                total_acquire = Meters.aig("k.db_pool_base.call.connection_acquire")
                per_sec_acquire = round(float(total_acquire) / sec, 2)
                total_release = Meters.aig("k.db_pool_base.call.connection_release")
                per_sec_release = round(float(total_release) / sec, 2)

                logger.info("Running..., run=%s, ok=%s, ps.ack/rel=%s/%s", self.thread_running.get(), self.thread_running_ok.get(), per_sec_acquire, per_sec_release)
                if check_exception:
                    self.assertEqual(self.exception_raised, 0)

            # Over, signal
            logger.info("Signaling")
            self.run_event.set()

            # Wait
            for g in g_event:
                g.wait(30.0)
                self.assertTrue(g.isSet())

            g_event = None
            g_array = None

            # Check it
            self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), Meters.aig("k.db_pool_base.call.connection_release"))

            if check_exception:
                self.assertEquals(Meters.aig("k.db_pool_base.call.connection_acquire"), Meters.aig("k.db_pool_mysql.call._connection_ping"))

            self.assertLessEqual(Meters.aig("k.db_pool_mysql.call._get_connection"), pool_max)
            self.assertLessEqual(Meters.aig("k.db_pool_mysql.call._connection_create"), pool_max)
            self.assertLessEqual(Meters.aig("k.db_pool_hash.cur"), 1)
            self.assertLessEqual(Meters.aig("k.db_pool_base.cur_size"), pool_max)
            self.assertLessEqual(Meters.aig("k.db_pool_base.max_size"), pool_max)

            self.assertEquals(Meters.aig("k.db_pool_mysql.call.__init"), 1)
        finally:
            self.run_event.set()
            if g_event:
                for g in g_event:
                    g.set()

            if g_array:
                for g in g_array:
                    g.kill()

    def test_bench_greenlet_1_1(self):
        """
        Test
        :return:
        """
        self._go_greenlet(greenlet_count=1, pool_max=1)

    def test_bench_greenlet_100_100(self):
        """
        Test
        :return:
        """
        self._go_greenlet(greenlet_count=100, pool_max=100)

    def test_bench_greenlet_100_100_sleep(self):
        """
        Test
        :return:
        """
        self._go_greenlet(greenlet_count=100, pool_max=100, sql="SELECT SLEEP(1);")

        self.assertEquals(Meters.aig("k.db_pool_base.pool_maxed"), 0)

    def test_bench_greenlet_100_10_maxed_sleep(self):
        """
        Test
        :return:
        """
        self._go_greenlet(greenlet_count=100, pool_max=10, sql="SELECT SLEEP(1);", check_exception=False)

        # Must have some maxed
        self.assertGreater(Meters.aig("k.db_pool_base.pool_maxed"), 0)
