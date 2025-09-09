"""
# -*- coding: utf-8 -*-
# ===============================================================================
#
# Copyright (C) 2013/2025 Laurent Labatut / Laurent Champagnac
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

# noinspection PyUnresolvedReferences
import ujson
import logging
from contextlib import closing

from threading import Lock
from pysolmeters.Meters import Meters

from pysolmysql.Pool.mysql_pool import MysqlConnectionPool

logger = logging.getLogger(__name__)


class MysqlApi(object):
    """
    Mysql Api
    """

    # Lock
    POOL_LOCK = Lock()

    # Static pool instances (hash from config dict => MysqlConnectionPool)
    D_POOL_INSTANCES = dict()

    @classmethod
    def reset_pools(cls):
        """
        Reset all pools
        """

        with cls.POOL_LOCK:
            for s_hash, pool in cls.D_POOL_INSTANCES.items():
                logger.info("Closing pool, s_hash=%s", s_hash)
                pool.close_all()
            cls.D_POOL_INSTANCES = dict()

    @classmethod
    def _get_pool_hash(cls, conf_dict):
        """
        Get pool hash
        :param conf_dict: dict
        :type conf_dict: dict
        :return: str
        :rtype: str
        """

        s_hash = str(hash(ujson.dumps(conf_dict, sort_keys=True)))
        return s_hash

    @classmethod
    def _get_pool(cls, conf_dict):
        """
        Init static pool
        :param conf_dict: dict
        :type conf_dict: dict
        :return pysolmysql.Pool.mysql_pool.MysqlConnectionPool
        :rtype pysolmysql.Pool.mysql_pool.MysqlConnectionPool
        """

        # Hash
        s_hash = cls._get_pool_hash(conf_dict)

        # Alloc if needed
        if s_hash not in cls.D_POOL_INSTANCES:
            with cls.POOL_LOCK:
                if s_hash not in cls.D_POOL_INSTANCES:
                    cls.D_POOL_INSTANCES[s_hash] = MysqlConnectionPool(conf_dict)
                    logger.info("Allocated pool, s_hash=%s, pool.len=%s", s_hash, len(cls.D_POOL_INSTANCES))
                    Meters.aii("k.db_pool.hash.cur")

        # Over
        return cls.D_POOL_INSTANCES[s_hash]

    @classmethod
    def _fix_type(cls, data):
        """
        Fix type
        :param data: data
        """
        if isinstance(data, bytearray):
            return data.decode("utf-8")
        else:
            return data

    @classmethod
    def exec_0(cls, conf_dict, statement):
        """
        Execute a sql statement, returning row affected.
        :param conf_dict: configuration dict
        :type conf_dict: dict
        :param statement: statement to execute
        :type statement: str
        :rtype: int
        :return rows affected
        """

        cnx = None
        try:
            cnx = cls._get_pool(conf_dict).connection_acquire()
            with closing(cnx.cursor()) as cur:
                cur.execute(statement)
                return cur.rowcount
        finally:
            cls._get_pool(conf_dict).connection_release(cnx)

    @classmethod
    def exec_n(cls, conf_dict, statement, fix_types=True):
        """
        Execute a sql statement, returning 0..N rows
        :param conf_dict: configuration dict
        :type conf_dict: dict
        :param statement: statement to execute
        :type statement: str
        :param fix_types: If true, fix data type
        :type fix_types: bool
        :return list of dict.
        :rtype list
        """

        cnx = None
        try:
            cnx = cls._get_pool(conf_dict).connection_acquire()
            with closing(cnx.cursor()) as cur:
                cur.execute(statement)
                rows = cur.fetchall()
                for row in rows:
                    logger.debug("row=%s", row)
                    for k, v in row.items():
                        logger.debug("k=%s, %s, %s", k, type(v), v)
                        if fix_types:
                            row[k] = MysqlApi._fix_type(v)
                return rows
        finally:
            cls._get_pool(conf_dict).connection_release(cnx)

    @classmethod
    def exec_1(cls, conf_dict, statement, fix_types=True):
        """
        Execute a sql statement, returning 1 row.
        Method will fail if 1 row is not returned.
        :rtype: object
        :param conf_dict: configuration dict
        :type conf_dict: dict
        :param statement: statement to execute
        :type statement: str
        :param fix_types: If true, fix data type
        :type fix_types: bool
        :return dict
        :rtype dict
        """

        cnx = None
        try:
            cnx = cls._get_pool(conf_dict).connection_acquire()
            with closing(cnx.cursor()) as cur:
                cur.execute(statement)
                rows = cur.fetchall()
                for row in rows:
                    logger.debug("row=%s", row)
                    for k, v in row.items():
                        logger.debug("k=%s, %s, %s", k, type(v), v)
                        if fix_types:
                            row[k] = MysqlApi._fix_type(v)
                if len(rows) != 1:
                    raise Exception("Invalid row len, expecting 1, having={0}".format(len(rows)))
                return rows[0]
        finally:
            cls._get_pool(conf_dict).connection_release(cnx)

    @classmethod
    def exec_01(cls, conf_dict, statement, fix_types=True):
        """
        Execute a sql statement, returning 0 or 1 row.
        Method will fail if 0 or 1 row is not returned.
        :param conf_dict: configuration dict
        :type conf_dict: dict
        :param statement: statement to execute
        :type statement: str
        :param fix_types: If true, fix data type
        :type fix_types: bool
        :return dict, None
        :rtype dict, None
        """

        cnx = None
        try:
            cnx = cls._get_pool(conf_dict).connection_acquire()
            with closing(cnx.cursor()) as cur:
                cur.execute(statement)
                rows = cur.fetchall()
                for row in rows:
                    logger.debug("row=%s", row)
                    for k, v in row.items():
                        logger.debug("k=%s, %s, %s", k, type(v), v)
                        if fix_types:
                            row[k] = MysqlApi._fix_type(v)
                if len(rows) == 0:
                    return None
                elif len(rows) != 1:
                    raise Exception("Invalid row len, expecting 1, having={0}".format(len(rows)))
                else:
                    return rows[0]
        finally:
            cls._get_pool(conf_dict).connection_release(cnx)

    @classmethod
    def multi_n(cls, conf_dict, ar_statement):
        """
        Execute multiple sql statement, reading nothing from mysql.
        :type conf_dict: dict
        :param ar_statement: list of statements to execute (for instance, batch of insert or whatever)
        :type ar_statement: list
        """

        cnx = None
        try:
            cnx = cls._get_pool(conf_dict).connection_acquire()
            with closing(cnx.cursor()) as cur:
                for s in ar_statement:
                    cur.execute(s)
        finally:
            cls._get_pool(conf_dict).connection_release(cnx)
