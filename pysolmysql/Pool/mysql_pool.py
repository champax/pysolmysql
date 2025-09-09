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
import copy
import logging
import random

import pymysql
import time
from pymysql.cursors import DictCursor
from pysolbase.SolBase import SolBase
from pysolmeters.Meters import Meters

from pysolmysql.Pool.base_pool import DatabaseConnectionPool

logger = logging.getLogger(__name__)

# Init random
random.seed()


class MysqlConnectionPool(DatabaseConnectionPool):
    """
    Mysql connection pool, gevent compliant.

    This support multiple target hosts.

    In multiple target hosts mode, this is intended to be used with a mariadb active/active galera cluster.
    """

    # Public sample config dict
    # FOR : pysolmysql.Pool.mysql_pool.MysqlConnectionPool#create_connection
    # FOR : pysolmysql.Mysql.MysqlApi.MysqlApi
    PUBLIC_SAMPLE_CONFIG_DICT = {
        # HIGH PRIO
        "hosts": ["127.0.0.1", "127.0.0.1"],
        # LOW PRIO, DEPRECATED
        # Can be
        # - "host": "127.0.0.1"
        # - "host": "127.0.0.1,127.0.0.1"
        "host": "127.0.0.1",
        "port": 3306,
        "database": "mysql",
        "user": "your_account",
        "password": "your_password",
        "autocommit": True,
        "encoding": "utf8",
        # Pool
        "pool_max_size": 10,
    }

    # "unix" or "host" (not both), "host" has precedence
    # FOR : pysolmysql.Pool.mysql_pool.MysqlConnectionPool#_get_connection
    INTERNAL_SAMPLE_CONFIG_DICT = {
        "host": "127.0.0.1",
        "unix": "/var/run/mysqld/mysqld.sock",
        "port": 3306,
        "database": "mysql",
        "user": "your_account",
        "password": "your_password",
        "autocommit": True,
        "encoding": "utf8",
    }

    def __init__(self, conf_dict):
        """
        Init
        :param conf_dict: dict
        :type conf_dict: dict
        """

        Meters.aii("k.db_pool.mysql.call.__init")

        # Base
        super(MysqlConnectionPool, self).__init__(conf_dict)

        # Init us
        self.host_status = dict()

        # Check
        if "hosts" not in self.conf_dict and "host" not in self.conf_dict and "unix" not in self.conf_dict:
            raise Exception("No server specified (hosts, host, unix not found in conf_dict")

        # Init, "hosts" first
        if "hosts" in self.conf_dict:
            for host in self.conf_dict["hosts"]:
                self.host_status[host] = 0.0
        elif "host" in self.conf_dict:
            logger.warning("Using deprecated entry, prefer using 'hosts', got host=%s", self.conf_dict["host"])
            for host in self.conf_dict["host"].split(","):
                self.host_status[host] = 0.0
        elif "unix" in self.conf_dict:
            for host in self.conf_dict["unix"].split(","):
                self.host_status[host] = 0.0

    # ------------------------------------------------
    # HELPERS
    # ------------------------------------------------

    @classmethod
    def _get_connection(cls, conf_dict):
        """
        Get a connection
        :param conf_dict: dict
        :type conf_dict: dict
        :return: pymysql.connections.Connection
        :rtype: pymysql.connections.Connection
        """

        Meters.aii("k.db_pool.mysql.call._get_connection")

        # DOC :
        #     def __init__(self, host=None, user=None, password="",
        #          database=None, port=3306, unix_socket=None,
        #          charset='', sql_mode=None,
        #          read_default_file=None, conv=decoders, use_unicode=None,
        #          client_flag=0, cursorclass=Cursor, init_command=None,
        #          connect_timeout=None, ssl=None, read_default_group=None,
        #          compress=None, named_pipe=None, no_delay=None,
        #          autocommit=False, db=None, passwd=None, local_infile=False,
        #          max_allowed_packet=16*1024*1024, defer_connect=False,
        #          auth_plugin_map={}):

        logger.debug("mysql connect, server=%s:%s, unix=%s, user=%s, db=%s, enc=%s",
                     conf_dict.get("host"), conf_dict.get("port"), conf_dict.get("unix"),
                     conf_dict.get("user"),
                     conf_dict.get("database"),
                     conf_dict.get("encoding", "utf8")
                     )

        # Host
        h = conf_dict.get("host")

        # Unix detection (IF host startswith "/", we assume unix, even if unix not specified)
        if h and not h.startswith("/"):
            c = pymysql.connect(
                host=conf_dict["host"],
                port=int(conf_dict["port"]),

                db=conf_dict["database"],

                user=conf_dict["user"],
                password=conf_dict["password"],

                autocommit=conf_dict["autocommit"],

                charset=conf_dict.get("encoding", "utf8"),

                cursorclass=DictCursor
            )
            return c

        # Unix
        h = conf_dict.get("unix")
        if not h:
            # Fallback host if unix format
            h = conf_dict.get("host")
            # Must start with "/"
            if not h.startswith("/"):
                h = None

        # Unix try
        if h:
            c = pymysql.connect(
                unix_socket=conf_dict["unix"],

                db=conf_dict["database"],

                user=conf_dict["user"],
                password=conf_dict["password"],

                autocommit=conf_dict["autocommit"],

                cursorclass=DictCursor
            )
            return c

        # Nothing
        return None

    def _get_random_host(self):
        """
        Return a host in HOSTS_STATUS where the host is up
        :return str,bool (False)
        :rtype str,bool
        """
        now = time.time()
        hosts_up = [host for host, prison in self.host_status.items() if prison < now]
        try:
            host = random.choice(hosts_up)
            return host
        except IndexError:
            return False

    # ------------------------------------------------
    # OVERRIDES
    # ------------------------------------------------

    def _connection_create(self):
        """
        Create a connection, trying all available hosts if required (and disabling them if required)

        - conf_dict["hosts"] = ["host1", "host2"]

        where each host entry can be :
        - ip address (127.0.0.1)
        - host name (localhost)
        - unix socket name (/var/run/mysqld/mysqld.sock)

        :return: pymysql.connections.Connection
        :rtype: pymysql.connections.Connection
        """

        Meters.aii("k.db_pool.mysql.call._connection_create")

        # ------------------------
        # Try to get a connection
        # ------------------------
        out_conn = None
        while out_conn is None:
            # Pick a host
            host = self._get_random_host()

            # Check it
            if not host:
                Meters.aii("k.db_pool.mysql.hosts.all_down")
                raise Exception("No mysql host available, %s are down" % self.host_status.keys())

            # This host seems up => try open a connection
            try:
                # Build the dict for underlying _get_connection
                d_local = copy.deepcopy(self.conf_dict)
                if "hosts" in d_local:
                    del d_local["hosts"]
                d_local["host"] = host

                # Open it
                out_conn = self._get_connection(d_local)

                # Ping it (underlying base pool do NOT do it when opening connection)
                if not self._connection_ping(out_conn):
                    raise Exception("Connection ping failed")
            except Exception as e:
                # NOTE :
                # - We disable the host for ALL errors (even if DatabaseError can be raised when database do not exist but when the server is up...)

                # Deactivate host
                Meters.aii("k.db_pool.mysql.hosts.deactivate_one")
                logger.error("Host de-activate for 1 minute, host=%s, ex=%s", host, SolBase.extostr(e))
                self.host_status[host] = time.time() + 60.0
                # Kick connection
                self._connection_close(out_conn)

                # Reset
                out_conn = None

        # Over
        return out_conn

    def _connection_ping(self, conn):
        """
        Ping connection

        This sends a ping, write/read toward mysql.

        :param conn: pymysql.connections.Connection
        :type conn: pymysql.connections.Connection
        :return bool
        :rtype bool
        """

        Meters.aii("k.db_pool.mysql.call._connection_ping")

        # noinspection PyBroadException
        try:
            # TODO : PING MODE MUST BE CONFIGURABLE
            conn.ping(reconnect=False)
        except Exception as e:
            Meters.aii("k.db_pool.mysql.ex_ping")
            logger.debug("Ping failed, obj=%s, ex=%s", conn, SolBase.extostr(e))
            return False
        else:
            return True

    def _connection_close(self, conn):
        """
        Close a connection
        Must not raise anything.
        :param conn: pymysql.connections.Connection
        :type conn: pymysql.connections.Connection
        """

        Meters.aii("k.db_pool.mysql.call._connection_close")

        # noinspection PyBroadException
        try:
            if conn:
                conn.close()
        except Exception as e:
            # Don't care of exception in case of closing
            Meters.aii("k.db_pool.mysql.ex_close")
            logger.debug("Close exception (non fatal), ex=%s", SolBase.extostr(e))
