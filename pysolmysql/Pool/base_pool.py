"""
# -*- coding: utf-8 -*-
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

import logging
from gevent import queue

logger = logging.getLogger(__name__)


class DatabaseConnectionPool(object):
    """
    Connection pool.
    """

    def __init__(self, conf_dict):
        """
        Init
        :param conf_dict: dict
        :type conf_dict: dict
        """

        # Store
        self.conf_dict = conf_dict

        # Max size
        self.max_size = self.conf_dict.get("pool_max_size", 10)

        # Alloc
        self.pool = queue.Queue(maxsize=self.max_size)

        # Init
        self.size = 0

    def connection_acquire(self):
        """
        Get a connection
        :return: object
        :rtype object
        """

        if self.size >= self.max_size or self.pool.qsize():
            conn = self.pool.get()
            if not self._connection_ping(conn):
                # noinspection PyBroadException
                try:
                    self._connection_close(conn)
                except Exception:
                    pass
                conn = self._connection_create()
            return conn
        else:
            self.size += 1
            try:
                conn = self._connection_create()
            except Exception:
                self.size -= 1
                raise
            return conn

    def connection_release(self, conn):
        """
        Put a connection back in the pool
        :param conn: object
        :type conn: object
        """

        # If none, decrement + return
        if conn is None:
            self.size -= 1
            return

        # Put it back
        try:
            self.pool.put(conn, timeout=2)
        except queue.Full:
            # If full, close it
            self._connection_close(conn)

    def close_all(self):
        """
        Close all connections
        """
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            self._connection_close(conn)

        self.size = 0

    # ------------------------------------------------
    # OVERRIDES
    # ------------------------------------------------

    def _connection_create(self, *args, **kwargs):
        """
        Create connection
        :param args: object
        :type args: object
        :param kwargs: object
        :type kwargs: object
        :return object
        :rtype object
        """
        raise NotImplementedError("create_connection")

    def _connection_ping(self, conn):
        """
        Ping connection
        :param conn: object
        :type conn: object
        :return bool
        :rtype bool
        """

        raise NotImplementedError("ping_connection")

    def _connection_close(self, conn):
        """
        Close a connection.
        Must not raise anything.
        :param conn: object
        :type conn: object
        """

        raise NotImplementedError("close_connection")
