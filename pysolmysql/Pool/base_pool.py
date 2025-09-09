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

import logging
from threading import Lock

from gevent import queue

from pysolmeters.Meters import Meters

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

        # Lock for acquire/release
        self.pool_lock = Lock()

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
        # TODO : In case client cannot release (greenlet kill) : add a spawn_later to protect pull exhaust (+ kill the connection in this case)
        # TODO : this requires a timeout by config (lets say 60 sec by default)
        :return: object
        :rtype object
        """

        with self.pool_lock:

            Meters.aii("k.db_pool.base.call.connection_acquire")

            if self.pool.qsize() > 0:
                # ------------------------------
                # GET CONNECTION FROM POOL
                # ------------------------------
                conn = self.pool.get()

                # Ping it
                if not self._connection_ping(conn):
                    # Failed => close it
                    self._connection_close(conn)

                    # Re-create a new one (we just closed a connection)
                    conn = self._connection_create()

                # Send it back
                return conn
            elif self.size >= self.max_size:
                # ------------------------------
                # POOL MAXED => ERROR
                # ------------------------------
                Meters.aii("k.db_pool.base.pool_maxed")
                raise Exception("Pool maxed, size=%s, max_size=%s" % (self.size, self.max_size))
            else:
                # ------------------------------
                # POOL NOT MAXED, NO CONNECTION IN POOL => NEW CONNECTION
                # ------------------------------
                try:
                    conn = self._connection_create()
                    self.size += 1
                    Meters.aii("k.db_pool.base.cur_size", increment_value=1)
                    Meters.ai("k.db_pool.base.max_size").set(max(Meters.aig("k.db_pool.base.max_size"), Meters.aig("k.db_pool.base.cur_size")))
                except Exception:
                    raise
                return conn

    def connection_release(self, conn):
        """
        Put a connection back in the pool
        :param conn: object
        :type conn: object
        """

        with self.pool_lock:

            Meters.aii("k.db_pool.base.call.connection_release")

            # If none, decrement + return
            if conn is None:
                return

            # Put it back
            try:
                self.pool.put(conn)
            except queue.Full:
                # If full, close it
                self._connection_close(conn)

    def close_all(self):
        """
        Close all connections
        """
        n = 0
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            self._connection_close(conn)
            n += 1

        Meters.aii("k.db_pool.base.cur_size", increment_value=-n)
        Meters.ai("k.db_pool.base.max_size").set(max(Meters.aig("k.db_pool.base.max_size"), Meters.aig("k.db_pool.base.cur_size")))
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
