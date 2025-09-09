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

# This is forked and updated from : https://github.com/laurentL/django-mysql-geventpool-27

# Which was forked : https://github.com/shunsukeaihara/django-mysql-geventpool

# NOTE :
# we do not close connection or timeout them, we assume underlying backend (mariadb) will close inactive connections on its end

# TODO : handle initial connection opening (for warm-up the pool when allocating)
# TODO : handle connection_acquire with timeout (in case pool is full - currently we raise immediatly)
# TODO : handle better locking in connection_acquire (global lock currently)
