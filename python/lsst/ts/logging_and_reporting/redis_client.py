#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Shared Redis client for the adapter cache layer.

One client (holding one connection pool) is created at application
startup and passed to every adapter and the ``RefreshWorker``, keeping
the total number of connections to Redis bounded.

The server itself must be configured as a cache, not a datastore:
``maxmemory`` with ``allkeys-lru`` eviction and persistence disabled.
The dev docker-compose stack (frontend repo) provides this; production
deployments must match it.

Redis is assumed always available — if the server is running, Redis
is reachable; no fallback path is implemented. The socket timeouts
below only bound how long a request can hang if that assumption
breaks.
"""

import functools
import logging
import os

import redis

logger = logging.getLogger(__name__)


def create_redis_client() -> redis.Redis:
    """Create the shared Redis client from environment configuration.

    Environment variables
    ---------------------
    REDIS_HOST
        Hostname of the Redis server (default ``localhost``; the dev
        compose stack sets ``redis``).
    REDIS_PORT
        Port of the Redis server (default ``6379``).
    REDIS_DB
        Redis logical database number (default ``0``).

    Returns
    -------
    `redis.Redis`
        Client with its own connection pool. Call once at application
        startup and share the instance.
    """
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    db = int(os.environ.get("REDIS_DB", "0"))
    logger.info(f"Creating Redis client for {host}:{port} (db {db})")
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


@functools.cache
def get_redis_client() -> redis.Redis:
    return create_redis_client()
