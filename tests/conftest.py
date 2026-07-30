# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import os
import threading
import time

import pytest


@pytest.fixture(scope="session", autouse=True)
def set_test_env():
    os.environ["EXTERNAL_INSTANCE_URL"] = "https://usdf-rsp-dev.slac.stanford.edu"


class FakeRedis:
    """Minimal in-memory stand-in for redis-py.

    Supports the subset the caching layer uses: get, set (with nx/ex),
    delete, and exists, with real time-based expiry (float ``ex`` is
    accepted so tests can use tiny TTLs). Thread-safe, since the
    single-flight tests exercise it concurrently.
    The last TTL passed for each key is recorded in ``ttls`` so tests
    can assert on TTL policy.
    """

    def __init__(self):
        self._data = {}  # key -> (value: bytes, expires_at: float | None)
        self._mutex = threading.Lock()
        self.ttls = {}

    def _purge(self, key):
        entry = self._data.get(key)
        if entry is not None and entry[1] is not None and time.monotonic() >= entry[1]:
            del self._data[key]

    def get(self, key):
        with self._mutex:
            self._purge(key)
            entry = self._data.get(key)
            return entry[0] if entry is not None else None

    def set(self, key, value, ex=None, nx=False):
        with self._mutex:
            self._purge(key)
            if nx and key in self._data:
                return None
            if isinstance(value, str):
                value = value.encode()
            expires_at = time.monotonic() + ex if ex is not None else None
            self._data[key] = (value, expires_at)
            self.ttls[key] = ex
            return True

    def delete(self, *keys):
        with self._mutex:
            return sum(1 for key in keys if self._data.pop(key, None) is not None)

    def exists(self, key):
        with self._mutex:
            self._purge(key)
            return 1 if key in self._data else 0

    def keys(self):
        with self._mutex:
            for key in list(self._data):
                self._purge(key)
            return list(self._data)

    def flushdb(self):
        with self._mutex:
            self._data.clear()
            return True


@pytest.fixture
def fake_redis():
    return FakeRedis()
