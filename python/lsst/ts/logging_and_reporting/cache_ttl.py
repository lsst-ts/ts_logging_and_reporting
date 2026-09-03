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

"""Cache lifetimes for the Redis layer and Cache-Control headers.

Every cached datum is one of three kinds:

- Historic: fully in the past and immutable (ephemeris, closed
  nights).
- Today: changes as the night progresses.
- Mutable: can change on any dayobs (log messages, tickets), so
  historical entries have a shorter, separate TTL; entries covering
  today use the today TTLs.

Each kind has two lifetimes: a client TTL served as ``Cache-Control:
max-age`` (how long browsers and the nginx proxy may reuse a
response) and a Redis TTL (how long the adapter layer keeps the
entry). The two stack — a response built from a nearly expired Redis
entry can then sit in a client cache for its full max-age — so the
client TTLs are kept short relative to their Redis counterparts:
only the Redis copy can be flushed centrally.
"""

SECONDS_PER_DAY = 86400

HISTORIC_TTL_CLIENT = SECONDS_PER_DAY
"""Client max-age for immutable, fully historical responses."""

HISTORIC_TTL_REDIS = 30 * SECONDS_PER_DAY
"""Redis TTL for immutable historical entries."""

TODAY_TTL_CLIENT = 300
"""Client max-age for responses covering today's dayobs.

Also the RefreshWorker's default interval, so clients are never
served or use data significantly more stale than one refresh cycle.
"""

TODAY_TTL_REDIS = 900
"""Redis TTL for today's entry.

Must comfortably exceed the RefreshWorker interval so today's entry
cannot expire between refresh cycles.
"""

MUTABLE_TTL_CLIENT = 300
"""Client max-age for mutable data on past dayobs."""

MUTABLE_TTL_REDIS = 1800
"""Redis TTL for mutable entries on past dayobs.

Long enough to spare the upstream service, short enough that edits
to a past night appear within the hour.
"""
