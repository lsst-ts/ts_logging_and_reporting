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
# #############################################################################

# EXAMPLE:
#   pytest tests/test_all_sources.py
# Use FactoryBoy https://factoryboy.readthedocs.io/
# to generate faux source records

import datetime as dt
import unittest

import lsst.ts.logging_and_reporting.utils as ut
import pytz


class TestBackEnd(unittest.TestCase):
    """Test backend methods used by NightLog.ipynb"""

    def test_get_datetime_from_dayobs_str_1(self):
        actual = ut.get_utc_datetime_from_dayobs_str("2024-10-14")
        expected = dt.datetime(2024, 10, 14, 15, 0, tzinfo=pytz.utc)
        self.assertEqual(actual, expected)

    def test_get_datetime_from_dayobs_str_2(self):
        actual = ut.get_utc_datetime_from_dayobs_str("20241014")
        expected = dt.datetime(2024, 10, 14, 15, 0, tzinfo=pytz.utc)
        self.assertEqual(actual, expected)

    # TODO Reevaluate how to test times/strings that will be relative
    # and changing around midnight depending on the time zone


if __name__ == "__main__":
    unittest.main()
