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

from unittest.mock import patch

from lsst.ts.logging_and_reporting.exposure_log import ExposurelogAdapter
from lsst.ts.logging_and_reporting.utils import Server


def test_exposurelog_adapter():
    initial_params = {
        "server_url": Server.usdfdev,
        "min_dayobs": "2024-01-01",
        "max_dayobs": "2024-02-01",
        "limit": 345,
        "auth_token": "my_auth_token",
    }

    adapter = ExposurelogAdapter(**initial_params)

    assert adapter.server == Server.usdfdev
    assert adapter.min_dayobs == "2024-01-01"
    assert adapter.max_dayobs == "2024-02-01"
    assert adapter.limit == 345
    assert adapter.token == "my_auth_token"
    assert "https://usdf-rsp-dev.slac.stanford.edu/exposurelog" in adapter.sources.values()


@patch("lsst.ts.logging_and_reporting.exposure_log.ExposurelogAdapter.protected_get")
def test_get_messages(mock_get):
    mock_get.return_value = (True, [], 200)
    initial_params = {
        "server_url": Server.usdfdev,
        "min_dayobs": "2024-01-01",
        "max_dayobs": "2024-02-01",
        "auth_token": "my_auth_token",
    }

    adapter = ExposurelogAdapter(**initial_params)
    adapter.get_messages(
        instrument="LATISS",
        is_human="true",
        order_by="-day_obs",
        offset=10,
        limit=5,
    )
    args, kwargs = mock_get.call_args
    actual_url = args[0]

    assert actual_url.startswith("https://usdf-rsp-dev.slac.stanford.edu/exposurelog/messages?")
    assert "instrument=LATISS" in actual_url
    assert "min_day_obs=20240101" in actual_url
    assert "max_day_obs=20240201" in actual_url
    assert "is_human=true" in actual_url
    assert "order_by=-day_obs" in actual_url
    assert "offset=10" in actual_url
    assert "limit=5" in actual_url
    assert "instrument=LATISS" in actual_url
    assert "limit=5" in actual_url


@patch("lsst.ts.logging_and_reporting.exposure_log.ExposurelogAdapter.protected_get")
def test_get_messages_defaults(mock_get):
    mock_get.return_value = (True, [], 200)
    adapter = ExposurelogAdapter(
        server_url=Server.usdfdev,
        min_dayobs="2024-01-01",
        max_dayobs="2024-02-01",
        auth_token="my_auth_token",
    )

    # Use defaults by only providing the required 'instrument' argument
    adapter.get_messages(instrument="LATISS")

    args, _ = mock_get.call_args
    actual_url = args[0]

    assert "instrument=LATISS" in actual_url

    assert "is_human=true" in actual_url
    assert "order_by=-date_added" in actual_url
    assert "limit=2500" in actual_url

    # Verify that things not provided (and not defaulted) are not here
    assert "offset=" not in actual_url
