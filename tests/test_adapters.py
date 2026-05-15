from unittest.mock import patch

from lsst.ts.logging_and_reporting.exposure_log import ExposurelogAdapter
from lsst.ts.logging_and_reporting.utils import Server


def test_exposurelog_adapter():
    initial_params = {
        "server_url": Server.usdfdev,
        "min_dayobs": "2024-01-01",
        "max_dayobs": "2024-02-01",
        "limit": 345,
        "verbose": False,
        "warning": False,
        "auth_token": "my_auth_token",
    }

    adapter = ExposurelogAdapter(**initial_params)

    assert adapter.server == Server.usdfdev
    assert adapter.min_dayobs == "2024-01-01"
    assert adapter.max_dayobs == "2024-02-01"
    assert adapter.limit == 345
    assert adapter.verbose is False
    assert adapter.warning is False
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
        is_valid="false",
    )
    args, kwargs = mock_get.call_args
    actual_url = args[0]

    assert actual_url.startswith("https://usdf-rsp-dev.slac.stanford.edu/exposurelog/messages?")
    assert "instrument=LATISS" in actual_url
    assert "min_day_obs=20240101" in actual_url
    assert "max_day_obs=20240201" in actual_url
    assert "is_human=true" in actual_url
    assert "is_valid=false" in actual_url
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
