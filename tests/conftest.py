# test/conftest.py
import os
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture(scope="session", autouse=True)
def set_test_env():
    os.environ["EXTERNAL_INSTANCE_URL"] = "https://usdf-rsp-dev.slac.stanford.edu"


@pytest.fixture(scope="module", autouse=True)
def mock_rubin_nights(request):
    mock_modules = {
        "rubin_nights": MagicMock(),
        "rubin_nights.connections": MagicMock(),
        "rubin_nights.dayobs_utils" : MagicMock(),
        "rubin_nights.rubin_scheduler_addons": MagicMock(),
        "rubin_nights.augment_visits": MagicMock(),
        "rubin_nights.observatory_status": MagicMock(),
        "rubin_nights.influx_query": MagicMock(),
    }

    patcher = patch.dict("sys.modules", mock_modules)
    patcher.start()

    def teardown():
        patcher.stop()

    request.addfinalizer(teardown)
