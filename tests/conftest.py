# test/conftest.py
import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def set_test_env():
    os.environ["EXTERNAL_INSTANCE_URL"] = "https://usdf-rsp-dev.slac.stanford.edu"
