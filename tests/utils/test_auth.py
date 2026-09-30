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

import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    get_auth_header,
    get_jira_hostname,
    retrieve_access_token,
)

AUTH_SOURCE_ENV_VARS = [
    ("rsp", "ACCESS_TOKEN"),
    ("jira", "JIRA_API_TOKEN"),
    ("zephyr", "ZEPHYR_API_TOKEN"),
]


@pytest.mark.parametrize("source, env_var", AUTH_SOURCE_ENV_VARS)
def test_retrieve_access_token_env(monkeypatch, source, env_var):
    monkeypatch.setenv(env_var, f"{source}-token")
    assert retrieve_access_token(AUTH_SOURCES[source]) == f"{source}-token"


@pytest.mark.parametrize("source, env_var", AUTH_SOURCE_ENV_VARS)
def test_retrieve_access_token_missing_is_a_server_error(monkeypatch, source, env_var):
    monkeypatch.delenv(env_var, raising=False)
    with pytest.raises(HTTPException) as excinfo:
        retrieve_access_token(AUTH_SOURCES[source])
    assert excinfo.value.status_code == 500
    # The missing variable is named in the log, never in the response.
    assert excinfo.value.detail == "Server configuration error"
    assert env_var not in excinfo.value.detail


def test_get_auth_header_valid():
    token = "my-token"
    header = get_auth_header(token)
    assert header == {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize("token", [None, ""])
def test_get_auth_header_missing_token(token):
    with pytest.raises(ValueError, match="Auth token is required"):
        get_auth_header(token)


def test_get_jira_hostname_env(monkeypatch):
    monkeypatch.setenv("JIRA_API_HOSTNAME", "jira.example.com")
    hostname = get_jira_hostname()
    assert hostname == "jira.example.com"


def test_get_jira_hostname_missing(monkeypatch):
    monkeypatch.delenv("JIRA_API_HOSTNAME", raising=False)
    with pytest.raises(HTTPException) as excinfo:
        get_jira_hostname()
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Jira hostname not configured"
