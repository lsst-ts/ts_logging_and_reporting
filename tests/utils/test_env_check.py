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

import logging

import pytest

from lsst.ts.logging_and_reporting.utils.auth import Server
from lsst.ts.logging_and_reporting.utils.env_check import (
    DEBUG_ENV_VAR,
    PRODUCTION_ENV_VARS,
    REQUIRED_ENV_VARS,
    check_environment,
    debug_mode,
)


@pytest.fixture
def complete_env(monkeypatch):
    """Set every checked variable to a valid value, outside debug mode."""
    for name in REQUIRED_ENV_VARS + PRODUCTION_ENV_VARS:
        monkeypatch.setenv(name, f"{name.lower()}-value")
    monkeypatch.setenv("EXTERNAL_INSTANCE_URL", Server.usdfdev)
    monkeypatch.delenv(DEBUG_ENV_VAR, raising=False)
    return monkeypatch


def critical_messages(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno == logging.CRITICAL]


class TestDebugMode:
    def test_unset_is_off(self, monkeypatch):
        monkeypatch.delenv(DEBUG_ENV_VAR, raising=False)
        assert not debug_mode()

    @pytest.mark.parametrize("value", ["", "0", "  ", " 0 "])
    def test_empty_and_zero_are_off(self, monkeypatch, value):
        monkeypatch.setenv(DEBUG_ENV_VAR, value)
        assert not debug_mode()

    @pytest.mark.parametrize("value", ["1", "true", "yes", "false"])
    def test_any_other_value_is_on(self, monkeypatch, value):
        monkeypatch.setenv(DEBUG_ENV_VAR, value)
        assert debug_mode()


class TestCheckEnvironment:
    def test_complete_environment_passes(self, complete_env, caplog):
        check_environment()
        assert critical_messages(caplog) == []

    @pytest.mark.parametrize("name", REQUIRED_ENV_VARS)
    @pytest.mark.parametrize("debug", [False, True])
    def test_required_variable_unset_fails_in_any_mode(self, complete_env, caplog, name, debug):
        complete_env.delenv(name)
        if debug:
            complete_env.setenv(DEBUG_ENV_VAR, "1")
        with pytest.raises(SystemExit) as excinfo:
            check_environment()
        assert excinfo.value.code == 1
        assert f"{name} is unset" in critical_messages(caplog)[0]

    @pytest.mark.parametrize("name", REQUIRED_ENV_VARS + PRODUCTION_ENV_VARS)
    def test_blank_counts_as_unset(self, complete_env, caplog, name):
        complete_env.setenv(name, "   ")
        with pytest.raises(SystemExit):
            check_environment()
        assert f"{name} is unset" in critical_messages(caplog)[0]

    @pytest.mark.parametrize("name", PRODUCTION_ENV_VARS)
    def test_production_variable_unset_fails_outside_debug_mode(self, complete_env, caplog, name):
        complete_env.delenv(name)
        with pytest.raises(SystemExit):
            check_environment()
        assert f"{name} is unset" in critical_messages(caplog)[0]

    @pytest.mark.parametrize("name", PRODUCTION_ENV_VARS)
    def test_production_variable_unset_only_warns_in_debug_mode(self, complete_env, caplog, name):
        complete_env.delenv(name)
        complete_env.setenv(DEBUG_ENV_VAR, "1")
        with caplog.at_level(logging.WARNING):
            check_environment()
        assert critical_messages(caplog) == []
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert name in warnings[0]

    def test_debug_mode_with_nothing_missing_does_not_warn(self, complete_env, caplog):
        complete_env.setenv(DEBUG_ENV_VAR, "1")
        with caplog.at_level(logging.WARNING):
            check_environment()
        assert caplog.records == []

    def test_unknown_instance_url_fails(self, complete_env, caplog):
        complete_env.setenv("EXTERNAL_INSTANCE_URL", "https://example.com")
        with pytest.raises(SystemExit):
            check_environment()
        assert "'https://example.com' is not a known deployment" in critical_messages(caplog)[0]

    @pytest.mark.parametrize("url", Server.get_all())
    def test_every_known_instance_url_passes(self, complete_env, url):
        complete_env.setenv("EXTERNAL_INSTANCE_URL", url)
        check_environment()

    def test_unset_instance_url_is_reported_once(self, complete_env, caplog):
        complete_env.delenv("EXTERNAL_INSTANCE_URL")
        with pytest.raises(SystemExit):
            check_environment()
        (message,) = critical_messages(caplog)
        assert "EXTERNAL_INSTANCE_URL is unset" in message
        assert "not a known deployment" not in message

    def test_every_problem_is_reported_in_one_message(self, complete_env, caplog):
        complete_env.delenv("ACCESS_TOKEN")
        complete_env.delenv("REDIS_HOST")
        complete_env.setenv("EXTERNAL_INSTANCE_URL", "https://example.com")
        with pytest.raises(SystemExit):
            check_environment()
        (message,) = critical_messages(caplog)
        assert "ACCESS_TOKEN is unset" in message
        assert "REDIS_HOST is unset" in message
        assert "not a known deployment" in message
