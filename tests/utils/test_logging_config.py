import contextvars
import logging
import threading

import pytest

from lsst.ts.logging_and_reporting.utils.logging_config import (
    DEFAULT_LOG_LEVEL,
    LOG_FORMAT,
    LOG_LEVELS,
    NO_TRACE_ID,
    TraceIdFilter,
    configure_logging,
    current_trace_id,
    log_level,
    set_trace_id,
)


@pytest.fixture(autouse=True)
def clear_trace_id():
    """Keep one test's trace ID out of the next one's context."""
    yield
    set_trace_id(NO_TRACE_ID)


@pytest.fixture
def clean_root():
    """Yield a callable that strips the root logger's handlers.

    Cleared from the test body rather than at setup because pytest
    installs its own capture handler after fixtures run, and
    ``basicConfig`` does nothing while any handler is attached.
    """
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level

    def clear():
        root.handlers = []
        return root

    yield clear
    root.handlers, root.level = saved_handlers, saved_level


def filter_count(handler):
    return sum(isinstance(f, TraceIdFilter) for f in handler.filters)


def make_record(message="hello"):
    return logging.LogRecord("some.logger", logging.INFO, "path.py", 1, message, None, None)


class TestTraceId:
    def test_defaults_to_the_no_trace_marker(self):
        assert current_trace_id() == NO_TRACE_ID

    def test_set_then_read(self):
        set_trace_id("abc12345")
        assert current_trace_id() == "abc12345"

    def test_a_plain_thread_does_not_inherit_it(self):
        # Why fetch_concurrently has to copy the context explicitly: a
        # new thread starts with an empty one.
        set_trace_id("abc12345")
        seen = []
        thread = threading.Thread(target=lambda: seen.append(current_trace_id()))
        thread.start()
        thread.join()
        assert seen == [NO_TRACE_ID]

    def test_a_copied_context_does_inherit_it(self):
        set_trace_id("abc12345")
        seen = []
        context = contextvars.copy_context()
        thread = threading.Thread(target=lambda: context.run(lambda: seen.append(current_trace_id())))
        thread.start()
        thread.join()
        assert seen == ["abc12345"]

    def test_setting_inside_a_copy_does_not_escape_it(self):
        contextvars.copy_context().run(set_trace_id, "abc12345")
        assert current_trace_id() == NO_TRACE_ID


class TestTraceIdFilter:
    def test_tags_records_with_the_current_id(self):
        set_trace_id("abc12345")
        record = make_record()
        assert TraceIdFilter().filter(record) is True
        assert record.trace_id == "abc12345"

    def test_tags_records_logged_outside_a_request(self):
        record = make_record()
        TraceIdFilter().filter(record)
        assert record.trace_id == NO_TRACE_ID

    def test_the_id_reaches_the_formatted_line(self):
        set_trace_id("abc12345")
        record = make_record("something happened")
        TraceIdFilter().filter(record)
        formatted = logging.Formatter(LOG_FORMAT).format(record)
        assert formatted == "INFO [some.logger] [abc12345] something happened"


class TestLogLevel:
    def test_reads_the_environment_case_insensitively(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "debug")
        assert log_level() == "DEBUG"

    def test_defaults_when_unset(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        assert log_level() == DEFAULT_LOG_LEVEL

    def test_defaults_when_blank(self, monkeypatch):
        # Compose writes an empty value for an unset variable, which
        # basicConfig would reject outright.
        monkeypatch.setenv("LOG_LEVEL", "   ")
        assert log_level() == DEFAULT_LOG_LEVEL

    def test_defaults_when_unrecognised(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
        assert log_level() == DEFAULT_LOG_LEVEL

    def test_rejects_level_names_uvicorn_does_not_know(self, monkeypatch):
        # Valid to the stdlib, but uvicorn is handed the same name and
        # raises KeyError on either, taking the server down with it.
        for name in ("WARN", "NOTSET"):
            monkeypatch.setenv("LOG_LEVEL", name)
            assert log_level() == DEFAULT_LOG_LEVEL

    def test_every_accepted_level_is_a_real_stdlib_level(self):
        assert all(isinstance(logging.getLevelName(name), int) for name in LOG_LEVELS)

    def test_the_default_is_one_of_the_accepted_levels(self):
        assert DEFAULT_LOG_LEVEL in LOG_LEVELS


class TestConfigureLogging:
    def test_attaches_the_filter_to_every_root_handler(self, clean_root):
        root = clean_root()
        configure_logging()
        assert root.handlers
        assert all(filter_count(handler) == 1 for handler in root.handlers)

    def test_is_idempotent(self, clean_root):
        # Called once per process, but a worker re-enters it after the
        # fork; the filter must not stack up.
        root = clean_root()
        configure_logging()
        configure_logging()
        configure_logging()
        assert all(filter_count(handler) == 1 for handler in root.handlers)

    def test_level_comes_from_the_environment(self, clean_root, monkeypatch):
        root = clean_root()
        monkeypatch.setenv("LOG_LEVEL", "debug")
        configure_logging()
        assert root.level == logging.DEBUG

    def test_level_defaults_to_info(self, clean_root, monkeypatch):
        root = clean_root()
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        configure_logging()
        assert root.level == logging.INFO

    def test_a_blank_level_falls_back_without_raising(self, clean_root, monkeypatch):
        root = clean_root()
        monkeypatch.setenv("LOG_LEVEL", "")
        configure_logging()
        assert root.level == logging.INFO

    def test_an_unrecognised_level_says_so(self, clean_root, monkeypatch, capsys):
        root = clean_root()
        monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
        configure_logging()
        assert root.level == logging.INFO
        assert "Unrecognised LOG_LEVEL 'VERBOSE'" in capsys.readouterr().err

    def test_an_accepted_level_says_nothing(self, clean_root, monkeypatch, capsys):
        clean_root()
        monkeypatch.setenv("LOG_LEVEL", "debug")
        configure_logging()
        assert "Unrecognised" not in capsys.readouterr().err

    def test_records_are_formatted_with_the_id(self, clean_root):
        root = clean_root()
        configure_logging()
        set_trace_id("abc12345")
        handler = root.handlers[0]
        record = make_record("through the handler")
        handler.filter(record)
        assert handler.format(record).endswith("[some.logger] [abc12345] through the handler")
