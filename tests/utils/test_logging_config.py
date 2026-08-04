import contextvars
import logging
import threading

import pytest

from lsst.ts.logging_and_reporting.utils.logging_config import (
    LOG_FORMAT,
    NO_REQUEST_ID,
    RequestIdFilter,
    configure_logging,
    current_request_id,
    set_request_id,
)


@pytest.fixture(autouse=True)
def clear_request_id():
    """Keep one test's request ID out of the next one's context."""
    yield
    set_request_id(NO_REQUEST_ID)


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
    return sum(isinstance(f, RequestIdFilter) for f in handler.filters)


def make_record(message="hello"):
    return logging.LogRecord(
        "some.logger", logging.INFO, "path.py", 1, message, None, None
    )


class TestRequestId:
    def test_defaults_to_the_no_request_marker(self):
        assert current_request_id() == NO_REQUEST_ID

    def test_set_then_read(self):
        set_request_id("abc12345")
        assert current_request_id() == "abc12345"

    def test_a_plain_thread_does_not_inherit_it(self):
        # Why fetch_concurrently has to copy the context explicitly: a
        # new thread starts with an empty one.
        set_request_id("abc12345")
        seen = []
        thread = threading.Thread(target=lambda: seen.append(current_request_id()))
        thread.start()
        thread.join()
        assert seen == [NO_REQUEST_ID]

    def test_a_copied_context_does_inherit_it(self):
        set_request_id("abc12345")
        seen = []
        context = contextvars.copy_context()
        thread = threading.Thread(
            target=lambda: context.run(lambda: seen.append(current_request_id()))
        )
        thread.start()
        thread.join()
        assert seen == ["abc12345"]

    def test_setting_inside_a_copy_does_not_escape_it(self):
        contextvars.copy_context().run(set_request_id, "abc12345")
        assert current_request_id() == NO_REQUEST_ID


class TestRequestIdFilter:
    def test_tags_records_with_the_current_id(self):
        set_request_id("abc12345")
        record = make_record()
        assert RequestIdFilter().filter(record) is True
        assert record.request_id == "abc12345"

    def test_tags_records_logged_outside_a_request(self):
        record = make_record()
        RequestIdFilter().filter(record)
        assert record.request_id == NO_REQUEST_ID

    def test_the_id_reaches_the_formatted_line(self):
        set_request_id("abc12345")
        record = make_record("something happened")
        RequestIdFilter().filter(record)
        formatted = logging.Formatter(LOG_FORMAT).format(record)
        assert formatted == "INFO [some.logger] [abc12345] something happened"


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

    def test_records_are_formatted_with_the_id(self, clean_root):
        root = clean_root()
        configure_logging()
        set_request_id("abc12345")
        handler = root.handlers[0]
        record = make_record("through the handler")
        handler.filter(record)
        assert handler.format(record).endswith(
            "[some.logger] [abc12345] through the handler"
        )
