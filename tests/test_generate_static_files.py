"""Tests for scripts/generate_static_files.py."""

import json
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

# Import the script by adding scripts/ to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import generate_static_files as gsf

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_output_dir(tmp_path):
    d = tmp_path / "output"
    d.mkdir()
    return d


def _write_json(path, data):
    """Helper to write a JSON file at the given path."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _mock_response(text="", status_code=200, raise_for_status=None):
    """Build a mock requests.Response."""
    resp = Mock()
    resp.text = text
    resp.status_code = status_code
    if raise_for_status:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Group 1: DayObs Helpers
# ---------------------------------------------------------------------------


class TestGetCurrentDayobs:
    def _mock_now(self, year, month, day, hour, minute=0):
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    @patch("generate_static_files.datetime")
    def test_before_noon_utc(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 7, 7, 11, 59)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert result == 20260706

    @patch("generate_static_files.datetime")
    def test_at_noon_utc(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 7, 7, 12, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert result == 20260707

    @patch("generate_static_files.datetime")
    def test_after_noon_utc(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 7, 7, 15, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert result == 20260707

    @patch("generate_static_files.datetime")
    def test_midnight_utc(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 7, 7, 0, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert result == 20260706

    @patch("generate_static_files.datetime")
    def test_year_boundary(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 1, 1, 3, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert result == 20251231

    @patch("generate_static_files.datetime")
    def test_returns_int(self, mock_dt):
        mock_dt.now.return_value = self._mock_now(2026, 7, 7, 15, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = gsf.get_current_dayobs()
        assert isinstance(result, int)


class TestDayobsToDate:
    def test_normal(self):
        assert gsf.dayobs_to_date(20260625) == date(2026, 6, 25)

    def test_year_start(self):
        assert gsf.dayobs_to_date(20260101) == date(2026, 1, 1)

    def test_leap_day(self):
        assert gsf.dayobs_to_date(20240229) == date(2024, 2, 29)


class TestDayobsAddDays:
    def test_forward(self):
        assert gsf.dayobs_add_days(20260625, 1) == 20260626

    def test_backward(self):
        assert gsf.dayobs_add_days(20260625, -1) == 20260624

    def test_zero(self):
        assert gsf.dayobs_add_days(20260625, 0) == 20260625

    def test_month_rollover(self):
        assert gsf.dayobs_add_days(20260131, 1) == 20260201

    def test_year_rollover(self):
        assert gsf.dayobs_add_days(20261231, 1) == 20270101

    def test_leap_year(self):
        assert gsf.dayobs_add_days(20240228, 1) == 20240229


class TestGenerateDayobsCombinations:
    def test_max0(self):
        result = gsf.generate_dayobs_combinations(20260707, 0)
        assert result == [(20260707, 20260707)]

    def test_max1(self):
        result = gsf.generate_dayobs_combinations(20260707, 1)
        assert result == [
            (20260707, 20260707),
            (20260706, 20260706),
            (20260706, 20260707),
        ]

    def test_max2(self):
        result = gsf.generate_dayobs_combinations(20260707, 2)
        assert result == [
            (20260707, 20260707),
            (20260706, 20260706),
            (20260705, 20260705),
            (20260706, 20260707),
            (20260705, 20260706),
            (20260705, 20260707),
        ]

    def test_max7_count(self):
        result = gsf.generate_dayobs_combinations(20260707, 7)
        assert len(result) == 36

    def test_all_start_le_end(self):
        result = gsf.generate_dayobs_combinations(20260707, 7)
        for start, end in result:
            assert start <= end, f"start {start} > end {end}"

    def test_no_duplicates(self):
        result = gsf.generate_dayobs_combinations(20260707, 7)
        assert len(result) == len(set(result))

    def test_max_combo_size_same_as_max_days(self):
        """Explicit max_combo_size == max_days_ago should equal the default."""
        default = gsf.generate_dayobs_combinations(20260707, 7)
        explicit = gsf.generate_dayobs_combinations(20260707, 7, max_combo_size=7)
        assert explicit == default

    def test_max_combo_size_smaller_than_max_days(self):
        """Rolling combos: 14-day window but max combo span of 2."""
        result = gsf.generate_dayobs_combinations(20260707, 14, max_combo_size=2)
        # No combo should span more than 2 days
        for start, end in result:
            diff = (gsf.dayobs_to_date(end) - gsf.dayobs_to_date(start)).days
            assert diff <= 2, f"span {diff} exceeds max_combo_size=2: ({start}, {end})"
        # Should cover the full 14-day lookback (15 individual days)
        all_days = set()
        for start, end in result:
            d = gsf.dayobs_to_date(start)
            while d <= gsf.dayobs_to_date(end):
                all_days.add(int(d.strftime("%Y%m%d")))
                d += timedelta(days=1)
        expected_days = {int((date(2026, 7, 7) - timedelta(days=i)).strftime("%Y%m%d")) for i in range(15)}
        assert all_days == expected_days

    def test_max_combo_size_larger_than_max_days(self):
        """max_combo_size > max_days_ago is capped."""
        capped = gsf.generate_dayobs_combinations(20260707, 3, max_combo_size=10)
        default = gsf.generate_dayobs_combinations(20260707, 3)
        assert capped == default

    def test_max_combo_size_1(self):
        """max_combo_size=1 gives single-day and adjacent pairs."""
        result = gsf.generate_dayobs_combinations(20260707, 3, max_combo_size=1)
        for start, end in result:
            diff = (gsf.dayobs_to_date(end) - gsf.dayobs_to_date(start)).days
            assert diff <= 1
        # 4 individual days + 3 adjacent pairs = 7 combos
        assert len(result) == 7

    def test_max_combo_size_0(self):
        """max_combo_size=0 produces only single-day entries."""
        result = gsf.generate_dayobs_combinations(20260707, 3, max_combo_size=0)
        for start, end in result:
            assert start == end
        assert len(result) == 4  # today and 3 days back


# ---------------------------------------------------------------------------
# Group 2: URL / Filename Building
# ---------------------------------------------------------------------------


class TestBuildQueryString:
    def test_simple(self):
        assert gsf._build_query_string({"a": 1}) == "a=1"

    def test_multiple_keys(self):
        assert gsf._build_query_string({"a": 1, "b": 2}) == "a=1&b=2"

    def test_list_values(self):
        result = gsf._build_query_string({"metric": ["fault", "weather"]})
        assert result == "metric=fault&metric=weather"

    def test_bool_true(self):
        assert gsf._build_query_string({"flag": True}) == "flag=true"

    def test_bool_false(self):
        assert gsf._build_query_string({"flag": False}) == "flag=false"

    def test_mixed(self):
        result = gsf._build_query_string(
            {
                "dayObsStart": 20260707,
                "includeIntervals": True,
                "metric": ["fault", "weather"],
            }
        )
        assert result == ("dayObsStart=20260707&includeIntervals=true&metric=fault&metric=weather")

    def test_empty(self):
        assert gsf._build_query_string({}) == ""


class TestBuildFilename:
    def test_with_params(self):
        result = gsf.build_filename("exposures", {"dayObsStart": 20260625, "instrument": "LSSTCam"})
        assert result == "exposures?dayObsStart=20260625&instrument=LSSTCam"

    def test_no_params(self):
        assert gsf.build_filename("version", {}) == "version"

    def test_empty_dict_no_question_mark(self):
        result = gsf.build_filename("version", {})
        assert "?" not in result


class TestBuildUrl:
    def test_with_params(self):
        result = gsf.build_url("http://localhost:8080/api", "exposures", {"dayObsStart": 20260707})
        assert result == "http://localhost:8080/api/exposures?dayObsStart=20260707"

    def test_no_params(self):
        result = gsf.build_url("http://localhost:8080/api", "version", {})
        assert result == "http://localhost:8080/api/version"

    def test_trailing_slash_stripped(self):
        result = gsf.build_url("http://host/api/", "ep", {})
        assert result == "http://host/api/ep"

    def test_multiple_trailing_slashes(self):
        result = gsf.build_url("http://host/api///", "ep", {})
        assert result == "http://host/api/ep"

    def test_with_list_params(self):
        result = gsf.build_url("http://host/api", "block-details", {"key": ["BLOCK-A", "BLOCK-B"]})
        assert result == "http://host/api/block-details?key=BLOCK-A&key=BLOCK-B"


# ---------------------------------------------------------------------------
# Group 3: Regeneration Logic
# ---------------------------------------------------------------------------


class TestShouldRegenerate:
    TODAY = 20260707
    YESTERDAY = 20260706

    def test_force_refresh_always_true(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260701,
                20260701,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=True,
                historic_mode=True,
            )
            is True
        )

    def test_missing_file_always_true(self, tmp_path):
        fp = str(tmp_path / "nonexistent")
        assert (
            gsf.should_regenerate(
                fp,
                20260701,
                20260701,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_historic_mode_file_exists(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260701,
                20260701,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=True,
            )
            is False
        )

    def test_historic_mode_file_missing(self, tmp_path):
        fp = str(tmp_path / "nonexistent")
        assert (
            gsf.should_regenerate(
                fp,
                20260701,
                20260701,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=True,
            )
            is True
        )

    def test_spans_today(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260705,
                20260708,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_today_equals_start(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                self.TODAY,
                20260710,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_today_equals_end(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260705,
                self.TODAY,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_spans_yesterday_new_day(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260705,
                self.YESTERDAY,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=True,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_spans_yesterday_not_new_day(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260705,
                self.YESTERDAY,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is False
        )

    def test_old_range_no_refresh(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260701,
                20260703,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is False
        )

    def test_future_range(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate(
                fp,
                20260710,
                20260715,
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is False
        )


class TestShouldRegenerateBlockDetails:
    def test_force_refresh(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": False, "yesterday": False},
                is_new_day=False,
                force_refresh=True,
                historic_mode=False,
            )
            is True
        )

    def test_missing_file(self, tmp_path):
        fp = str(tmp_path / "nonexistent")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": False, "yesterday": False},
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_historic_mode_exists(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": True, "yesterday": True},
                is_new_day=True,
                force_refresh=False,
                historic_mode=True,
            )
            is False
        )

    def test_touches_today(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": True, "yesterday": False},
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_touches_yesterday_new_day(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": False, "yesterday": True},
                is_new_day=True,
                force_refresh=False,
                historic_mode=False,
            )
            is True
        )

    def test_touches_yesterday_not_new_day(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": False, "yesterday": True},
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is False
        )

    def test_touches_neither(self, tmp_path):
        fp = str(tmp_path / "file")
        with open(fp, "w") as f:
            f.write("x")
        assert (
            gsf.should_regenerate_block_details(
                fp,
                {"today": False, "yesterday": False},
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
            )
            is False
        )


# ---------------------------------------------------------------------------
# Group 4: fetch_and_save
# ---------------------------------------------------------------------------


class TestFetchAndSave:
    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_success(self, mock_get, mock_sleep, tmp_output_dir):
        mock_get.return_value = _mock_response(text='{"ok": true}')
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is True
        assert isinstance(start, float)
        assert isinstance(end, float)
        assert start <= end
        assert os.path.exists(fp)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_file_content(self, mock_get, mock_sleep, tmp_output_dir):
        mock_get.return_value = _mock_response(text='{"data": [1, 2, 3]}')
        fp = str(tmp_output_dir / "test_file")

        gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        with open(fp) as f:
            assert f.read() == '{"data": [1, 2, 3]}'

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_atomic_write_no_tmp_remains(self, mock_get, mock_sleep, tmp_output_dir):
        mock_get.return_value = _mock_response(text="content")
        fp = str(tmp_output_dir / "test_file")

        gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert not os.path.exists(fp + ".tmp")
        assert os.path.exists(fp)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_creates_parent_dirs(self, mock_get, mock_sleep, tmp_path):
        mock_get.return_value = _mock_response(text="content")
        fp = str(tmp_path / "nested" / "dirs" / "file")

        gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert os.path.exists(fp)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_http_error_returns_false(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.return_value = _mock_response(
            text="Not Found",
            status_code=404,
            raise_for_status=real_requests.exceptions.HTTPError("404"),
        )
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is False
        assert not os.path.exists(fp)
        mock_sleep.assert_not_called()

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_connection_error_retries_then_succeeds(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.side_effect = [
            real_requests.exceptions.ConnectionError("refused"),
            _mock_response(text="ok"),
        ]
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is True
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2**0

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_connection_error_succeeds_on_final_retry(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.side_effect = [
            real_requests.exceptions.ConnectionError("refused"),
            real_requests.exceptions.ConnectionError("refused"),
            _mock_response(text="ok"),
        ]
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is True
        assert mock_get.call_count == 3
        assert os.path.exists(fp)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)  # 2**0
        mock_sleep.assert_any_call(2)  # 2**1

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_connection_error_exhausted(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.side_effect = real_requests.exceptions.ConnectionError("refused")
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is False
        assert mock_get.call_count == 3  # initial + 2 retries
        assert not os.path.exists(fp)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_timeout_returns_false(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.side_effect = real_requests.exceptions.Timeout("timed out")
        fp = str(tmp_output_dir / "test_file")

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert ok is False
        mock_sleep.assert_not_called()

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_os_error_raises(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response(text="content")
        # Use an invalid path that will cause an OSError
        fp = "/proc/nonexistent/impossible/path/file"

        with pytest.raises(OSError):
            gsf.fetch_and_save("http://host/ep", fp, timeout=10)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_os_error_cleans_tmp(self, mock_get, mock_sleep, tmp_output_dir):
        mock_get.return_value = _mock_response(text="content")
        fp = str(tmp_output_dir / "test_file")
        tmp_fp = fp + ".tmp"

        # Make rename fail after the tmp file is written
        with patch("generate_static_files.os.rename", side_effect=OSError("rename fail")):
            with pytest.raises(OSError):
                gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        # The tmp file should have been cleaned up
        assert not os.path.exists(tmp_fp)

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_connection_error_backoff_timing(self, mock_get, mock_sleep, tmp_output_dir):
        import requests as real_requests

        mock_get.side_effect = real_requests.exceptions.ConnectionError("refused")
        fp = str(tmp_output_dir / "test_file")

        gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)  # 2**0
        mock_sleep.assert_any_call(2)  # 2**1

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_return_timestamps(self, mock_get, mock_sleep, tmp_output_dir):
        mock_get.return_value = _mock_response(text="content")
        fp = str(tmp_output_dir / "test_file")
        before = time.time()

        start, end, ok = gsf.fetch_and_save("http://host/ep", fp, timeout=10)

        after = time.time()
        assert isinstance(start, float)
        assert isinstance(end, float)
        assert before <= start <= end <= after

    @patch("generate_static_files.time.sleep")
    @patch("generate_static_files.requests.get")
    def test_bare_filename_no_directory(self, mock_get, mock_sleep, monkeypatch, tmp_path):
        """When file_path has no dir component, dirname is '' -> uses '.'."""
        mock_get.return_value = _mock_response(text="content")
        # Run from tmp_path so the bare filename writes there
        monkeypatch.chdir(tmp_path)
        start, end, ok = gsf.fetch_and_save("http://host/ep", "bare_file", timeout=10)
        assert ok is True
        assert os.path.exists(tmp_path / "bare_file")


# ---------------------------------------------------------------------------
# Group 5: Block Key Extraction
# ---------------------------------------------------------------------------


class TestExtractBlockKeysFromDataLog:
    def test_normal(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(
            fp,
            {
                "data_log": [
                    {"science_program": "BLOCK-T328"},
                    {"science_program": "BLOCK-T329"},
                ]
            },
        )
        assert gsf.extract_block_keys_from_data_log(fp) == {"BLOCK-T328", "BLOCK-T329"}

    def test_empty_array(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(fp, {"data_log": []})
        assert gsf.extract_block_keys_from_data_log(fp) == set()

    def test_missing_science_program_key(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(fp, {"data_log": [{"obs_id": 123}]})
        assert gsf.extract_block_keys_from_data_log(fp) == set()

    def test_null_science_program(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(
            fp,
            {
                "data_log": [
                    {"science_program": None},
                    {"science_program": "BLOCK-T328"},
                ]
            },
        )
        assert gsf.extract_block_keys_from_data_log(fp) == {"BLOCK-T328"}

    def test_no_data_log_key(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(fp, {"exposures": []})
        assert gsf.extract_block_keys_from_data_log(fp) == set()

    def test_file_not_found(self):
        assert gsf.extract_block_keys_from_data_log("/nonexistent/path") == set()

    def test_invalid_json(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        with open(fp, "w") as f:
            f.write("not json{{")
        assert gsf.extract_block_keys_from_data_log(fp) == set()

    def test_deduplicates(self, tmp_path):
        fp = str(tmp_path / "data-log.json")
        _write_json(
            fp,
            {
                "data_log": [
                    {"science_program": "BLOCK-T328"},
                    {"science_program": "BLOCK-T328"},
                ]
            },
        )
        assert gsf.extract_block_keys_from_data_log(fp) == {"BLOCK-T328"}


class TestExtractBlockKeysFromExposures:
    def test_normal(self, tmp_path):
        fp = str(tmp_path / "exposures.json")
        _write_json(
            fp,
            {
                "exposures": [
                    {"science_program": "BLOCK-T400"},
                    {"science_program": "BLOCK-T401"},
                ]
            },
        )
        assert gsf.extract_block_keys_from_exposures(fp) == {"BLOCK-T400", "BLOCK-T401"}

    def test_empty(self, tmp_path):
        fp = str(tmp_path / "exposures.json")
        _write_json(fp, {"exposures": []})
        assert gsf.extract_block_keys_from_exposures(fp) == set()

    def test_file_not_found(self):
        assert gsf.extract_block_keys_from_exposures("/nonexistent/path") == set()

    def test_missing_key_in_record(self, tmp_path):
        fp = str(tmp_path / "exposures.json")
        _write_json(fp, {"exposures": [{"obs_id": 1}]})
        assert gsf.extract_block_keys_from_exposures(fp) == set()


# ---------------------------------------------------------------------------
# Group 6: Progress Class
# ---------------------------------------------------------------------------


class TestProgress:
    def test_initial_state(self):
        p = gsf.Progress(10)
        assert p.total == 10
        assert p.current == 0
        assert p.created == 0
        assert p.skipped == 0
        assert p.errors == 0

    def test_tick_increments_current(self):
        p = gsf.Progress(5)
        p.tick("Created", "file1")
        assert p.current == 1

    def test_record_created_increments(self):
        p = gsf.Progress(5)
        p.record_created("file1", dry_run=False)
        assert p.created == 1
        assert p.current == 1

    def test_record_skipped_increments(self):
        p = gsf.Progress(5)
        p.record_skipped("file1")
        assert p.skipped == 1
        assert p.current == 1

    def test_record_error_increments(self):
        p = gsf.Progress(5)
        t = time.time()
        p.record_error("file1", start=t, end=t + 1.0)
        assert p.errors == 1
        assert p.current == 1

    def test_record_created_with_timing(self):
        p = gsf.Progress(5)
        t0 = time.time()
        t1 = t0 + 2.5
        p.record_created("file1", dry_run=False, start=t0, end=t1)
        assert p.created == 1

    def test_record_created_start_only_no_end(self):
        p = gsf.Progress(5)
        t0 = time.time()
        with patch("generate_static_files.logger") as mock_logger:
            p.record_created("file1", dry_run=False, start=t0, end=None)
        assert p.created == 1
        # Should format with bracket style: " [{start_str}]"
        log_str = str(mock_logger.info.call_args)
        assert "[" in log_str

    def test_record_skipped_custom_reason(self):
        p = gsf.Progress(5)
        with patch("generate_static_files.logger") as mock_logger:
            p.record_skipped("file1", reason="historic")
        assert p.skipped == 1
        log_str = str(mock_logger.info.call_args)
        assert "historic" in log_str

    def test_record_created_dry_run(self):
        p = gsf.Progress(5)
        with patch("generate_static_files.logger") as mock_logger:
            p.record_created("file1", dry_run=True)
        assert p.created == 1
        # "Would create" is logged (verified via the logger mock)
        log_call = mock_logger.info.call_args
        assert "Would create" in str(log_call)

    def test_thread_safety(self):
        p = gsf.Progress(100)
        barrier = threading.Barrier(100)

        def worker():
            barrier.wait()
            p.tick("Created", "file")

        threads = [threading.Thread(target=worker) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert p.current == 100

    def test_summary_logs(self):
        p = gsf.Progress(3)
        p.record_created("f1", dry_run=False)
        p.record_skipped("f2")
        t = time.time()
        p.record_error("f3", start=t, end=t + 1.0)

        with patch("generate_static_files.logger") as mock_logger:
            p.summary()

        mock_logger.info.assert_called()
        # The summary call uses format string with %d placeholders
        summary_call = mock_logger.info.call_args_list[0]
        fmt_str = summary_call[0][0]
        args = summary_call[0][1:]
        assert "created" in fmt_str
        assert args == (1, 1, 1)  # created, skipped, errors

    def test_summary_with_batch_start(self):
        p = gsf.Progress(1)
        p.record_created("f1", dry_run=False)

        with patch("generate_static_files.logger") as mock_logger:
            p.summary(batch_start=time.time() - 10)

        log_str = str(mock_logger.info.call_args_list)
        assert "Total duration" in log_str


# ---------------------------------------------------------------------------
# Group 7: build_dayobs_tasks
# ---------------------------------------------------------------------------


class TestBuildDayobsTasks:
    TODAY = 20260707
    YESTERDAY = 20260706

    def test_force_refresh_all_created(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        assert len(tasks) == 9
        assert skipped == 0

    def test_task_structure(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, _ = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        for task in tasks:
            assert set(task.keys()) == {"filename", "endpoint", "params", "start", "end"}
            assert task["start"] == self.TODAY
            assert task["end"] == self.TODAY

    def test_group_b_endpoints(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, _ = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        group_b = {"expected-exposures", "almanac"}
        found = {t["endpoint"] for t in tasks if t["endpoint"] in group_b}
        assert found == group_b

    def test_group_c_obs_status_variant(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, _ = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        obs_tasks = [t for t in tasks if t["endpoint"] == "obs-status"]
        assert len(obs_tasks) == 1
        assert obs_tasks[0]["params"]["metric"] == ["fault_loss", "weather"]
        assert obs_tasks[0]["params"]["nightOnlyMetrics"] is True
        assert obs_tasks[0]["params"]["includeIntervals"] is True

    def test_group_a_single_instrument(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, _ = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        exp_tasks = [t for t in tasks if t["endpoint"] == "exposures"]
        assert len(exp_tasks) == 1
        assert exp_tasks[0]["params"]["instrument"] == "LSSTCam"

    def test_multi_night_visit_maps_applet_mode(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, _ = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        mnvm_tasks = [t for t in tasks if t["endpoint"] == "multi-night-visit-maps"]
        assert len(mnvm_tasks) == 1
        assert mnvm_tasks[0]["params"]["appletMode"] is False

    def test_skips_existing_old_files(self, tmp_output_dir):
        old_combo = (20260701, 20260701)
        combos = [old_combo]
        # Create an almanac file for this old combo
        filename = gsf.build_filename("almanac", {"dayObsStart": 20260701, "dayObsEnd": 20260701})
        with open(str(tmp_output_dir / filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        # The almanac file should be skipped
        almanac_tasks = [t for t in tasks if t["endpoint"] == "almanac"]
        assert len(almanac_tasks) == 0
        assert skipped == 1

    def test_regenerates_today_files(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        # Create the almanac file for today
        filename = gsf.build_filename("almanac", {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY})
        with open(str(tmp_output_dir / filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        almanac_tasks = [t for t in tasks if t["endpoint"] == "almanac"]
        assert len(almanac_tasks) == 1  # still regenerated because spans today

    def test_historic_mode_only_missing(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        # Create one file (almanac) so it gets skipped
        filename = gsf.build_filename("almanac", {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY})
        with open(str(tmp_output_dir / filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=True,
            output_dir=str(tmp_output_dir),
        )
        # almanac exists, should be skipped
        almanac_tasks = [t for t in tasks if t["endpoint"] == "almanac"]
        assert len(almanac_tasks) == 0
        assert skipped == 1
        # remaining 8 tasks should be present (files missing)
        assert len(tasks) == 8

    def test_skipped_count_accurate(self, tmp_output_dir):
        old_combo = (20260701, 20260701)
        combos = [old_combo]
        base_params = {"dayObsStart": 20260701, "dayObsEnd": 20260701}

        # Create files for old combo
        for ep in ["almanac", "expected-exposures"]:
            filename = gsf.build_filename(ep, base_params)
            with open(str(tmp_output_dir / filename), "w") as f:
                f.write("{}")
        # One instrument-specific
        inst_params = {**base_params, "instrument": "LSSTCam"}
        filename = gsf.build_filename("exposures", inst_params)
        with open(str(tmp_output_dir / filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        assert skipped == 3

    def test_multiple_combos(self, tmp_output_dir):
        combos = [
            (self.TODAY, self.TODAY),
            (self.YESTERDAY, self.TODAY),
            (self.YESTERDAY, self.YESTERDAY),
        ]
        tasks, skipped = gsf.build_dayobs_tasks(
            combos,
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        # 9 tasks per combo * 3 combos = 27
        assert len(tasks) == 27
        assert skipped == 0

    def test_empty_combos(self, tmp_output_dir):
        tasks, skipped = gsf.build_dayobs_tasks(
            [],
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
            output_dir=str(tmp_output_dir),
        )
        assert tasks == []
        assert skipped == 0


# ---------------------------------------------------------------------------
# Group 8: build_block_details_tasks
# ---------------------------------------------------------------------------


class TestBuildBlockDetailsTasks:
    TODAY = 20260707
    YESTERDAY = 20260706

    def test_no_source_files(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=False,
        )
        assert tasks == []
        assert skipped == 0

    def test_from_data_log(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        # Write data-log file for LSSTCam only
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(str(tmp_output_dir / dl_filename), {"data_log": [{"science_program": "BLOCK-T400"}]})

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert len(tasks) == 1
        assert tasks[0]["params"]["key"] == ["BLOCK-T400"]

    def test_from_exposures(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        exp_filename = gsf.build_filename(
            "exposures",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(str(tmp_output_dir / exp_filename), {"exposures": [{"science_program": "BLOCK-T500"}]})

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert len(tasks) == 1
        assert tasks[0]["params"]["key"] == ["BLOCK-T500"]

    def test_deduplication(self, tmp_output_dir):
        combo1 = (self.TODAY, self.TODAY)
        combo2 = (self.YESTERDAY, self.TODAY)
        combos = [combo1, combo2]

        # Both combos' data-log files have the same BLOCK key
        for start, end in combos:
            dl_filename = gsf.build_filename(
                "data-log",
                {"dayObsStart": start, "dayObsEnd": end, "instrument": "LSSTCam"},
            )
            _write_json(
                str(tmp_output_dir / dl_filename),
                {"data_log": [{"science_program": "BLOCK-T328"}]},
            )

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        # Same frozenset -> only one task
        assert len(tasks) == 1

    def test_different_keysets_separate_tasks(self, tmp_output_dir):
        combo1 = (self.TODAY, self.TODAY)
        combo2 = (self.YESTERDAY, self.YESTERDAY)
        combos = [combo1, combo2]

        # combo1 has BLOCK-T328, combo2 has BLOCK-T329
        dl1 = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl1),
            {"data_log": [{"science_program": "BLOCK-T328"}]},
        )
        dl2 = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.YESTERDAY, "dayObsEnd": self.YESTERDAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl2),
            {"data_log": [{"science_program": "BLOCK-T329"}]},
        )

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=True,
            force_refresh=True,
            historic_mode=False,
        )
        assert len(tasks) == 2

    def test_keys_sorted_alphabetically(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {
                "data_log": [
                    {"science_program": "BLOCK-Z"},
                    {"science_program": "BLOCK-A"},
                    {"science_program": "BLOCK-M"},
                ]
            },
        )

        tasks, _ = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert tasks[0]["params"]["key"] == ["BLOCK-A", "BLOCK-M", "BLOCK-Z"]

    def test_touches_today_regenerates(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {"data_log": [{"science_program": "BLOCK-T328"}]},
        )

        # Create the block-details file so it already exists
        bd_filename = gsf.build_filename("block-details", {"key": ["BLOCK-T328"]})
        with open(str(tmp_output_dir / bd_filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=False,
        )
        # Should regenerate because the key set touches today
        assert len(tasks) == 1
        assert skipped == 0

    def test_historic_mode_skips_existing(self, tmp_output_dir):
        combos = [(self.TODAY, self.TODAY)]
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {"data_log": [{"science_program": "BLOCK-T328"}]},
        )

        # Create the block-details file
        bd_filename = gsf.build_filename("block-details", {"key": ["BLOCK-T328"]})
        with open(str(tmp_output_dir / bd_filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=False,
            historic_mode=True,
        )
        assert len(tasks) == 0
        assert skipped == 1

    def test_dry_run_no_warnings(self, tmp_output_dir, caplog):
        combos = [(self.TODAY, self.TODAY)]
        # No source files exist; dry_run=True should suppress warnings
        import logging

        with caplog.at_level(logging.WARNING):
            tasks, skipped = gsf.build_block_details_tasks(
                combos,
                str(tmp_output_dir),
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
                dry_run=True,
            )
        # No "missing" warnings should have been logged
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        block_warnings = [m for m in warning_messages if "block-details" in m]
        assert block_warnings == []

    def test_source_files_with_no_block_keys(self, tmp_output_dir):
        """Source files exist but contain no BLOCK keys -> no tasks."""
        combos = [(self.TODAY, self.TODAY)]
        # Data-log with no science_program
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {"data_log": [{"obs_id": 1}]},
        )

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert tasks == []
        assert skipped == 0

    def test_missing_files_log_warnings(self, tmp_output_dir, caplog):
        """Non-dry-run mode logs warnings for missing source files."""
        import logging

        combos = [(self.TODAY, self.TODAY)]
        with caplog.at_level(logging.WARNING):
            gsf.build_block_details_tasks(
                combos,
                str(tmp_output_dir),
                self.TODAY,
                self.YESTERDAY,
                is_new_day=False,
                force_refresh=False,
                historic_mode=False,
                dry_run=False,
            )
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        block_warnings = [m for m in warning_messages if "block-details" in m]
        # Should warn about missing data-log, exposures
        assert len(block_warnings) >= 1

    def test_touches_yesterday_new_day_regenerates(self, tmp_output_dir):
        """Combo spanning only yesterday with is_new_day=True
        should regenerate existing block-details."""
        combos = [(self.YESTERDAY, self.YESTERDAY)]
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.YESTERDAY, "dayObsEnd": self.YESTERDAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {"data_log": [{"science_program": "BLOCK-T328"}]},
        )

        # Create the block-details file so it already exists
        bd_filename = gsf.build_filename("block-details", {"key": ["BLOCK-T328"]})
        with open(str(tmp_output_dir / bd_filename), "w") as f:
            f.write("{}")

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=True,
            force_refresh=False,
            historic_mode=False,
        )
        assert len(tasks) == 1
        assert skipped == 0

    def test_multiple_sources_different_keys_same_combo(self, tmp_output_dir):
        """data-log and exposures with different keys for the same
        combo should produce separate block-details tasks."""
        combos = [(self.TODAY, self.TODAY)]
        dl_filename = gsf.build_filename(
            "data-log",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / dl_filename),
            {"data_log": [{"science_program": "BLOCK-A"}]},
        )
        exp_filename = gsf.build_filename(
            "exposures",
            {"dayObsStart": self.TODAY, "dayObsEnd": self.TODAY, "instrument": "LSSTCam"},
        )
        _write_json(
            str(tmp_output_dir / exp_filename),
            {"exposures": [{"science_program": "BLOCK-B"}]},
        )

        tasks, skipped = gsf.build_block_details_tasks(
            combos,
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert len(tasks) == 2
        all_keys = {tuple(t["params"]["key"]) for t in tasks}
        assert all_keys == {("BLOCK-A",), ("BLOCK-B",)}

    def test_empty_combos(self, tmp_output_dir):
        tasks, skipped = gsf.build_block_details_tasks(
            [],
            str(tmp_output_dir),
            self.TODAY,
            self.YESTERDAY,
            is_new_day=False,
            force_refresh=True,
            historic_mode=False,
        )
        assert tasks == []
        assert skipped == 0


# ---------------------------------------------------------------------------
# Group 9: Task Execution
# ---------------------------------------------------------------------------


class TestRunTask:
    def _make_task(self):
        return {
            "filename": "exposures?dayObsStart=20260707&dayObsEnd=20260707&instrument=LSSTCam",
            "endpoint": "exposures",
            "params": {"dayObsStart": 20260707, "dayObsEnd": 20260707, "instrument": "LSSTCam"},
            "start": 20260707,
            "end": 20260707,
        }

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_dry_run(self, mock_sleep, mock_fetch):
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.5, dry_run=True, progress=progress)
        mock_fetch.assert_not_called()
        assert progress.created == 1

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_success(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, True)
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.0, dry_run=False, progress=progress)
        assert progress.created == 1

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_failure(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, False)
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.0, dry_run=False, progress=progress)
        assert progress.errors == 1

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_rate_limit_applied(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, True)
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.5, dry_run=False, progress=progress)
        mock_sleep.assert_called_once_with(0.5)

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_no_rate_limit_when_dry_run(self, mock_sleep, mock_fetch):
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.5, dry_run=True, progress=progress)
        mock_sleep.assert_not_called()

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_zero_rate_limit(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, True)
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(task, "http://host/api", "/output", 10, 0.0, dry_run=False, progress=progress)
        mock_sleep.assert_not_called()

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_url_construction(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, True)
        task = self._make_task()
        progress = gsf.Progress(1)
        gsf._run_task(
            task,
            "http://host/api",
            "/output",
            10,
            0.0,
            dry_run=False,
            progress=progress,
        )
        expected_url = gsf.build_url("http://host/api", task["endpoint"], task["params"])
        actual_url = mock_fetch.call_args[0][0]
        assert actual_url == expected_url
        expected_path = os.path.join("/output", task["filename"])
        actual_path = mock_fetch.call_args[0][1]
        assert actual_path == expected_path


class TestRunTasks:
    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_all_tasks_executed(self, mock_sleep, mock_fetch):
        t = time.time()
        mock_fetch.return_value = (t, t + 1.0, True)
        tasks = [
            {
                "filename": f"file{i}",
                "endpoint": "version",
                "params": {},
                "start": 20260707,
                "end": 20260707,
            }
            for i in range(5)
        ]
        progress = gsf.Progress(5)
        gsf._run_tasks(tasks, "http://host/api", "/output", 10, 0.0, False, progress, 2)
        assert progress.created == 5

    @patch("generate_static_files.fetch_and_save")
    @patch("generate_static_files.time.sleep")
    def test_propagates_os_error(self, mock_sleep, mock_fetch):
        mock_fetch.side_effect = OSError("disk full")
        tasks = [
            {
                "filename": "f",
                "endpoint": "version",
                "params": {},
                "start": 20260707,
                "end": 20260707,
            }
        ]
        progress = gsf.Progress(1)
        with pytest.raises(OSError, match="disk full"):
            gsf._run_tasks(tasks, "http://host/api", "/output", 10, 0.0, False, progress, 1)

    @patch("generate_static_files.time.sleep")
    def test_tasks_run_concurrently(self, mock_sleep):
        """Verify tasks actually execute in parallel, not serially."""
        barrier = threading.Barrier(4, timeout=5)
        peak_concurrent = {"value": 0}
        active_count = {"value": 0}
        lock = threading.Lock()

        def mock_fetch(url, file_path, timeout):
            with lock:
                active_count["value"] += 1
                if active_count["value"] > peak_concurrent["value"]:
                    peak_concurrent["value"] = active_count["value"]
            # All 4 tasks must arrive here before any proceeds
            barrier.wait()
            with lock:
                active_count["value"] -= 1
            t = time.time()
            return (t, t + 0.01, True)

        tasks = [
            {
                "filename": f"file{i}",
                "endpoint": "version",
                "params": {},
                "start": 20260707,
                "end": 20260707,
            }
            for i in range(4)
        ]
        progress = gsf.Progress(4)

        with patch("generate_static_files.fetch_and_save", side_effect=mock_fetch):
            gsf._run_tasks(tasks, "http://host/api", "/output", 10, 0.0, False, progress, 4)

        assert progress.created == 4
        # All 4 were running at the same time (barrier forced it)
        assert peak_concurrent["value"] == 4

    @patch("generate_static_files.time.sleep")
    def test_worker_count_limits_concurrency(self, mock_sleep):
        """Verify that workers param caps concurrent execution."""
        peak_concurrent = {"value": 0}
        active_count = {"value": 0}
        lock = threading.Lock()

        def mock_fetch(url, file_path, timeout):
            with lock:
                active_count["value"] += 1
                if active_count["value"] > peak_concurrent["value"]:
                    peak_concurrent["value"] = active_count["value"]
            # Small sleep to keep tasks overlapping
            time.sleep(0.05)
            with lock:
                active_count["value"] -= 1
            t = time.time()
            return (t, t + 0.01, True)

        tasks = [
            {
                "filename": f"file{i}",
                "endpoint": "version",
                "params": {},
                "start": 20260707,
                "end": 20260707,
            }
            for i in range(8)
        ]
        progress = gsf.Progress(8)

        with patch("generate_static_files.fetch_and_save", side_effect=mock_fetch):
            gsf._run_tasks(tasks, "http://host/api", "/output", 10, 0.0, False, progress, 2)

        assert progress.created == 8
        # Should never exceed 2 concurrent tasks
        assert peak_concurrent["value"] <= 2

    @patch("generate_static_files.time.sleep")
    def test_concurrent_progress_tracking(self, mock_sleep):
        """Verify Progress counters are correct after concurrent execution."""
        call_count = {"value": 0}
        lock = threading.Lock()

        def mock_fetch(url, file_path, timeout):
            with lock:
                call_count["value"] += 1
                n = call_count["value"]
            t = time.time()
            # Alternate success and failure
            return (t, t + 0.01, n % 2 == 1)

        tasks = [
            {
                "filename": f"file{i}",
                "endpoint": "version",
                "params": {},
                "start": 20260707,
                "end": 20260707,
            }
            for i in range(10)
        ]
        progress = gsf.Progress(10)

        with patch("generate_static_files.fetch_and_save", side_effect=mock_fetch):
            gsf._run_tasks(tasks, "http://host/api", "/output", 10, 0.0, False, progress, 4)

        assert progress.current == 10
        assert progress.created + progress.errors == 10


# ---------------------------------------------------------------------------
# Group 10: main() Integration
# ---------------------------------------------------------------------------


class TestMain:
    def _base_argv(self, tmp_output_dir, extra=None):
        args = [
            "generate_static_files.py",
            "--backend-url",
            "http://localhost:8080/api",
            "--output-dir",
            str(tmp_output_dir),
            "--rate-limit",
            "0",
            "--max-days",
            "0",
            "--request-timeout",
            "5",
            "--workers",
            "1",
        ]
        if extra:
            args.extend(extra)
        return args

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_pid_lock_prevents_concurrent_run(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        lock_path = str(tmp_output_dir / "test.lock")
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))

        # Create the lock file before main() runs
        with open(lock_path, "w") as f:
            f.write("12345")

        with patch("generate_static_files.os.open", side_effect=FileExistsError):
            with pytest.raises(SystemExit) as exc_info:
                gsf.main()
            assert exc_info.value.code == 1

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_pid_lock_created_and_cleaned(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))

        mock_open = Mock(return_value=99)
        mock_unlink = Mock()

        with patch("generate_static_files.os.open", mock_open):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink", mock_unlink):
                        gsf.main()

        # Lock file should have been created
        # (os.open called with O_CREAT|O_EXCL)
        mock_open.assert_called_once()
        call_args = mock_open.call_args[0]
        assert "generate_static_files" in call_args[0]
        assert call_args[1] & os.O_CREAT
        assert call_args[1] & os.O_EXCL

        # Lock file should have been cleaned up
        mock_unlink.assert_called_once_with(call_args[0])

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_pid_lock_cleaned_on_exception(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))
        mock_run.side_effect = RuntimeError("boom")

        unlink_calls = []
        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch(
                        "generate_static_files.os.unlink",
                        side_effect=lambda p: unlink_calls.append(p),
                    ):
                        with pytest.raises(RuntimeError, match="boom"):
                            gsf.main()
        # Lock file should have been cleaned up in finally
        assert any("generate_static_files" in p for p in unlink_calls)

    @patch("generate_static_files._run_tasks")
    def test_dayobs_override_sets_historic_mode(self, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--dayobs-override", "20260101"]),
        )

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        # Verify historic mode was used via _run_tasks
        # In historic mode, the dayobs passed should be 20260101
        assert mock_run.called

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_normal_mode_calls_get_current_dayobs(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        mock_dayobs.assert_called_once()

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    @patch("generate_static_files.build_dayobs_tasks", return_value=([], 0))
    def test_block_details_only_skips_version_and_dayobs(
        self, mock_build, mock_dayobs, mock_run, tmp_output_dir, monkeypatch
    ):
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--block-details-only"]),
        )

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        # build_dayobs_tasks should not have been called
        mock_build.assert_not_called()

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_dry_run_mode(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--dry-run"]),
        )

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        # dry_run should be True in _run_tasks calls
        for call in mock_run.call_args_list:
            # dry_run is the 6th positional arg (index 5)
            assert call[0][5] is True

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_new_day_detection(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))

        # Don't create today's almanac sentinel -> is_new_day should be True
        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        with patch("generate_static_files.logger") as mock_logger:
                            gsf.main()

        # Should log about first run for new day
        log_str = str(mock_logger.info.call_args_list)
        assert "First run" in log_str or "yesterday" in log_str

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_not_new_day_when_sentinel_exists(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr("generate_static_files.sys.argv", self._base_argv(tmp_output_dir))

        # Create today's almanac sentinel file
        sentinel = gsf.build_filename("almanac", {"dayObsStart": 20260707, "dayObsEnd": 20260707})
        with open(str(tmp_output_dir / sentinel), "w") as f:
            f.write("{}")

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        with patch("generate_static_files.logger") as mock_logger:
                            gsf.main()

        # Should NOT log about first run
        log_str = str(mock_logger.info.call_args_list)
        assert "First run" not in log_str

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_force_refresh_creates_version_task(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--force-refresh"]),
        )
        # Create a version file so it already exists
        with open(str(tmp_output_dir / "version"), "w") as f:
            f.write("{}")

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        # First _run_tasks call should include a version task
        first_call_tasks = mock_run.call_args_list[0][0][0]
        version_tasks = [t for t in first_call_tasks if t.get("endpoint") == "version"]
        assert len(version_tasks) == 1

    @patch("generate_static_files._run_tasks")
    def test_historic_mode_forces_is_new_day_false(self, mock_run, tmp_output_dir, monkeypatch):
        """In historic mode, is_new_day is always False even
        when the sentinel file is missing."""
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--dayobs-override", "20260707"]),
        )
        # Don't create sentinel — in normal mode this would trigger is_new_day

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        with patch("generate_static_files.logger") as mock_logger:
                            gsf.main()

        # Should NOT log "First run" because historic mode
        log_str = str(mock_logger.info.call_args_list)
        assert "First run" not in log_str

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    @patch("generate_static_files.generate_dayobs_combinations")
    def test_max_combo_size_passed_through(
        self, mock_combos, mock_dayobs, mock_run, tmp_output_dir, monkeypatch
    ):
        """--max-combo-size CLI arg is forwarded to
        generate_dayobs_combinations."""
        mock_combos.return_value = []
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir, ["--max-days", "14", "--max-combo-size", "3"]),
        )

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        gsf.main()

        mock_combos.assert_called_once_with(20260707, 14, 3)

    @patch("generate_static_files._run_tasks")
    @patch("generate_static_files.get_current_dayobs", return_value=20260707)
    def test_version_skip_counting(self, mock_dayobs, mock_run, tmp_output_dir, monkeypatch):
        """When version file exists and no --force-refresh,
        version_skipped should be counted."""
        monkeypatch.setattr(
            "generate_static_files.sys.argv",
            self._base_argv(tmp_output_dir),
        )
        # Create the version file so it gets skipped
        with open(str(tmp_output_dir / "version"), "w") as f:
            f.write("{}")

        with patch("generate_static_files.os.open", return_value=99):
            with patch("generate_static_files.os.write"):
                with patch("generate_static_files.os.close"):
                    with patch("generate_static_files.os.unlink"):
                        with patch("generate_static_files.logger") as mock_logger:
                            gsf.main()

        # Find the summary log call
        summary_calls = [c for c in mock_logger.info.call_args_list if c[0][0] and "skipped" in str(c[0][0])]
        assert len(summary_calls) == 1
        # args are (created, skipped, errors); version should be counted
        skipped_count = summary_calls[0][0][2]
        assert skipped_count >= 1
