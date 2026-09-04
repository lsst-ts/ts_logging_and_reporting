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

import json

import numpy as np
import pandas as pd
from astropy.time import Time

from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe, stringify_special_floats


def test_stringify_special_floats_nan():
    assert stringify_special_floats(np.nan) == "NaN"


def test_stringify_special_floats_pos_inf():
    assert stringify_special_floats(np.inf) == "Infinity"


def test_stringify_special_floats_neg_inf():
    assert stringify_special_floats(-np.inf) == "-Infinity"


def test_stringify_special_floats_regular_float():
    assert stringify_special_floats(42.5) == 42.5


def test_stringify_special_floats_non_float_type():
    assert stringify_special_floats("hello") == "hello"
    assert stringify_special_floats(123) == 123


def test_stringify_special_floats_numpy_float32():
    assert stringify_special_floats(np.float32("nan")) == "NaN"
    assert stringify_special_floats(np.float32("inf")) == "Infinity"
    assert stringify_special_floats(np.float32("-inf")) == "-Infinity"


# Basic types
def test_make_json_safe_basic_types():
    assert make_json_safe(None) is None
    assert make_json_safe(True) is True
    assert make_json_safe("hello") == "hello"
    assert make_json_safe(42) == 42
    assert make_json_safe(3.14) == 3.14


# Special floats
def test_make_json_safe_nan_and_inf():
    assert make_json_safe(float("nan")) is None
    assert make_json_safe(float("inf")) is None
    assert make_json_safe(np.nan) is None
    assert make_json_safe(np.inf) is None


# NumPy types
def test_make_json_safe_numpy_bool():
    result = make_json_safe(np.bool_(True))
    assert result is True
    assert isinstance(result, bool)


def test_make_json_safe_numpy_integers():
    assert make_json_safe(np.int32(100)) == 100
    assert make_json_safe(np.int64(1000)) == 1000
    assert isinstance(make_json_safe(np.int64(42)), int)


def test_make_json_safe_numpy_floats():
    assert make_json_safe(np.float32(2.5)) == 2.5
    assert make_json_safe(np.float64(3.5)) == 3.5


def test_make_json_safe_numpy_arrays():
    arr = np.array([1, 2, 3])
    assert make_json_safe(arr) == [1, 2, 3]

    arr_with_nan = np.array([1.0, np.nan, 3.0])
    assert make_json_safe(arr_with_nan) == [1.0, None, 3.0]


# Pandas types
def test_make_json_safe_pandas_types():
    assert make_json_safe(pd.NaT) is None
    assert make_json_safe(pd.NA) is None

    ts = pd.Timestamp("2024-01-15 12:30:45")
    assert "2024-01-15T12:30:45" in make_json_safe(ts)

    td = pd.Timedelta(hours=2)
    assert make_json_safe(td) == 7200.0


# Astropy Time objects
def test_make_json_safe_astropy_time():
    t = Time("2024-01-15T12:30:45")
    result = make_json_safe(t)
    assert isinstance(result, str)
    assert "2024-01-15" in result

    # Test with array of times
    t_array = Time(["2024-01-15", "2024-01-16"])
    result = make_json_safe(t_array)
    assert isinstance(result, list)
    assert len(result) == 2


# Containers
def test_make_json_safe_containers():
    assert make_json_safe([1, 2, 3]) == [1, 2, 3]
    assert make_json_safe({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    nested = {"values": [np.int64(1), np.nan], "count": np.int32(5)}
    result = make_json_safe(nested)
    assert result == {"values": [1, None], "count": 5}


def test_make_json_safe_tuple():
    result = make_json_safe((1, 2, 3))
    assert result == [1, 2, 3]
    assert isinstance(result, list)

    # Tuples nested inside other containers are also converted to lists.
    assert make_json_safe([(1, 2), (3, 4)]) == [[1, 2], [3, 4]]
    assert make_json_safe({"t": (1, np.int64(2))}) == {"t": [1, 2]}


# NumPy 0-d arrays
def test_make_json_safe_numpy_scalar_array():
    assert make_json_safe(np.array(5)) == 5
    assert isinstance(make_json_safe(np.array(5)), int)
    assert make_json_safe(np.array(np.nan)) is None


# NumPy datetime64 / timedelta64
def test_make_json_safe_numpy_datetime64():
    dt64 = np.datetime64("2024-01-15T12:30:45")
    result = make_json_safe(dt64)
    assert "2024-01-15T12:30:45" in result

    assert make_json_safe(np.datetime64("NaT", "ns")) is None


def test_make_json_safe_numpy_timedelta64():
    assert make_json_safe(np.timedelta64(2, "h")) == 7200.0
    assert make_json_safe(np.timedelta64("NaT", "ns")) is None


# NumPy floating NaN/Inf (distinct from the plain-float branch)
def test_make_json_safe_numpy_floating_nan_and_inf():
    assert make_json_safe(np.float64("nan")) is None
    assert make_json_safe(np.float32("inf")) is None
    assert make_json_safe(np.float64("-inf")) is None


# Unhandled type falls through unchanged
def test_make_json_safe_fallback_unhandled_type():
    value = 3 + 4j
    assert make_json_safe(value) is value


# JSON serialization
def test_make_json_safe_json_serializable():
    obj = {
        "int": np.int64(42),
        "float": np.float64(3.14),
        "nan": np.nan,
        "timestamp": pd.Timestamp("2024-01-15"),
    }
    result = make_json_safe(obj)
    json_str = json.dumps(result)  # Should not raise
    assert json_str is not None
