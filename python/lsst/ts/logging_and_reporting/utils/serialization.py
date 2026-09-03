#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Helpers for making values safe to JSON-serialize."""

import math

import numpy as np
import pandas as pd


def stringify_special_floats(val):
    """
    Convert special float values into JSON-safe string representations.

    This function ensures that pandas DataFrames containing NaN,
    positive infinity, or negative infinity values can be safely
    serialized to JSON without causing errors.

    Parameters
    ----------
    val : any
        The value to check and possibly convert.

    Returns
    -------
    any
        - "NaN" if the value is NaN
        - "Infinity" if the value is positive infinity
        - "-Infinity" if the value is negative infinity
        - The original value otherwise
    """
    if isinstance(val, (float, np.floating)):
        if np.isnan(val):
            return "NaN"
        elif np.isposinf(val):
            return "Infinity"
        elif np.isneginf(val):
            return "-Infinity"
    return val


def make_json_safe(obj):
    """
    Recursively converts objects to be JSON serializable.

    This function traverses the input object, converting
    any non-JSON-serializable types (such as Astropy Time objects,
    NumPy integers, floats, NaN, or infinity) into types
    that can be safely serialized.
    Dictionaries and lists are processed recursively. NaN and infinity values
    are replaced with None.

    Parameters
    ----------
    obj : any
        The object to convert. Can be a dict, list, or any value.

    Returns
    -------
    any
        The converted object, safe for JSON serialization.
    """
    if obj is None or isinstance(obj, (bool, str)):
        return obj

    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_safe(v) for v in obj]

    # Check for Astropy Time BEFORE NumPy array check
    obj_type_name = type(obj).__name__
    if obj_type_name == "Time" and hasattr(obj, "to_datetime"):
        dt = obj.to_datetime()
        # Handle both scalar and array Time objects
        if isinstance(dt, np.ndarray):
            return [make_json_safe(v) for v in dt]
        return dt.isoformat()

    if isinstance(obj, np.ndarray):
        if obj.ndim == 0:
            return make_json_safe(obj.item())
        return [make_json_safe(v) for v in obj.tolist()]

    if obj is pd.NaT or obj is pd.NA:
        return None

    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    if isinstance(obj, np.datetime64):
        if pd.isnull(obj):
            return None
        return pd.Timestamp(obj).isoformat()

    if isinstance(obj, (pd.Timedelta, np.timedelta64)):
        if pd.isnull(obj):
            return None
        return float(pd.Timedelta(obj).total_seconds())

    if isinstance(obj, np.bool_):
        return bool(obj)

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        v = float(obj)
        return None if (np.isnan(v) or np.isinf(v)) else v

    if isinstance(obj, int):
        return obj

    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj

    return obj
