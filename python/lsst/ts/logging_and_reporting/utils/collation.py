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
"""Helpers for shaping per-dayobs adapter data into service responses."""


def flatten_sorted(data: dict[int, list[dict]], sort_field: str, descending: bool = True) -> list[dict]:
    """Flatten per-dayobs records into one list sorted by a field.

    Parameters
    ----------
    data : `dict` [`int`, `list` [`dict`]]
        Records partitioned by dayobs.
    sort_field : `str`
        Record field to sort the flattened list by. Records missing
        the field sort as empty strings.
    descending : `bool`, optional
        Sort order; descending by default (newest first for timestamp
        fields).

    Returns
    -------
    `list` [`dict`]
        All records in one sorted list.
    """
    records = [record for dayobs in sorted(data) for record in data[dayobs]]
    records.sort(key=lambda record: record.get(sort_field) or "", reverse=descending)
    return records


def flatten_within_dayobs(data: dict[int, list[dict]], sort_field: str) -> list[dict]:
    """Flatten per-dayobs records, ordering within each night by a field.

    For fields that only count within a night, such as ``seq_num``: the
    dayobs ordering is the outer key, so `flatten_sorted`'s single global
    sort would interleave nights.

    Parameters
    ----------
    data : `dict` [`int`, `list` [`dict`]]
        Records partitioned by dayobs.
    sort_field : `str`
        Numeric record field to order each night's records by. Records
        missing the field sort as zero.

    Returns
    -------
    `list` [`dict`]
        All records in one list, ascending by dayobs then ``sort_field``.
    """
    records = []
    for dayobs in sorted(data):
        records.extend(sorted(data[dayobs], key=lambda record: record.get(sort_field) or 0))
    return records
