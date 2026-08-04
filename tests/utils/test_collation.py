import pytest

from lsst.ts.logging_and_reporting.utils.collation import flatten_sorted, flatten_within_dayobs


class TestFlattenSorted:
    def test_sorts_descending_by_default(self):
        data = {
            20250101: [{"key": "a", "date_added": "2025-01-01T00:00:00"}],
            20250102: [{"key": "b", "date_added": "2025-01-02T00:00:00"}],
        }
        assert [record["key"] for record in flatten_sorted(data, "date_added")] == ["b", "a"]

    def test_sorts_ascending_when_asked(self):
        data = {
            20250101: [{"key": "a", "date_added": "2025-01-01T00:00:00"}],
            20250102: [{"key": "b", "date_added": "2025-01-02T00:00:00"}],
        }
        result = flatten_sorted(data, "date_added", descending=False)
        assert [record["key"] for record in result] == ["a", "b"]

    def test_the_sort_is_global_not_per_night(self):
        # The dayobs buckets do not survive: a record from a later night
        # can precede one from an earlier night. Fields whose values only
        # order within a night want flatten_within_dayobs instead.
        data = {
            20250101: [{"key": "early_night", "date_added": "2025-01-02T00:00:00"}],
            20250102: [{"key": "late_night", "date_added": "2025-01-09T00:00:00"}],
        }
        result = flatten_sorted(data, "date_added")
        assert [record["key"] for record in result] == ["late_night", "early_night"]

    def test_ties_keep_dayobs_order_even_when_descending(self):
        # list.sort is stable and reverse=True does not flip equal
        # elements, so the dayobs pass acts as the tiebreak.
        data = {
            20250102: [{"key": "b", "date_added": "2025-01-01T00:00:00"}],
            20250101: [{"key": "a", "date_added": "2025-01-01T00:00:00"}],
        }
        assert [record["key"] for record in flatten_sorted(data, "date_added")] == ["a", "b"]

    def test_records_missing_the_field_sort_as_empty_strings(self):
        data = {20250101: [{"key": "a"}, {"key": "b", "date_added": "2025-01-01T00:00:00"}]}
        assert [record["key"] for record in flatten_sorted(data, "date_added")] == ["b", "a"]

    def test_a_numeric_field_needs_every_record_to_carry_a_truthy_value(self):
        # The missing-value fallback is "", so a numeric sort field is
        # only safe while every record has a non-zero value for it —
        # which is why seq_num goes through flatten_within_dayobs.
        data = {20250101: [{"key": "a", "day_obs": 20250101}, {"key": "b"}]}
        with pytest.raises(TypeError):
            flatten_sorted(data, "day_obs")

    def test_empty_input_returns_an_empty_list(self):
        assert flatten_sorted({}, "date_added") == []


class TestFlattenWithinDayobs:
    def test_orders_nights_then_records_within_each(self):
        data = {
            20250102: [{"key": "c", "seq_num": 2}, {"key": "b", "seq_num": 1}],
            20250101: [{"key": "a", "seq_num": 1}],
        }
        assert [record["key"] for record in flatten_within_dayobs(data, "seq_num")] == ["a", "b", "c"]

    def test_dayobs_outranks_the_sort_field(self):
        # The distinction from flatten_sorted: a per-night sequence
        # restarts each night, so a global sort would interleave them.
        data = {
            20250101: [{"key": "a", "seq_num": 99}],
            20250102: [{"key": "b", "seq_num": 1}],
        }
        assert [record["key"] for record in flatten_within_dayobs(data, "seq_num")] == ["a", "b"]

    def test_records_missing_the_field_sort_first(self):
        data = {20250101: [{"key": "a", "seq_num": 3}, {"key": "b"}]}
        assert [record["key"] for record in flatten_within_dayobs(data, "seq_num")] == ["b", "a"]

    def test_missing_and_zero_tie_and_keep_their_given_order(self):
        data = {20250101: [{"key": "a"}, {"key": "b", "seq_num": 0}, {"key": "c"}]}
        assert [record["key"] for record in flatten_within_dayobs(data, "seq_num")] == ["a", "b", "c"]

    def test_nights_with_no_records_contribute_nothing(self):
        data = {20250101: [], 20250102: [{"key": "a", "seq_num": 1}]}
        assert [record["key"] for record in flatten_within_dayobs(data, "seq_num")] == ["a"]

    def test_empty_input_returns_an_empty_list(self):
        assert flatten_within_dayobs({}, "seq_num") == []

    def test_records_are_the_originals_not_copies(self):
        record = {"key": "a", "seq_num": 1}
        assert flatten_within_dayobs({20250101: [record]}, "seq_num")[0] is record
