import pytest
from fastapi import HTTPException
from rubin_sim.sim_archive import NoMatchingSimulationsFoundError

from lsst.ts.logging_and_reporting.services.expected_exposures import ExpectedExposuresService


class StubAdapter:
    """Stands in for the expected-exposures adapter."""

    def __init__(self, per_day=None, error=None):
        self.per_day = per_day or {}
        self.error = error
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        if self.error is not None:
            raise self.error
        return self.per_day


def make_service(per_day=None, error=None):
    return ExpectedExposuresService(expected_exposures_adapter=StubAdapter(per_day, error))


class TestExpectedExposures:
    def test_sums_counts_across_range(self):
        response = make_service({20250101: 100, 20250102: 250}).handle_request(20250101, 20250102)
        assert response == {"sum_exposures": 350}

    def test_empty_range_sums_to_zero(self):
        response = make_service({}).handle_request(20250101, 20250101)
        assert response == {"sum_exposures": 0}

    def test_bounds_passed_to_adapter_inclusive(self):
        adapter = StubAdapter({20250101: 1})
        ExpectedExposuresService(expected_exposures_adapter=adapter).handle_request(20250101, 20250103)
        assert adapter.calls == [(20250101, 20250103)]

    def test_no_matching_simulation_maps_to_404(self):
        service = make_service(error=NoMatchingSimulationsFoundError("no sim for that night"))
        with pytest.raises(HTTPException) as exc:
            service.handle_request(20250101, 20250101)
        assert exc.value.status_code == 404
