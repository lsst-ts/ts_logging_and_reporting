import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from lsst.ts.logging_and_reporting.web_app.main import get_data_log


@pytest.fixture
def mock_dataframe():
    return pd.DataFrame([
        {
            "exposure id": 2025060100001,
            "exposure name": "MC_O_20250601_000001",
            "day obs": 20250601,
            "seq num": 1,
            "physical filter": "g_6",
            "band": "g",
            "s ra": 244.90862809233787,
            "s dec": -30.23819582215416,
            "sky rotation": 188.1969891770383,
            "azimuth": 112.77994040225623,
            "altitude": 47.11203028318442,
            "airmass": 2.4938499796510014,
            "obs start": "2025-06-01T22:34:14.471000",
            "img type": "acq",
            "science program": "BLOCK-T123",
            "observation reason": "because",
            "target name": "",
            "air temp": 9.1234,
            "dimm seeing": None,
            "psf sigma median": 3.576219103584274,
            "sky bg median": 567.0356518118432,
            "zero point median": np.nan,
            "high snr source count median": None,
            "test_inf_field": np.inf,
            "test_neg_inf_field": -np.inf,
        },
        {
            "exposure id": 2025060100002,
            "exposure name": "MC_O_20250601_000002",
            "day obs": 20250601,
            "seq num": 2,
            "physical filter": "g_6",
            "band": "g",
            "s ra": 248.90862780923387,
            "s dec": -27.82381952215416,
            "sky rotation": 168.9891177039683,
            "azimuth": 107.76799404022523,
            "altitude": 49.30124128318042,
            "airmass": 2.6514939798490014,
            "obs start": "2025-06-01T22:35:04.295000",
            "img type": "acq",
            "science program": "BLOCK-T123",
            "observation reason": "because",
            "target name": "shiny thing",
            "air temp": 9.1234,
            "dimm seeing": 1.9149397658490014,
            "psf sigma median": 3.576219103584274,
            "sky bg median": 567.0356518118432,
            "zero point median": 39.29069020327295,
            "high snr source count median": None,
        },
        {
            "exposure id": 2025060100003,
            "exposure name": "MC_O_20250601_000003",
            "day obs": 20250601,
            "seq num": 3,
            "physical filter": "g_6",
            "band": "g",
            "s ra": 204.86037879928230,
            "s dec": -20.82238195412156,
            "sky rotation": 161.7383701916989,
            "azimuth": 100.42562077299403,
            "altitude": 50.08442280331112,
            "airmass": 4.4938499796510014,
            "obs start": "2025-06-01T22:36:31.836000",
            "img type": "science",
            "science program": "BLOCK-T124",
            "observation reason": "important stuff to be seen",
            "target name": "important stuff here",
            "air temp": 9.1234,
            "dimm seeing": np.nan,
            "psf sigma median": 2.036219142585774,
            "sky bg median": 621.1180518432356,
            "zero point median": 20.20369027229095,
            "high snr source count median": None,
        },
    ])

@patch(
        "lsst.ts.logging_and_reporting.consdb.ut.get_auth_header",
        return_value={"Authorization": "Bearer mocktoken"}
    )
@patch("lsst.ts.logging_and_reporting.web_app.services.consdb_service.nd_utils.Server.get_url", return_value="mock://url")
@patch("lsst.ts.logging_and_reporting.web_app.services.consdb_service.ConsdbAdapter")
def test_get_data_log(mock_adapter_cls, mock_get_url, mock_get_auth_header, mock_dataframe):
    # Arrange
    mock_adapter = MagicMock()
    mock_adapter.get_exposures.return_value = mock_dataframe
    mock_adapter.verbose = False
    mock_adapter_cls.return_value = mock_adapter

    # Act
    result = get_data_log(
        dayobs_start=20250601,
        dayobs_end=20250601,
        telescope="LSSTcam"
    )

    # Assert
    assert isinstance(result, list)
    assert len(result) == 3

    row0 = result[0]
    assert row0["dimm seeing"] == "NaN"
    assert row0["zero point median"] == "NaN"
    assert row0["psf sigma median"] == 3.576219103584274
    assert row0["img type"] == "acq"
    assert row0["test_inf_field"] == "inf"
    assert row0["test_neg_inf_field"] == "-inf"

    mock_adapter_cls.assert_called_once()
    mock_adapter.get_exposures.assert_called_once_with(instrument="LSSTcam")
