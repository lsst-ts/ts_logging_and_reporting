import os
import pytest
import requests

import lsst.ts.logging_and_reporting.utils as ut

from fastapi.testclient import TestClient
from lsst.ts.logging_and_reporting.web_app.main import app
from lsst.ts.logging_and_reporting.utils import get_access_token
from unittest.mock import patch, Mock

client = TestClient(app)


SERVICE_ENDPOINT_MOCK_RESPONSES = {
    "/exposurelog/instruments": {
        "butler_instruments_1": [
            "LSSTComCamSim",
            "LATISS",
            "LSSTComCam",
            "LSSTCam",
        ],
    },
    "/exposurelog/exposures": [
        {
            "obs_id": "MC_O_20250730_000001",
            "id": 2025073000001,
            "instrument": "LSSTCam",
            "observation_type": "bias",
            "observation_reason": "bias",
            "day_obs": 20250730,
            "seq_num": 1,
            "group_name": "2025-07-30T20:14:23.653",
            "target_name": "azel_target",
            "science_program": "unknown",
            "tracking_ra": None,
            "tracking_dec": None,
            "sky_angle": None,
            "timespan_begin": "2025-07-30T20:14:23.836969",
            "timespan_end": "2025-07-30T20:14:23.849000",
        },
    ],
    "/exposurelog/messages": [
        {
            "id": "6e915887-0dd0-4335-aa30-fa6a8e61660a",
            "site_id": "summit",
            "obs_id": "MC_O_20250730_000001",
            "instrument": "LSSTCam",
            "day_obs": 20250730,
            "seq_num": 1,
            "message_text": (
                "Filter change, the M2 haxapod (strut) went  out of position and returned"
                " to its  previous values\r\n"
            ),
            "level": 10,
            "tags": [
                "undefined",
            ],
            "urls": [],
            "user_id": "test@localhost",
            "user_agent": "exposurelog-service",
            "is_human": True,
            "is_valid": True,
            "exposure_flag": "junk",
            "date_added": "2025-07-30T22:14:23.266086",
            "date_invalidated": None,
            "parent_id": None,
        },
    ],
    "/consdb/schema": {
        "schema": [],
    },
    "/consdb/query": {
        "columns": [
            "exposure_id",
            "exposure_name",
            "controller",
            "day_obs",
            "seq_num",
            "physical_filter",
            "band",
            "s_ra",
            "s_dec",
            "sky_rotation",
            "azimuth_start",
            "azimuth_end",
            "azimuth",
            "altitude_start",
            "altitude_end",
            "altitude",
            "zenith_distance_start",
            "zenith_distance_end",
            "zenith_distance",
            "airmass",
            "exp_midpt",
            "exp_midpt_mjd",
            "obs_start",
            "obs_start_mjd",
            "obs_end",
            "obs_end_mjd",
            "exp_time",
            "shut_time",
            "dark_time",
            "group_id",
            "cur_index",
            "max_index",
            "img_type",
            "emulated",
            "science_program",
            "observation_reason",
            "target_name",
            "air_temp",
            "pressure",
            "humidity",
            "wind_speed",
            "wind_dir",
            "dimm_seeing",
            "focus_z",
            "simulated",
            "vignette",
            "vignette_min",
            "s_region",
            "scheduler_note",
            "can_see_sky",
            "visit_id",
            "day_obs",
            "seq_num",
            "n_inputs",
            "pixel_scale_min",
            "pixel_scale_max",
            "pixel_scale_median",
            "astrom_offset_mean_min",
            "astrom_offset_mean_max",
            "astrom_offset_mean_median",
            "astrom_offset_std_min",
            "astrom_offset_std_max",
            "astrom_offset_std_median",
            "eff_time_min",
            "eff_time_max",
            "eff_time_median",
            "eff_time_psf_sigma_scale_min",
            "eff_time_psf_sigma_scale_max",
            "eff_time_psf_sigma_scale_median",
            "eff_time_sky_bg_scale_min",
            "eff_time_sky_bg_scale_max",
            "eff_time_sky_bg_scale_median",
            "eff_time_zero_point_scale_min",
            "eff_time_zero_point_scale_max",
            "eff_time_zero_point_scale_median",
            "stats_mag_lim_min",
            "stats_mag_lim_max",
            "stats_mag_lim_median",
            "psf_ap_flux_delta_min",
            "psf_ap_flux_delta_max",
            "psf_ap_flux_delta_median",
            "psf_ap_corr_sigma_scaled_delta_min",
            "psf_ap_corr_sigma_scaled_delta_max",
            "psf_ap_corr_sigma_scaled_delta_median",
            "max_dist_to_nearest_psf_min",
            "max_dist_to_nearest_psf_max",
            "max_dist_to_nearest_psf_median",
            "mean_var_min",
            "mean_var_max",
            "mean_var_median",
            "n_psf_star_min",
            "n_psf_star_max",
            "n_psf_star_median",
            "n_psf_star_total",
            "psf_area_min",
            "psf_area_max",
            "psf_area_median",
            "psf_ixx_min",
            "psf_ixx_max",
            "psf_ixx_median",
            "psf_ixy_min",
            "psf_ixy_max",
            "psf_ixy_median",
            "psf_iyy_min",
            "psf_iyy_max",
            "psf_iyy_median",
            "psf_sigma_min",
            "psf_sigma_max",
            "psf_sigma_median",
            "psf_star_delta_e1_median_min",
            "psf_star_delta_e1_median_max",
            "psf_star_delta_e1_median_median",
            "psf_star_delta_e1_scatter_min",
            "psf_star_delta_e1_scatter_max",
            "psf_star_delta_e1_scatter_median",
            "psf_star_delta_e2_median_min",
            "psf_star_delta_e2_median_max",
            "psf_star_delta_e2_median_median",
            "psf_star_delta_e2_scatter_min",
            "psf_star_delta_e2_scatter_max",
            "psf_star_delta_e2_scatter_median",
            "psf_star_delta_size_median_min",
            "psf_star_delta_size_median_max",
            "psf_star_delta_size_median_median",
            "psf_star_delta_size_scatter_min",
            "psf_star_delta_size_scatter_max",
            "psf_star_delta_size_scatter_median",
            "psf_star_scaled_delta_size_scatter_min",
            "psf_star_scaled_delta_size_scatter_max",
            "psf_star_scaled_delta_size_scatter_median",
            "psf_trace_radius_delta_min",
            "psf_trace_radius_delta_max",
            "psf_trace_radius_delta_median",
            "sky_bg_min",
            "sky_bg_max",
            "sky_bg_median",
            "sky_noise_min",
            "sky_noise_max",
            "sky_noise_median",
            "seeing_zenith_500nm_min",
            "seeing_zenith_500nm_max",
            "seeing_zenith_500nm_median",
            "zero_point_min",
            "zero_point_max",
            "zero_point_median",
            "low_snr_source_count_min",
            "low_snr_source_count_max",
            "low_snr_source_count_median",
            "low_snr_source_count_total",
            "high_snr_source_count_min",
            "high_snr_source_count_max",
            "high_snr_source_count_median",
            "high_snr_source_count_total",
            "z4",
            "z5",
            "z6",
            "z7",
            "z8",
            "z9",
            "z10",
            "z11",
            "z12",
            "z13",
            "z14",
            "z15",
            "z16",
            "z17",
            "z18",
            "z19",
            "z20",
            "z21",
            "z22",
            "z23",
            "z24",
            "z25",
            "z26",
            "z27",
            "z28",
            "ringss_seeing",
            "aos_fwhm",
            "donut_blur_fwhm",
            "physical_rotator_angle",
        ],
        "data": [
            [
                2025073000001,
                "MC_O_20250730_000001",
                "O",
                20250730,
                1,
                "r_57",
                "r",
                223.98160761028913,
                -38.47251714841746,
                214.5835616140157,
                147.890872803333,
                148.364345585214,
                148.1875048738588,
                79.962007878832,
                80.0211721975604,
                79.98837362381481,
                10.037992121168003,
                9.978827802439596,
                10.011626376185191,
                1.0155152572474475,
                "2025-07-30T23:33:58.534000",
                60872.981927475375,
                "2025-07-30T23:33:43.069000",
                60872.981748480845,
                "2025-07-30T23:34:13.999000",
                60872.982106469906,
                30.0,  # exp_time
                30.000021934509277,
                30.930307149887085,
                "2025-07-30T23:33:25.587",
                1,
                1,
                "science",
                None,
                "BLOCK-365",
                "field_survey_science",
                "Rubin_SV_225_-40",
                10.925000190734863,
                74375.0,
                3.6500000953674316,
                1.417799949645996,
                332.7699890136719,
                1.592031478881836,
                -2.517409119738486,
                None,
                "NO",
                "NO",
                (
                    "Polygon ICRS 224.141971 -36.430646 226.331121 -37.601423"
                    " 226.178218 -37.786952 226.204528 -37.781783 226.439414"
                    " -37.903219 226.141683 -38.325247 226.295989 -38.139533"
                    " 226.594131 -38.576481 225.096460 -40.329430 224.849697"
                    " -40.202984 224.694377 -40.410317 224.449363 -40.283736"
                    " 224.387252 -40.292723 224.219693 -40.478742 223.809895"
                    " -40.514161 221.574282 -39.295526 221.742171 -39.116264"
                    " 221.654996 -38.807240 221.885362 -38.937682 221.412038"
                    " -38.626410 221.578861 -38.447174 221.376787 -38.310850"
                    " 222.921622 -36.606110 223.151654 -36.735725 223.376292"
                    " -36.841340 223.145414 -36.712043 223.981718 -36.591738"
                    " 223.821559 -36.777264"
                ),
                "Rubin_SV_225_-40",
                True,
                2025071600135,
                20250716,
                135,
                189,
                None,
                None,
                None,
                0.005855140741914511,
                0.037743836641311646,
                0.013799363747239113,
                0.0031081733759492636,
                0.018279051408171654,
                0.007472486235201359,
                3.4409351348876953,
                8.433363914489746,
                6.292477607727051,
                0.2043115794658661,
                0.3478623330593109,
                0.28445112705230713,
                0.41277727484703064,
                0.7668044567108154,
                0.6340815424919128,
                0.7321116328239441,
                2.4645116329193115,
                1.1551984548568726,
                23.6835880279541,
                24.127700805664062,
                23.950923919677734,
                None,
                None,
                None,
                None,
                None,
                None,
                539.647705078125,
                2864.3671875,
                904.5316162109375,
                48.500614166259766,
                6306.08837890625,
                1580.742431640625,
                0,
                123,
                91,
                17312,
                113.57209014892578,
                184.65811157226562,
                137.0709228515625,
                6.5269575119018555,
                11.480645179748535,
                8.075767517089844,
                -0.9237869381904602,
                0.796488344669342,
                0.03614915907382965,
                7.255825519561768,
                12.607644081115723,
                9.044437408447266,
                2.6449708938598633,
                3.451263427734375,
                2.9249677658081055,
                -0.004791662096977234,
                0.00305322278290987,
                1.0833609849214554e-05,
                0.004929862916469574,
                0.020184984430670738,
                0.009087219834327698,
                -0.0035890955477952957,
                0.004892665892839432,
                -0.00016600824892520905,
                0.00532715767621994,
                0.019489606842398643,
                0.010255683213472366,
                -0.019053561612963676,
                0.007478527724742889,
                -0.004949399270117283,
                0.012609905563294888,
                0.0545121468603611,
                0.021554280072450638,
                0.004641216713935137,
                0.01764441840350628,
                0.007301146164536476,
                0.020858056843280792,
                1.253257393836975,
                0.14162714779376984,
                1289.1170654296875,
                2394.755615234375,
                1558.94921875,
                4.87351131439209,
                95.76366424560547,
                43.181095123291016,
                None,
                None,
                None,
                31.883525848388672,
                32.5424690246582,
                32.131126403808594,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ],
    },
}


def mock_get_response():
    """Function that returns
    a mocked `requests.Response` object for simulating HTTP GET requests.

    The returned response object has a status code of 200 and
    a custom `.json()` method that returns a mock payload
    based on the requested endpoint. The endpoint is determined
    by extracting the URL from the most recent call to `requests.get`
    and mapping it to a predefined mock response.

    Intended to be used with `unittest.mock.patch` to mock
    the `requests.get` method in tests.

    Examples
    --------
    ```python
    mock_requests_get_patcher = patch("requests.get")
    mock_requests_get = mock_requests_get_patcher.start()
    mock_requests_get.return_value = mock_get_response()
    ... calls to requests.get ...
    mock_requests_get_patcher.stop()
    ```

    Yields
    ------
        mocked_response : requests.Response
            A mocked response object with a custom `.json()` method dependent
            on the queried service endpoint.
    """
    response_get = requests.Response()
    response_get.status_code = 200

    def response_json_payload():
        called_url = requests.get.call_args[0][0]
        endpoint = called_url.replace(ut.Server.get_url(), "").split("?")[0]
        return SERVICE_ENDPOINT_MOCK_RESPONSES[endpoint]

    response_get.json = response_json_payload
    return response_get


def mock_post_response():
    """Function that returns
    a mocked `requests.Response` object for simulating HTTP POST requests.

    The returned response object has a status code of 200 and
    a custom `.json()` method that returns a mock payload
    based on the requested endpoint. The endpoint is determined
    by extracting the URL from the most recent call to `requests.post`
    and mapping it to a predefined mock response.

    Intended to be used with `unittest.mock.patch` to mock
    the `requests.post` method in tests.

    Examples
    --------
    ```python
    mock_requests_post_patcher = patch("requests.post")
    mock_requests_post = mock_requests_post_patcher.start()
    mock_requests_post.return_value = mock_post_response()
    ... calls to requests.post ...
    mock_requests_post_patcher.stop()
    ```

    Yields
    ------
        mocked_response : requests.Response
            A mocked response object with a custom `.json()` method dependent
            on the queried service endpoint.
    """
    response_post = requests.Response()
    response_post.status_code = 200

    def response_json_payload():
        called_url = requests.post.call_args[0][0]
        endpoint = called_url.replace(ut.Server.get_url(), "").split("?")[0]
        return SERVICE_ENDPOINT_MOCK_RESPONSES[endpoint]

    response_post.json = response_json_payload
    return response_post


def _test_endpoint_auth_header(endpoint):
    headers = {"Authorization": "Bearer header-token"}
    response = client.get(endpoint, headers=headers)
    assert response.status_code == 200


def _test_endpoint_auth_env_var(endpoint):
    os.environ["ACCESS_TOKEN"] = "env-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    del os.environ["ACCESS_TOKEN"]


def _test_endpoint_auth_rsp_utils(endpoint):
    mock_lsst = Mock()
    mock_lsst.rsp.utils.get_info.return_value = "mocked-token"

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp.utils": mock_lsst.rsp.utils,
        },
    ):
        response = client.get(endpoint)
        assert response.status_code == 200


def _test_endpoint_no_auth(endpoint):
    response = client.get(endpoint)
    assert response.status_code == 401


def _test_endpoint_authentication(endpoint):
    _test_endpoint_auth_header(endpoint)
    _test_endpoint_auth_env_var(endpoint)
    _test_endpoint_auth_rsp_utils(endpoint)
    _test_endpoint_no_auth(endpoint)


@pytest.fixture
def mock_requests_get():
    patcher = patch("requests.get")
    mock_get = patcher.start()
    mock_get.return_value = mock_get_response()
    yield mock_get
    patcher.stop()


@pytest.fixture
def mock_requests_post():
    patcher = patch("requests.post")
    mock_post = patcher.start()
    mock_post.return_value = mock_post_response()
    yield mock_post
    patcher.stop()


def test_exposure_entries_endpoint(mock_requests_get):
    endpoint = (
        "/exposure-entries?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
    )
    _test_endpoint_authentication(endpoint)

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert "exposure_entries" in data
    expected_entry_params = [
        "obs_id",
        "id",
        "instrument",
        "observation_type",
        "observation_reason",
        "day_obs",
        "seq_num",
        "group_name",
        "target_name",
        "science_program",
        "tracking_ra",
        "tracking_dec",
        "sky_angle",
        "timespan_begin",
        "timespan_end",
        "exposure_flag",
        "exposure_time",
        "message_text",
    ]
    for entry in data["exposure_entries"]:
        for param in expected_entry_params:
            assert param in entry, f"Missing {param} in exposure entry: {entry}"
    app.dependency_overrides.pop(get_access_token, None)


def test_exposures_endpoint(mock_requests_get, mock_requests_post):
    endpoint = "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
    _test_endpoint_authentication(endpoint)

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert "exposures" in data
    assert data["exposures_count"] == 1
    assert data["sum_exposure_time"] == 30
    assert data["on_sky_exposures_count"] == 1
    assert data["total_on_sky_exposure_time"] == 30

    app.dependency_overrides.pop(get_access_token, None)


def test_almanac_endpoint(monkeypatch):
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_almanac",
        lambda dayObsStart, dayObsEnd: {"sunset": 123, "sunrise": 456},
    )
    response = client.get("/almanac?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    data = response.json()
    assert "almanac_info" in data
    assert data["almanac_info"] == {"sunset": 123, "sunrise": 456}
