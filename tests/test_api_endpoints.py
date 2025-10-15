from fastapi.testclient import TestClient
import os
import pytest
import requests
from datetime import datetime, timedelta
from unittest.mock import patch, Mock, MagicMock

from bokeh.plotting import figure
import pandas as pd

from rubin_nights.connections import get_clients

from lsst.ts.logging_and_reporting import __version__
from lsst.ts.logging_and_reporting.web_app.main import app
from lsst.ts.logging_and_reporting.utils import get_access_token
import lsst.ts.logging_and_reporting.utils as ut


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
    "/nightreport/reports": [
        {
            "id": "10753873-f651-4e3b-9832-f7c42661aea6",
            "site_id": "summit",
            "day_obs": 20250730,
            "summary": (
                "In terms of the checkout and procedures, AuxTel and Simonyi"
                " passed without major issues. \nAlarm for Chiller.2 and "
                "Chiller.3, were triggered; they seemed not to be real,"
                " it was more likely a software issue. This problem is"
                " still under investigation.\n\nA couple of AOS test were"
                " completed during the Simonyi night, together with SV FBS"
                " observations. AuxTel was up most of the night with only"
                " one recurrent issue that was temporarily solved."
            ),
            "weather": (
                "Warm and clear sky. The evening outside temperature "
                "reported was around 12°C. The air temperature at sunset"
                " is 11.7°C, and the wind speed ~3.5 m/s. At around 02:00"
                " UT the sky covered by clouds for about 30 minutes."
                " The rest of the night was clear."
            ),
            "maintel_summary": (
                "Operational activities Hardpoint and Bump Test for M1M3,"
                " were conducted successfully, and the MTCamera completed"
                " its warm-up without issues. Today, the M2 hexapod warm-up"
                " was run in the calibration position. During the initial"
                " attempt, using max_iteration value of 700, the script"
                " published many TimeOut errors until it eventually failed."
                " We cycled the Hexapod CSC, changed the max_iter value to"
                " 500, and the script completed, even though it was clear"
                ' that "y" and "x" were struggling to complete movements'
                " even at 22 degrees elevation. \nThe HVAC chiller triggered"
                " the system's alarms, reporting pressure-related alarms"
                " likely caused by a temporary software communication glitch."
                " This was resolved on its own; the telemetry was recovered,"
                " and the system was working at 100% (OBS-1171).  \n\nOnce"
                " on-sky, we started with the Initial alignment block"
                " (BLOCK-T539). Afterwards we moved to BLOCK-T579 LUT Update"
                " Test with one dome fault (OBS-696) instance. At 02:00 "
                "clouds covered the sky, after 30 minutes they sky cleared"
                " up and we were able to continue. Once the LUT Update Test"
                " was finished we moved to SV FBS observations. At the end "
                "of the night we completed the remaining bending modes of "
                "the BLOCK-T598 Sensitivity Matrix Repeatability "
                "(m2_b10, m2_b11, m2_dz). \n\nDuring SV FBS, we had a "
                "recurrent fault in the scheduler reporting to fail to update"
                " telemetry (OBS-1160). The issue seems related with the "
                "lfa files from DREAM. A change in the scheduler "
                "configuration was deployed avoiding the issue for the rest"
                ' of SV FBS observations.\n\nThe "water drop noise" was'
                " heard several instances. The timestamps are recorder "
                "on OBS-1158.\n"
            ),
            "auxtel_summary": (
                "AuxTel Weekend Calibrations have been performed. The "
                "ATQueue was fulfilled with the procedure prepared to"
                " go on sky, and fulffiled with a re-enabling of "
                "the Scheduler2 after the venting procedure."
                " The venting stops at 22.13 UTC, and the Scheduler"
                " waits until 22:49 UTC before populating the observing"
                " queue (Sun elevation -8 degrees).\n\nThe procedure to"
                " switch off the ATWhiteLight, after calibrations, did"
                " not work properly. The problem was that the "
                "MTWhileLight CSC was not connected to the lamp "
                'controller, we executed the "power_on_atcalsys" '
                "script, and the CSC went to fault, turned off the light"
                " (OBS-1172).\n\nOnce we went to sky, we were not able"
                " to correct for pointing and LATISS images were not"
                " properly transferred, it was found that the DIMM set"
                " the SEEING header value to .nan making that the"
                " rapid analysis failed (OBS-1174), User 1. and "
                "User 2. provided support to help find the solution."
                " \n\nAuxTel correct_pointing recurrent issues (OBS-1169)"
                " has been temporary solved by User 3. who identify that "
                "the ATPtg.mountPositions topic stopped publishing on the"
                " night of Aug 9th. Possibly related with the power glitch"
                " suffered that night. The ATMCS cRIO/ATMCS CSC in argoCD"
                " were restarted without success in the telemetry return."
                " Since the topic was only informative, it was removed "
                "from the info statement on the run branch. \n"
            ),
            "confluence_url": "https://rubinobs.atlassian.net/projects/BLOCK?selectedItem=com.atlassian.plugins.atlassian-connect-plugin:com.kanoah.test-manager__main-project-page#!/testPlayer/BLOCK-R341",
            "user_id": "test@localhost",
            "user_agent": "nightreport-service",
            "date_added": "2025-07-30T22:06:14.003952",
            "date_sent": "2025-07-31T09:59:14.156767",
            "is_valid": True,
            "date_invalidated": None,
            "parent_id": "7b4881c2-aed9-45f3-8710-513c368b2338",
            "observers_crew": [
                "User 1",
                "User 2",
                "User 3",
            ],
        }
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


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data == {"status": "ok"}


def test_version_endpoint():
    response = client.get("/version")
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == __version__


def test_nightreport_endpoint(mock_requests_get):
    endpoint = "/night-reports?dayObsStart=20250730&dayObsEnd=20250731"
    _test_endpoint_authentication(endpoint)

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert "reports" in data
    assert len(data["reports"]) == 1
    report = data["reports"][0]
    expected_params = [
        "id",
        "site_id",
        "day_obs",
        "summary",
        "weather",
        "maintel_summary",
        "auxtel_summary",
        "confluence_url",
        "user_id",
        "user_agent",
        "date_added",
        "date_sent",
        "is_valid",
        "date_invalidated",
        "parent_id",
        "observers_crew",
    ]
    for param in expected_params:
        assert param in report, f"Missing {param} in night report: {report}"
    app.dependency_overrides.pop(get_access_token, None)


def test_exposure_entries_endpoint(mock_requests_get):
    endpoint = "/exposure-entries?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
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

    with (
        patch("lsst.ts.logging_and_reporting.web_app.main.get_open_close_dome") as mock_open_close,
        patch("lsst.ts.logging_and_reporting.web_app.main.get_time_accounting") as mock_time_accounting,
    ):
        import pandas as pd

        mock_open_close.return_value = pd.DataFrame({"open_hours": [2.5]})
        mock_time_accounting.return_value = pd.DataFrame(
            {
                "exposure_id": [2025073000001],
                "exposure_name": ["MC_O_20250730_000001"],
                "exp_time": [30],
                "img_type": ["science"],
                "observation_reason": ["BLOCK-365"],
                "science_program": ["field_survey_science"],
                "target_name": ["Rubin_SV_225_-40"],
                "can_see_sky": [True],
                "band": ["r"],
                "obs_start": ["2025-07-30T23:33:43.069000"],
                "physical_filter": ["r_57"],
                "day_obs": [20250730],
                "seq_num": [1],
                "obs_end": ["2025-07-30T23:34:13.999000"],
                "overhead": [9.0],
                "zero_point_median": [32.1],
                "visit_id": [2025071600135],
                "pixel_scale_median": [0.2],
                "psf_sigma_median": [1.1],
            }
        )
        app.dependency_overrides[get_access_token] = lambda: "dummy-token"
        app.dependency_overrides[get_clients] = lambda: {"efd": Mock()}

        response = client.get(endpoint)
        assert response.status_code == 200
        data = response.json()
        assert "exposures" in data
        assert data["exposures_count"] == 1
        assert data["sum_exposure_time"] == 30
        assert data["on_sky_exposures_count"] == 1
        assert data["total_on_sky_exposure_time"] == 30
        assert data["open_dome_hours"] == 2.5
        mock_time_accounting.assert_called_once()
        mock_open_close.assert_called_once()

        # test that the request succeeds if the
        # rubin_nights data wasn't available
        mock_open_close.return_value = pd.DataFrame()
        mock_time_accounting.return_value = pd.DataFrame()
        response = client.get(endpoint)
        assert response.status_code == 200
        data = response.json()
        assert "exposures" in data
        assert data["exposures_count"] == 1
        assert data["open_dome_hours"] == 0

        app.dependency_overrides.pop(get_access_token, None)
        app.dependency_overrides.pop(get_clients, None)


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


def test_context_feed_endpoint(monkeypatch):
    endpoint = "/context-feed?dayObsStart=20240101&dayObsEnd=20240102"

    dummy_cols = [
        "time",
        "name",
        "description",
        "config",
        "script_salIndex",
        "salIndex",
        "finalStatus",
        "timestampProcessStart",
        "timestampConfigureEnd",
        "timestampRunStart",
        "timestampProcessEnd",
    ]
    dummy_data = [
        {
            "time": "2024-01-01T01:23:45Z",
            "name": "ScriptQueue",
            "description": "Dummy run",
            "config": "config-string",
            "script_salIndex": 1,
            "salIndex": 2,
            "finalStatus": "SUCCESS",
            "timestampProcessStart": "2024-01-01T01:00:00Z",
            "timestampConfigureEnd": "2024-01-01T01:05:00Z",
            "timestampRunStart": "2024-01-01T01:10:00Z",
            "timestampProcessEnd": "2024-01-01T01:20:00Z",
        }
    ]

    # Patch before auth check so real function never runs
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_context_feed",
        lambda dayObsStart, dayObsEnd, auth_token: (dummy_data, dummy_cols),
    )

    # Authentication test --
    _test_endpoint_authentication(endpoint)

    # API test --
    # Override token-fetching dependency
    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    # Make request
    response = client.get(endpoint)
    assert response.status_code == 200

    # Parse JSON response
    data = response.json()
    assert "data" in data
    assert "cols" in data
    assert data["cols"] == dummy_cols

    # Verify cols match data cols
    for record in data["data"]:
        for col in dummy_cols:
            assert col in record

    # Remove override
    app.dependency_overrides.pop(get_access_token, None)

    # Error-path API test --
    # Simulate a service failure by patching get_context_feed
    # to raise an Exception
    def raise_error(dayObsStart, dayObsEnd, auth_token):
        raise Exception("failure")

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_context_feed",
        raise_error,
    )

    # Override token again
    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    # Expect API to return 500 with exception message
    response = client.get(endpoint)
    assert response.status_code == 500
    assert response.json()["detail"] == "failure"

    # Clean up override
    app.dependency_overrides.pop(get_access_token, None)


@pytest.fixture
def sample_visit_data_for_visit_maps():
    """Sample visit data for testing
    multi-night visit maps."""

    base_date = datetime(2024, 1, 1)
    visits_list = []

    for day_offset in range(3):
        day_obs = int((base_date + timedelta(days=day_offset)).strftime("%Y%m%d"))
        for obs_idx in range(5):
            visits_list.append(
                {
                    "day_obs": day_obs,
                    "observationStartMJD": 60000.0 + day_offset + obs_idx * 0.1,
                    "fieldRA": 180.0 + obs_idx,
                    "fieldDec": -30.0 + obs_idx,
                    "band": "r",
                    "rotSkyPos": 45.0,
                }
            )

    return pd.DataFrame(visits_list)


@pytest.fixture
def mock_conditions():
    mock_cond = MagicMock()
    mock_cond.mjd = 60000.0
    mock_cond.sun_ra = 0.5
    mock_cond.sun_dec = -0.5
    mock_cond.moon_ra = 1.0
    mock_cond.moon_dec = 0.2
    mock_cond.sun_n12_setting = 60000.0
    mock_cond.sun_n12_rising = 60000.5

    return mock_cond


@patch("lsst.ts.logging_and_reporting.web_app.main.read_visits")
@patch("lsst.ts.logging_and_reporting.web_app.main.ModelObservatory")
@patch("lsst.ts.logging_and_reporting.web_app.main.create_visit_skymaps")
@patch("lsst.ts.logging_and_reporting.web_app.main.add_coords_tuple")
def test_visit_maps_applet_mode_planisphere_only(
    mock_add_coords,
    mock_create_skymaps,
    mock_observatory,
    mock_read_visits,
    sample_visit_data_for_visit_maps,
):
    mock_read_visits.return_value = sample_visit_data_for_visit_maps
    mock_add_coords.return_value = sample_visit_data_for_visit_maps

    dummy_fig = figure(title="Test Figure")
    mock_create_skymaps.return_value = (dummy_fig, {})

    mock_observatory_instance = MagicMock()
    mock_observatory.return_value = mock_observatory_instance

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    response = client.get(
        "/multi-night-visit-maps",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240103,
            "instrument": "lsstCam",
            "planisphereOnly": True,
            "appletMode": True,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "interactive" in data
    assert isinstance(data["interactive"], dict)
    assert "target_id" in data["interactive"]
    assert "root_id" in data["interactive"]

    mock_read_visits.assert_called_once()
    mock_create_skymaps.assert_called_once()

    call_kwargs = mock_create_skymaps.call_args[1]
    assert call_kwargs["planisphere_only"] is True
    assert call_kwargs["applet_mode"] is True
    assert call_kwargs["timezone"] == "UTC"

    app.dependency_overrides.pop(get_access_token, None)


@patch("lsst.ts.logging_and_reporting.web_app.main.read_visits")
@patch("lsst.ts.logging_and_reporting.web_app.main.ModelObservatory")
@patch("lsst.ts.logging_and_reporting.web_app.main.create_visit_skymaps")
@patch("lsst.ts.logging_and_reporting.web_app.main.add_coords_tuple")
def test_visit_maps_full_mode_both_maps(
    mock_add_coords,
    mock_create_skymaps,
    mock_observatory,
    mock_read_visits,
    sample_visit_data_for_visit_maps,
):
    mock_read_visits.return_value = sample_visit_data_for_visit_maps
    mock_add_coords.return_value = sample_visit_data_for_visit_maps

    dummy_fig = figure(title="Test Figure")
    mock_create_skymaps.return_value = (dummy_fig, {})

    mock_observatory_instance = MagicMock()
    mock_observatory.return_value = mock_observatory_instance

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    response = client.get(
        "/multi-night-visit-maps",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240104,
            "instrument": "latiss",
            "planisphereOnly": False,
            "appletMode": False,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "interactive" in data
    assert "target_id" in data["interactive"]
    assert "root_id" in data["interactive"]

    mock_read_visits.assert_called_once()
    call_kwargs = mock_read_visits.call_args[1]
    assert call_kwargs["num_nights"] == 3

    call_kwargs = mock_create_skymaps.call_args[1]
    assert call_kwargs["planisphere_only"] is False
    assert call_kwargs["applet_mode"] is False

    app.dependency_overrides.pop(get_access_token, None)


@patch("lsst.ts.logging_and_reporting.web_app.main.read_visits")
@patch("lsst.ts.logging_and_reporting.web_app.main.ModelObservatory")
def test_visit_maps_no_visits_data(
    mock_observatory,
    mock_read_visits,
):
    # empty visits DataFrame
    mock_read_visits.return_value = pd.DataFrame()
    mock_observatory_instance = MagicMock()
    mock_observatory.return_value = mock_observatory_instance

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    response = client.get(
        "/multi-night-visit-maps",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    # Should still return 200 with empty interactive data
    assert response.status_code == 200
    data = response.json()
    assert "interactive" in data
    assert data["interactive"] is None

    app.dependency_overrides.pop(get_access_token, None)


@patch("lsst.ts.logging_and_reporting.web_app.main.read_visits")
@patch("lsst.ts.logging_and_reporting.web_app.main.ModelObservatory")
def test_visit_maps_read_visits_exception(
    mock_observatory,
    mock_read_visits,
):
    mock_read_visits.side_effect = Exception("Database connection error")

    mock_observatory_instance = MagicMock()
    mock_observatory.return_value = mock_observatory_instance

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"

    response = client.get(
        "/multi-night-visit-maps",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    assert response.status_code == 500
    assert "Database connection error" in response.json()["detail"]

    app.dependency_overrides.pop(get_access_token, None)


def test_expected_exposures_endpoint(monkeypatch):
    endpoint = "/expected-exposures?dayObsStart=20240101&dayObsEnd=20240102"

    dummy_expected_exposures = {
        "nightly": [100, 120],
        "sum": 220,
    }

    # Patch get_expected_exposures to return dummy data
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_expected_exposures",
        lambda dayObsStart, dayObsEnd: dummy_expected_exposures,
    )

    # Success path --
    response = client.get(endpoint)
    assert response.status_code == 200

    data = response.json()
    assert "nightly_exposures" in data
    assert "sum_exposures" in data
    assert data["nightly_exposures"] == dummy_expected_exposures["nightly"]
    assert data["sum_exposures"] == dummy_expected_exposures["sum"]
    assert sum(data["nightly_exposures"]) == data["sum_exposures"]

    # Error path (generic exception) --
    def raise_error(dayObsStart, dayObsEnd):
        raise Exception("failure")

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_expected_exposures",
        raise_error,
    )

    response = client.get(endpoint)
    assert response.status_code == 500
    assert response.json()["detail"] == "failure"
