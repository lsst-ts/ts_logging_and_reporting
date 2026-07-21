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

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from bokeh.plotting import figure
from fastapi import HTTPException
from fastapi.testclient import TestClient
from rubin_sim.sim_archive import NoMatchingSimulationsFoundError

import lsst.ts.logging_and_reporting.utils as ut
from lsst.ts.logging_and_reporting import __version__
from lsst.ts.logging_and_reporting import services as web_services
from lsst.ts.logging_and_reporting.adapters.almanac import get_almanac_adapter
from lsst.ts.logging_and_reporting.adapters.exposurelog import get_exposurelog_adapter
from lsst.ts.logging_and_reporting.adapters.jira_block import get_jira_block_adapter
from lsst.ts.logging_and_reporting.adapters.jira_obs import get_jira_obs_adapter
from lsst.ts.logging_and_reporting.adapters.narrativelog import get_narrativelog_adapter
from lsst.ts.logging_and_reporting.adapters.nightreport import get_nightreport_adapter
from lsst.ts.logging_and_reporting.adapters.zephyr import get_zephyr_adapter
from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError
from lsst.ts.logging_and_reporting.main import app, rsp_auth
from lsst.ts.logging_and_reporting.services.block_details import ZEPHYR_TEST_CASE_PATH
from lsst.ts.logging_and_reporting.services.data_log import DataLogService
from lsst.ts.logging_and_reporting.services.expected_exposures import ExpectedExposuresService
from lsst.ts.logging_and_reporting.services.exposures import ExposuresService

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
    "/narrativelog/messages": [
        {
            "id": "8a1a3ea6-9c1a-4a52-b587-4b135b7b46d9",
            "site_id": "summit",
            "message_text": "M2 hexapod strut fault during filter change",
            "level": 100,
            "tags": [],
            "urls": [],
            "time_lost": 1.5,
            "date_begin": "2025-07-30T22:00:00",
            "user_id": "test@localhost",
            "user_agent": "narrativelog-service",
            "is_human": True,
            "is_valid": True,
            "date_added": "2025-07-31T01:00:00",
            "date_invalidated": None,
            "parent_id": None,
            "date_end": "2025-07-30T23:00:00",
            "components_json": {"name": "Simonyi"},
            "category": "fault",
            "time_lost_type": "fault",
        },
        {
            "id": "0273cd7a-8c53-40a1-9c15-2f3e1d4f0f61",
            "site_id": "summit",
            "message_text": "Clouds covered the sky for 30 minutes",
            "level": 100,
            "tags": [],
            "urls": [],
            "time_lost": 0.5,
            "date_begin": "2025-07-31T02:00:00",
            "user_id": "test@localhost",
            "user_agent": "narrativelog-service",
            "is_human": True,
            "is_valid": True,
            "date_added": "2025-07-31T02:30:00",
            "date_invalidated": None,
            "parent_id": None,
            "date_end": "2025-07-31T02:30:00",
            "components_json": {"name": "Simonyi"},
            "category": "weather",
            "time_lost_type": "weather",
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


def _test_endpoint_authentication(endpoint, monkeypatch):
    # Header auth
    response = client.get(endpoint, headers={"Authorization": "Bearer header-token"})
    assert response.status_code == 200

    # Env auth
    monkeypatch.setenv("ACCESS_TOKEN", "env-token")
    response = client.get(endpoint)
    assert response.status_code == 200
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)

    # RSP utils (RSPDiscovery)
    mock_rspdiscovery = Mock()
    mock_rspdiscovery.get_token.return_value = "mocked-discovery-token"

    mock_lsst = Mock()
    mock_lsst.rsp._services.RSPDiscovery = mock_rspdiscovery

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp._services": mock_lsst.rsp._services,
        },
    ):
        response = client.get(endpoint)
        assert response.status_code == 200

    # No auth --> 401
    response = client.get(endpoint)
    assert response.status_code == 401


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


@pytest.fixture
def exposurelog_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_exposurelog_adapter(), "_redis", fake_redis)
    return fake_redis


@pytest.fixture
def narrativelog_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_narrativelog_adapter(), "_redis", fake_redis)
    return fake_redis


@pytest.fixture
def nightreport_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_nightreport_adapter(), "_redis", fake_redis)
    return fake_redis


@pytest.fixture
def almanac_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_almanac_adapter(), "_redis", fake_redis)
    return fake_redis


@pytest.fixture
def jira_obs_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_jira_obs_adapter(), "_redis", fake_redis)
    return fake_redis


@pytest.fixture
def block_details_cache(fake_redis, monkeypatch):
    monkeypatch.setattr(get_jira_block_adapter(), "_redis", fake_redis)
    monkeypatch.setattr(get_zephyr_adapter(), "_redis", fake_redis)
    return fake_redis


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


def test_nightreport_endpoint(mock_requests_get, monkeypatch, nightreport_cache):
    monkeypatch.setenv("ACCESS_TOKEN", "env-token")
    endpoint = "/night-reports?dayObsStart=20250730&dayObsEnd=20250731"

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

    # Second identical request is served from the cache
    mock_requests_get.reset_mock()
    response = client.get(endpoint)
    assert response.status_code == 200
    mock_requests_get.assert_not_called()

    # A cold fetch with no token source available fails auth
    nightreport_cache.flushdb()
    monkeypatch.delenv("ACCESS_TOKEN")
    response = client.get(endpoint)
    assert response.status_code == 401


def test_narrative_log_endpoint(mock_requests_get, monkeypatch, narrativelog_cache):
    monkeypatch.setenv("ACCESS_TOKEN", "env-token")
    endpoint = "/narrative-log?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam"

    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert len(data["narrative_log"]) == 2
    assert [m["date_begin"] for m in data["narrative_log"]] == [
        "2025-07-31T02:00:00",
        "2025-07-30T22:00:00",
    ]
    assert all(m["instrument"] == "LSSTCam" for m in data["narrative_log"])
    assert data["time_lost_to_weather"] == 0.5
    assert data["time_lost_to_faults"] == 1.5

    # Second identical request is served from the cache
    mock_requests_get.reset_mock()
    response = client.get(endpoint)
    assert response.status_code == 200
    mock_requests_get.assert_not_called()

    # A cold fetch with no token source available fails auth
    narrativelog_cache.flushdb()
    monkeypatch.delenv("ACCESS_TOKEN")
    response = client.get(endpoint)
    assert response.status_code == 401


def test_exposure_entries_endpoint(mock_requests_get, monkeypatch, exposurelog_cache):
    monkeypatch.setenv("ACCESS_TOKEN", "env-token")
    endpoint = "/exposure-entries?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam"

    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert "exposure_entries" in data
    assert len(data["exposure_entries"]) == 1
    expected_entry_params = [
        "id",
        "instrument",
        "day_obs",
        "seq_num",
        "exposure_flag",
        "level",
        "tags",
        "urls",
        "user_id",
        "user_agent",
        "is_human",
        "is_valid",
        "date_added",
        "date_invalidated",
        "parent_id",
        "message_text",
    ]
    for entry in data["exposure_entries"]:
        for param in expected_entry_params:
            assert param in entry, f"Missing {param} in exposure entry: {entry}"

    # Second identical request is served from the cache
    mock_requests_get.reset_mock()
    response = client.get(endpoint)
    assert response.status_code == 200
    mock_requests_get.assert_not_called()

    # A cold fetch with no token source available fails auth
    exposurelog_cache.flushdb()
    monkeypatch.delenv("ACCESS_TOKEN")
    response = client.get(endpoint)
    assert response.status_code == 401


def test_exposure_flags_endpoint(mock_requests_get, monkeypatch, exposurelog_cache):
    monkeypatch.setenv("ACCESS_TOKEN", "env-token")
    endpoint = "/exposure-flags?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam"

    response = client.get(endpoint)
    assert response.status_code == 200
    assert response.json() == {
        "exposure_flags": [{"obs_id": "MC_O_20250730_000001", "exposure_flag": "junk"}]
    }


EXPOSURES_ENDPOINT = "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
DATA_LOG_ENDPOINT = "/data-log?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"


class _StubService:
    """Returns a canned response, for endpoint wiring tests."""

    def __init__(self, result):
        self.result = result

    def handle(self, *args, **kwargs):
        return self.result


class _RaisingAdapter:
    """Adapter stand-in that raises when fetched."""

    def __init__(self, error):
        self.error = error

    def fetch(self, *args):
        raise self.error


@pytest.fixture
def override_service():
    yield app.dependency_overrides
    app.dependency_overrides.clear()


def test_exposures_endpoint_returns_service_response(override_service):
    result = {
        "exposures": [{"exposure_id": 1}],
        "exposures_count": 1,
        "sum_exposure_time": 30,
        "on_sky_exposures_count": 1,
        "total_on_sky_exposure_time": 30,
        "open_dome_times": [],
        "day_obs_open_dome_hours": {},
        "open_dome_error": None,
        "night_on_sky_time_accounting": {"sum": 0.0},
        "time_accounting_error": None,
    }
    override_service[web_services.get_exposures_service] = lambda: _StubService(result)
    override_service[rsp_auth] = lambda: "token"

    response = client.get(EXPOSURES_ENDPOINT)
    assert response.status_code == 200
    assert response.json() == result


def test_exposures_endpoint_consdb_failure_returns_502(override_service):
    service = ExposuresService(adapters={"consdb": _RaisingAdapter(requests.ConnectionError("consdb down"))})
    override_service[web_services.get_exposures_service] = lambda: service
    override_service[rsp_auth] = lambda: "token"

    response = client.get(EXPOSURES_ENDPOINT)
    assert response.status_code == 502


def test_exposures_endpoint_unknown_instrument_returns_422(override_service):
    service = ExposuresService(
        adapters={"consdb": _RaisingAdapter(HTTPException(status_code=422, detail="Unknown instrument"))}
    )
    override_service[web_services.get_exposures_service] = lambda: service
    override_service[rsp_auth] = lambda: "token"

    response = client.get(EXPOSURES_ENDPOINT)
    assert response.status_code == 422


def test_data_log_endpoint_returns_service_response(override_service):
    result = {"data_log": [{"exposure_id": 1, "value": "NaN"}]}
    override_service[web_services.get_data_log_service] = lambda: _StubService(result)

    response = client.get(DATA_LOG_ENDPOINT)
    assert response.status_code == 200
    assert response.json() == result


def test_data_log_endpoint_consdb_failure_returns_502(override_service):
    service = DataLogService(adapters={"consdb": _RaisingAdapter(requests.ConnectionError("consdb down"))})
    override_service[web_services.get_data_log_service] = lambda: service

    response = client.get(DATA_LOG_ENDPOINT)
    assert response.status_code == 502


def test_jira_tickets_endpoint(monkeypatch, jira_obs_cache):
    monkeypatch.setenv("JIRA_API_TOKEN", "env-token")
    monkeypatch.setenv("JIRA_API_HOSTNAME", "jira.test")
    endpoint = "/jira-tickets?dayObsStart=20250730&dayObsEnd=20250731&instrument=LATISS"

    issue = {
        "key": "OBS-1",
        "fields": {
            "summary": "Test ticket",
            "created": "2025-07-30T20:00:00.000+0000",
            "updated": "2025-07-30T22:00:00.000+0000",
            "status": {"name": "In Progress"},
            "customfield_10476": [{"name": "AuxTel"}],
            "customfield_10106": 1.5,
        },
    }

    def respond(url, **kwargs):
        response = Mock()
        response.raise_for_status.return_value = None
        if url.endswith("/myself"):
            response.json.return_value = {"timeZone": "UTC"}
        else:
            response.json.return_value = {"issues": [issue]}
        return response

    with patch("requests.get", side_effect=respond) as mock_get:
        response = client.get(endpoint)
        assert response.status_code == 200
        data = response.json()
        assert len(data["issues"]) == 1
        ticket = data["issues"][0]
        assert ticket["key"] == "OBS-1"
        assert ticket["system"] == ["AuxTel"]
        assert ticket["url"] == "https://jira.test/browse/OBS-1"
        assert ticket["isNew"] is True
        assert "created_utc" not in ticket

        # Second identical request is served from the cache
        mock_get.reset_mock()
        response = client.get(endpoint)
        assert response.status_code == 200
        mock_get.assert_not_called()

    # An upstream Jira failure maps to 502
    jira_obs_cache.flushdb()
    with patch("requests.get", side_effect=requests.ConnectionError("jira down")):
        response = client.get(endpoint)
        assert response.status_code == 502

    # A cold fetch with no token source available fails auth
    monkeypatch.delenv("JIRA_API_TOKEN")
    response = client.get(endpoint)
    assert response.status_code == 401


def test_almanac_endpoint(monkeypatch, almanac_cache):
    compute_night = Mock(
        side_effect=lambda dayobs: {
            "dayobs": dayobs,
            "night_hours": 9.5,
            "twilight_evening_12deg": "2024-01-01 23:00:00",
            "twilight_morning_12deg": "2024-01-02 08:30:00",
        }
    )
    monkeypatch.setattr(get_almanac_adapter(), "_compute_night", compute_night)

    response = client.get("/almanac?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    data = response.json()
    # Records are labeled by the morning-boundary dayobs
    assert [r["dayobs"] for r in data["almanac_info"]] == [20240102]
    record = data["almanac_info"][0]
    assert record["night_hours"] == 9.5
    # The 2024 night is long finished, so the full night counts
    assert record["elapsed_twilight_hours"] == pytest.approx(9.5)

    # Second identical request is served from the cache
    compute_night.reset_mock()
    response = client.get("/almanac?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    compute_night.assert_not_called()


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
        "lsst.ts.logging_and_reporting.main.get_context_feed",
        lambda dayObsStart, dayObsEnd, auth_token: (dummy_data, dummy_cols),
    )

    # Authentication test --
    _test_endpoint_authentication(endpoint, monkeypatch)

    # API test --
    # Override token-fetching dependency
    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

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
    app.dependency_overrides.pop(rsp_auth, None)

    # Error-path API test --
    # Simulate a service failure by patching get_context_feed
    # to raise an Exception
    def raise_error(dayObsStart, dayObsEnd, auth_token):
        raise Exception("failure")

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_context_feed",
        raise_error,
    )

    # Override token again
    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    # Expect API to return 500 with exception message
    response = client.get(endpoint)
    assert response.status_code == 500
    assert response.json()["detail"] == "failure"

    # Clean up override
    app.dependency_overrides.pop(rsp_auth, None)


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


@patch("lsst.ts.logging_and_reporting.main.get_visits")
@patch("lsst.ts.logging_and_reporting.main.build_visit_maps_using_builder")
def test_visit_maps_full_mode_both_maps(
    mock_build_visitmaps_using_builder,
    mock_get_visits,
    sample_visit_data_for_visit_maps,
):
    mock_get_visits.return_value = sample_visit_data_for_visit_maps

    # dummy figure with mock data to return from the builder function
    dummy_fig = figure(title="Test Figure")
    dummy_fig.scatter(
        x=[180.0, 181.0],
        y=[-30.0, -29.5],
        size=10,
        color=["navy", "firebrick"],
    )
    mock_build_visitmaps_using_builder.return_value = dummy_fig

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(
        "/multi-night-visit-maps",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240104,
            "instrument": "latiss",
            "appletMode": False,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "interactive" in data
    assert "target_id" in data["interactive"]
    assert "root_id" in data["interactive"]

    mock_get_visits.assert_called_once()
    call_kwargs = mock_get_visits.call_args[1]

    call_kwargs = mock_build_visitmaps_using_builder.call_args[1]
    assert call_kwargs["applet_mode"] is False

    app.dependency_overrides.pop(rsp_auth, None)


@pytest.fixture
def sample_visit_data_for_static_map():
    """Sample visit data for testing static visit maps."""

    return pd.DataFrame(
        [
            {
                "science_program": "BLOCK-365",
                "s_ra": 180.0,
                "s_dec": -30.0,
                "sky_rotation": 45.0,
                "obs_start_mjd": 60000.1,
            },
            {
                "s_ra": 181.5,
                "s_dec": -29.5,
                "sky_rotation": 46.0,
                "obs_start_mjd": 60000.2,
                "science_program": "BLOCK-365",
            },
        ]
    )


@patch("lsst.ts.logging_and_reporting.main.build_static_visit_map")
@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_static_visit_map_success(
    mock_get_visits,
    mock_build_static_visit_map,
    sample_visit_data_for_static_map,
):
    mock_get_visits.return_value = sample_visit_data_for_static_map
    mock_build_static_visit_map.return_value = b"fake-png-bytes"

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(
        "/static-visit-map",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["static_map"]["mime_type"] == "image/png"
    assert data["static_map"]["data"]

    mock_get_visits.assert_called_once_with(
        20240101, 20240102, "lsstCam", auth_token="dummy-token", augment=False
    )
    mock_build_static_visit_map.assert_called_once()
    map_data = mock_build_static_visit_map.call_args.args[0]
    assert len(map_data) == len(sample_visit_data_for_static_map)

    app.dependency_overrides.pop(rsp_auth, None)


@patch("lsst.ts.logging_and_reporting.main._encode_png_payload")
@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_static_visit_map_no_valid_rows(
    mock_get_visits,
    mock_encode_png_payload,
):
    mock_get_visits.return_value = pd.DataFrame(
        [
            {
                "science_program": "dummy_program",
                "s_ra": 180.0,
                "s_dec": -30.0,
                "sky_rotation": 45.0,
                "obs_start_mjd": 60000.1,
            }
        ]
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(
        "/static-visit-map",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"static_map": None}
    mock_encode_png_payload.assert_not_called()
    app.dependency_overrides.pop(rsp_auth, None)


@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_static_visit_map_get_visits_consdb_error(mock_get_visits):
    mock_get_visits.side_effect = ConsdbQueryError("consdb down")

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(
        "/static-visit-map",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "ConsDB query failed"

    app.dependency_overrides.pop(rsp_auth, None)


@patch("lsst.ts.logging_and_reporting.main.build_static_visit_map")
@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_static_visit_map_build_failure_returns_500(
    mock_get_visits,
    mock_build_static_visit_map,
    sample_visit_data_for_static_map,
):
    mock_get_visits.return_value = sample_visit_data_for_static_map
    mock_build_static_visit_map.side_effect = RuntimeError("plot failed")

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(
        "/static-visit-map",
        params={
            "dayObsStart": 20240101,
            "dayObsEnd": 20240102,
            "instrument": "lsstCam",
        },
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to generate static visit map"

    app.dependency_overrides.pop(rsp_auth, None)


@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_visit_maps_no_visits_data(
    mock_get_visits,
):
    # empty visits DataFrame
    mock_get_visits.return_value = pd.DataFrame()

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

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

    app.dependency_overrides.pop(rsp_auth, None)


@patch("lsst.ts.logging_and_reporting.main.get_visits")
def test_visit_maps_read_visits_exception(
    mock_get_visits,
):
    mock_get_visits.side_effect = Exception("Database connection error")

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

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

    app.dependency_overrides.pop(rsp_auth, None)


def test_expected_exposures_endpoint_returns_service_response(override_service):
    override_service[web_services.get_expected_exposures_service] = lambda: _StubService(
        {"sum_exposures": 220}
    )
    response = client.get("/expected-exposures?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    assert response.json() == {"sum_exposures": 220}


def test_expected_exposures_endpoint_no_sim_returns_404(override_service):
    service = ExpectedExposuresService(
        adapters={"expected_exposures": _RaisingAdapter(NoMatchingSimulationsFoundError("no sim"))}
    )
    override_service[web_services.get_expected_exposures_service] = lambda: service
    response = client.get("/expected-exposures?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 404


def block_requests_get(zephyr_cases, jira_summaries, zephyr_error=None, jira_error=None):
    """Route Zephyr test-case GETs and Jira BLOCK searches."""

    def respond(url, **kwargs):
        response = Mock()
        response.raise_for_status.return_value = None
        if "/testcases/" in url:
            if zephyr_error is not None:
                raise zephyr_error
            key = url.rsplit("/", 1)[-1]
            if key in zephyr_cases:
                response.json.return_value = {"key": key, "name": zephyr_cases[key]}
            else:
                response.status_code = 404
                response.raise_for_status.side_effect = requests.HTTPError("404 Not Found", response=response)
        else:
            if jira_error is not None:
                raise jira_error
            response.json.return_value = {
                "issues": [
                    {"key": key, "fields": {"summary": summary}} for key, summary in jira_summaries.items()
                ]
            }
        return response

    return Mock(side_effect=respond)


def block_details_env(monkeypatch):
    monkeypatch.setenv("ZEPHYR_API_TOKEN", "zephyr-token")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    monkeypatch.setenv("JIRA_API_HOSTNAME", "mock-host")


def test_block_details_endpoint(monkeypatch, block_details_cache):
    block_details_env(monkeypatch)
    endpoint = "/block-details?key=BLOCK-T123&key=BLOCK-T123_a&key=BLOCK-456&key=BLOCK-456&key=INVALID-1"

    mock_get = block_requests_get({"BLOCK-T123": "Zephyr case"}, {"BLOCK-456": "Jira block"})
    with patch("requests.get", mock_get):
        response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    zephyr_url = f"https://mock-host{ZEPHYR_TEST_CASE_PATH}BLOCK-T123"
    assert data["data"] == {
        "BLOCK-T123": {
            "key": "BLOCK-T123",
            "summary": "Zephyr case",
            "source": "zephyr",
            "url": zephyr_url,
        },
        "BLOCK-T123_a": {
            "key": "BLOCK-T123_a",
            "summary": "Zephyr case",
            "source": "zephyr",
            "url": zephyr_url,
        },
        "BLOCK-456": {
            "key": "BLOCK-456",
            "summary": "Jira block",
            "source": "jira",
            "url": "https://mock-host/browse/BLOCK-456",
        },
    }
    assert data["errors"] == {}

    # Second identical request is served from the cache
    mock_get = block_requests_get({}, {})
    with patch("requests.get", mock_get):
        response = client.get(endpoint)
    assert response.status_code == 200
    mock_get.assert_not_called()

    # A cold fetch with no tokens available fails auth
    block_details_cache.flushdb()
    monkeypatch.delenv("ZEPHYR_API_TOKEN")
    monkeypatch.delenv("JIRA_API_TOKEN")
    with patch("requests.get", block_requests_get({}, {})):
        response = client.get(endpoint)
    assert response.status_code == 401


def test_block_details_endpoint_unknown_keys_omitted(monkeypatch, block_details_cache):
    block_details_env(monkeypatch)
    with patch("requests.get", block_requests_get({}, {})):
        response = client.get("/block-details?key=BLOCK-T999&key=BLOCK-999")
    assert response.status_code == 200
    assert response.json() == {"data": {}, "errors": {}}


def test_block_details_endpoint_zephyr_failure(monkeypatch, block_details_cache):
    block_details_env(monkeypatch)
    mock_get = block_requests_get(
        {},
        {"BLOCK-456": "Jira block"},
        zephyr_error=requests.ConnectionError("Zephyr down"),
    )
    with patch("requests.get", mock_get):
        response = client.get("/block-details?key=BLOCK-T123&key=BLOCK-456")
    assert response.status_code == 200
    data = response.json()
    assert list(data["data"]) == ["BLOCK-456"]
    assert "Zephyr down" in data["errors"]["zephyr"]


def test_block_details_endpoint_jira_failure(monkeypatch, block_details_cache):
    block_details_env(monkeypatch)
    mock_get = block_requests_get(
        {"BLOCK-T123": "Zephyr case"},
        {},
        jira_error=requests.ConnectionError("Jira down"),
    )
    with patch("requests.get", mock_get):
        response = client.get("/block-details?key=BLOCK-T123&key=BLOCK-456")
    assert response.status_code == 200
    data = response.json()
    assert list(data["data"]) == ["BLOCK-T123"]
    assert "Jira down" in data["errors"]["jira"]


def test_block_details_endpoint_both_sources_failing(monkeypatch, block_details_cache):
    block_details_env(monkeypatch)
    mock_get = block_requests_get(
        {},
        {},
        zephyr_error=requests.ConnectionError("Zephyr down"),
        jira_error=requests.ConnectionError("Jira down"),
    )
    with patch("requests.get", mock_get):
        response = client.get("/block-details?key=BLOCK-T123&key=BLOCK-456")
    assert response.status_code == 500
    assert response.json()["detail"] == "Both Zephyr and Jira requests failed."


@pytest.fixture
def dummy_response():
    return {
        "entries": [],
        "intervals": [],
        "summary": {
            "fault_loss": 1.23,
        },
    }


@pytest.fixture(autouse=True)
def clear_dependency_overrides():
    """Ensure dependency overrides never leak between tests."""
    yield
    app.dependency_overrides.clear()


def test_obs_status_happy_path(monkeypatch, dummy_response):
    endpoint = (
        "/obs-status"
        "?dayObsStart=20250101"
        "&dayObsEnd=20250102"
        "&includeEntries=true"
        "&includeIntervals=false"
        "&nightOnlyMetrics=true"
        "&metric=fault_loss"
        "&metric=weather_loss"
    )

    captured_args = {}

    def mock_get_obs_status(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
        auth_token,
    ):
        captured_args["dayObsStart"] = dayObsStart
        captured_args["dayObsEnd"] = dayObsEnd
        captured_args["includeEntries"] = includeEntries
        captured_args["includeIntervals"] = includeIntervals
        captured_args["nightOnlyMetrics"] = nightOnlyMetrics
        captured_args["metrics"] = metrics
        captured_args["auth_token"] = auth_token

        return dummy_response

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        mock_get_obs_status,
    )

    # Authentication test
    _test_endpoint_authentication(endpoint, monkeypatch)

    # Override auth dependency
    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 200
    assert response.json() == dummy_response

    assert captured_args == {
        "dayObsStart": 20250101,
        "dayObsEnd": 20250102,
        "includeEntries": True,
        "includeIntervals": False,
        "nightOnlyMetrics": True,
        "metrics": ["fault_loss", "weather_loss"],
        "auth_token": "dummy-token",
    }


def test_obs_status_defaults(monkeypatch, dummy_response):
    endpoint = "/obs-status?dayObsStart=20250101&dayObsEnd=20250102"

    captured_args = {}

    def mock_get_obs_status(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
        auth_token,
    ):
        captured_args["includeEntries"] = includeEntries
        captured_args["includeIntervals"] = includeIntervals
        captured_args["nightOnlyMetrics"] = nightOnlyMetrics
        captured_args["metrics"] = metrics

        return dummy_response

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        mock_get_obs_status,
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 200

    assert captured_args == {
        "includeEntries": True,
        "includeIntervals": False,
        "nightOnlyMetrics": True,
        "metrics": None,
    }


def test_obs_status_single_metric(monkeypatch, dummy_response):
    endpoint = "/obs-status?dayObsStart=20250101&dayObsEnd=20250102&metric=fault_loss"

    captured_metrics = {}

    def mock_get_obs_status(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
        auth_token,
    ):
        captured_metrics["metrics"] = metrics
        return dummy_response

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        mock_get_obs_status,
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 200
    assert captured_metrics["metrics"] == ["fault_loss"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("false", False),
        ("1", True),
        ("0", False),
        ("yes", True),
        ("no", False),
        ("on", True),
        ("off", False),
    ],
)
def test_include_entries_boolean_parsing(
    monkeypatch,
    dummy_response,
    value,
    expected,
):
    endpoint = f"/obs-status?dayObsStart=20250101&dayObsEnd=20250102&includeEntries={value}"

    captured_value = {}

    def mock_get_obs_status(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
        auth_token,
    ):
        captured_value["includeEntries"] = includeEntries
        return dummy_response

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        mock_get_obs_status,
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 200
    assert captured_value["includeEntries"] is expected


def test_obs_status_empty_metric_list(monkeypatch, dummy_response):
    endpoint = "/obs-status?dayObsStart=20250101&dayObsEnd=20250102"

    captured_metrics = {}

    def mock_get_obs_status(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
        auth_token,
    ):
        captured_metrics["metrics"] = metrics
        return dummy_response

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        mock_get_obs_status,
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 200
    assert captured_metrics["metrics"] is None


@pytest.mark.parametrize(
    "missing_param",
    [
        "dayObsStart",
        "dayObsEnd",
    ],
)
def test_missing_required_params_return_422(monkeypatch, missing_param):
    params = {
        "dayObsStart": "20250101",
        "dayObsEnd": "20250102",
    }

    del params[missing_param]

    endpoint = "/obs-status"

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint, params=params)

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("param", "value"),
    [
        ("dayObsStart", "abc"),
        ("dayObsEnd", "abc"),
    ],
)
def test_invalid_integer_params_return_422(
    monkeypatch,
    param,
    value,
):
    params = {
        "dayObsStart": "20250101",
        "dayObsEnd": "20250102",
    }

    params[param] = value

    endpoint = "/obs-status"

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint, params=params)

    assert response.status_code == 422


def test_obs_status_internal_exception_returns_500(monkeypatch):
    endpoint = "/obs-status?dayObsStart=20250101&dayObsEnd=20250102"

    def raise_error(*args, **kwargs):
        raise Exception("failure")

    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.main.get_obs_status",
        raise_error,
    )

    app.dependency_overrides[rsp_auth] = lambda: "dummy-token"

    response = client.get(endpoint)

    assert response.status_code == 500
    assert response.json()["detail"] == "failure"
