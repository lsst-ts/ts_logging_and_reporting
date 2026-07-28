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

"""Service for the /multi-night-visit-maps endpoint.

The raw visit frame comes from `ConsdbVisitsAdapter`; augmentation and
the opsim conversion the Bokeh builder needs run here, on read.
"""

import copy
import functools
import logging

import pandas as pd
import rubin_nights.augment_visits as rn_aug
import uranography
from bokeh.embed import json_item
from bokeh.models.ui.ui_element import UIElement
from rubin_nights import rubin_sim_addons as rn_sim
from rubin_scheduler.scheduler.utils import get_current_footprint
from schedview.collect.visits import NIGHT_STACKERS
from schedview.compute.visits import add_coords_tuple
from schedview.plot.visit_skymaps import VisitMapBuilder
from uranography.api import ArmillarySphere, Planisphere

from lsst.ts.logging_and_reporting.adapters.consdb_visits import get_consdb_visits_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days
from lsst.utils.plotting import get_multiband_plot_colors

logger = logging.getLogger(__name__)

# Constants for interactive visit map rendering
THEMES = {
    "LIGHT": {
        "PLOT_BAND_COLORS": get_multiband_plot_colors(dark_background=False),
        "BACKGROUND_COLOR": "#FFFFFF",
        "HORIZON_COLOR": "#000000",
        "CONTROL_COLOR": "#18191d",
    },
    "DARK": {
        "PLOT_BAND_COLORS": get_multiband_plot_colors(dark_background=True),
        "BACKGROUND_COLOR": "#262626",
        "HORIZON_COLOR": "#E5E5E5",
        "CONTROL_COLOR": "#E5E5E5",
    },
}

VISIT_MAP_PROFILES = {
    "full": {
        "map_classes": [ArmillarySphere, Planisphere],
        "figure_kwargs": {
            "match_aspect": True,
        },
        "star_size": 15,
        "horizon_thickness": 5,
        "show_extra_controls": True,
    },
    "applet": {
        "map_classes": [Planisphere],
        "figure_kwargs": {
            "match_aspect": True,
            "width": 340,
            "height": 200,
        },
        "star_size": 10,
        "horizon_thickness": 3,
        "show_extra_controls": False,
    },
}


def _prepare_visit_maps_data(
    visits: pd.DataFrame,
):
    """Prepare visit data for plotting on visit maps.
    This includes converting from consdb columns to opsim columns,
    applying stackers, and adding coordinate tuples for plotting.

    Parameters
    ----------
    visits : `pd.DataFrame`
        DataFrame containing visit data in opsim format.

    Returns
    -------
    visits : `pd.DataFrame`
        Processed DataFrame ready for plotting on visit maps.
    """
    if visits.empty:
        logger.warning("No visits data provided.")
        return pd.DataFrame()

    # drop visits with no RA/Dec, since we can't plot them on the sky
    visits.dropna(subset=["s_ra"], inplace=True)
    opsdb = rn_sim.consdb_to_opsim(visits)
    opsdb_rec = opsdb.to_records()
    for stacker in NIGHT_STACKERS:
        opsdb_rec = stacker.run(opsdb_rec)
    visits = pd.DataFrame(opsdb_rec)
    visits = add_coords_tuple(visits)
    return visits


def _get_visit_map_config(
    *,
    theme: str = "DARK",
    applet_mode: bool = False,
) -> dict:
    """Get visit map rendering configuration.

    Parameters
    ----------
    theme : `str`, optional
        Theme to use for the visit map. Must be either ``DARK`` or ``LIGHT``.
        The default is ``DARK``.
    applet_mode : `bool`, optional
        If `True`, return configuration for applet mode, with simplified
        controls and a smaller layout. If `False`, return configuration for
        full mode. The default is `False`.

    Returns
    -------
    config : `dict`
        Configuration parameters for visit map rendering. The dictionary
        includes the following keys:

        map_classes : `list`
            Uranography map classes to render, such as ``ArmillarySphere`` and
            ``Planisphere``.
        figure_kwargs : `dict`
            Keyword arguments used when creating figures, such as size and
            background color.
        visit_fill_colors : `list`
            Colors used to fill visit patches, based on the selected theme.
        horizon_color : `str`
            Color used for the horizon line, based on the selected theme.
        star_size : `int` or `float`
            Size of the sun and moon markers.
        horizon_thickness : `int` or `float`
            Line width used for horizon lines.
        show_extra_controls : `bool`
            Whether to show additional controls, such as the zenith button and
            coordinate system selector.
        control_styles : `dict`
            Styles for interactive controls, such as sliders and buttons, with
            colors based on the selected theme.
    """
    profile_name = "applet" if applet_mode else "full"

    theme_config = THEMES[theme]
    profile_config = copy.deepcopy(VISIT_MAP_PROFILES[profile_name])

    figure_kwargs = copy.deepcopy(profile_config["figure_kwargs"])
    figure_kwargs["border_fill_color"] = theme_config["BACKGROUND_COLOR"]
    figure_kwargs["background_fill_color"] = theme_config["BACKGROUND_COLOR"]

    return {
        "map_classes": profile_config["map_classes"],
        "figure_kwargs": figure_kwargs,
        "visit_fill_colors": theme_config["PLOT_BAND_COLORS"],
        "horizon_color": theme_config["HORIZON_COLOR"],
        "star_size": profile_config["star_size"],
        "horizon_thickness": profile_config["horizon_thickness"],
        "show_extra_controls": profile_config["show_extra_controls"],
        "control_styles": {"width": None, "styles": {"color": theme_config["CONTROL_COLOR"]}},
    }


def build_visit_maps_using_builder(visits: pd.DataFrame, applet_mode=False, theme="DARK") -> UIElement | None:
    """Build interactive visit maps using the VisitMapBuilder
    class and uranography.

    Parameters
    ----------
    visits : `pd.DataFrame`
        DataFrame containing visit data in opsim format.
    applet_mode : `bool`, optional
        Whether to build the visit map in applet mode (with simplified
        controls and smaller size). Default is False (full mode).
    theme : `str`, optional
        Theme for the visit map, either `DARK` or `LIGHT`. Default is `DARK`.

    Returns
    -------
    viewable : `UIElement` or `None`
        A Bokeh UIElement containing the interactive visit map,
        or `None` if there were no valid visits to plot.
    """
    map_visits = _prepare_visit_maps_data(visits)
    if map_visits.empty:
        logger.warning("No valid visits to plot on visit maps.")
        return None

    nside = 64
    config = _get_visit_map_config(theme=theme, applet_mode=applet_mode)

    footprint_depth_by_band, footprint_regions = get_current_footprint(nside)

    # Set default slider and select styles for all spheremaps
    uranography.spheremap.SphereMap.default_slider_kwargs = config["control_styles"]
    uranography.spheremap.SphereMap.default_select_kwargs = config["control_styles"]

    tooltips = """
            <div style="padding:5px; font-size:12px; line-height:1.2">
                <div><strong>Observation ID:</strong> @observationId</div>
                <div><strong>Start Timestamp:</strong> @start_timestamp{%F %T} UTC</div>
                <div><strong>Band:</strong> @band</div>
                <div><strong>RA, Dec:</strong> @fieldRA{0.000}, @fieldDec{0.000}</div>
                <div><strong>Observation Reason:</strong> @observation_reason</div>
                <div><strong>Science Program:</strong> @science_program</div>
                <div><strong>Para Angle:</strong> @paraAngle°</div>
                <div><strong>azimulth, Altitude:</strong> @azimuth°, @altitude°</div>
            </div>
            """

    builder = (
        VisitMapBuilder(
            map_visits,
            mjd=map_visits["observationStartMJD"].max(),
            map_classes=config["map_classes"],
            visit_fill_colors=config["visit_fill_colors"],
            figure_kwargs=config["figure_kwargs"],
        )
        .add_footprint_outlines(footprint_regions, line_width=config["horizon_thickness"])
        .hide_horizon_sliders()
        .add_eq_sliders()
        .add_graticules()
        .add_ecliptic()
        .add_galactic_plane()
        .add_datetime_slider()
        .hide_mjd_slider()
        .add_visit_patches()
        .hide_future_and_other_night_visits()
        .highlight_recent_visits()
        .add_body("sun", size=config["star_size"], color="yellow", alpha=1.0)
        .add_body("moon", size=config["star_size"], color="orange", alpha=0.8)
        .add_horizon(color=config["horizon_color"], line_width=config["horizon_thickness"])
        .add_horizon(zd=70, color="red", line_width=config["horizon_thickness"])
        .add_hovertext(visit_tooltips=tooltips)
        .add_play_controls()
    )

    if config["show_extra_controls"]:
        (builder.add_zenith_button().add_coord_sys_selector())

    viewable = builder.build()
    return viewable


class VisitMapsService(Service):
    """Serves /multi-night-visit-maps: the interactive Bokeh visit map.

    The raw visit frame comes from `ConsdbVisitsAdapter`; augmentation and
    the opsim conversion the Bokeh builder needs run here, on read.
    """

    def handle_request(
        self, day_obs_start: int, day_obs_end: int, instrument: str, applet_mode: bool = False
    ) -> dict:
        """Build the interactive visit map for the range.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the frontend sends
            end + 1 day).
        instrument : `str`
            Instrument to query (e.g. ``LSSTCam``).
        applet_mode : `bool`, optional
            Render the simplified applet layout when `True`.
        """
        per_day = self.adapters["consdb"].fetch(
            instrument, day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        return self.collate_response(per_day, instrument=instrument, applet_mode=applet_mode)

    def collate_response(self, data: dict[int, list[dict]], *, instrument: str, applet_mode: bool) -> dict:
        rows = [record for dayobs in sorted(data) for record in data[dayobs]]
        visits = pd.DataFrame(rows)
        if visits.empty:
            return {"interactive": None}
        visits = rn_aug.augment_visits(visits, instrument=instrument.lower())
        figure = build_visit_maps_using_builder(visits, applet_mode=applet_mode)
        return {"interactive": json_item(figure) if figure is not None else None}


@functools.cache
def get_visit_maps_service() -> VisitMapsService:
    return VisitMapsService(adapters={"consdb": get_consdb_visits_adapter()})
