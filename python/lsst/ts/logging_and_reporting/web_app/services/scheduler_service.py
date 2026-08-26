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

import copy
import logging
from datetime import datetime, timedelta
from io import BytesIO

import healpy as hp
import matplotlib.pyplot as plt
import pandas as pd
import rubin_sim.maf as maf
import uranography
from bokeh.models.ui.ui_element import UIElement
from rubin_nights import rubin_sim_addons as rn_sim
from rubin_nights.reference_values import SCIENCE_PROGRAMS
from rubin_scheduler.scheduler.utils import get_current_footprint
from rubin_sim.sim_archive import fetch_sim_stats_for_night
from schedview.collect.visits import NIGHT_STACKERS
from schedview.compute.visits import add_coords_tuple
from schedview.plot.visit_skymaps import VisitMapBuilder
from uranography.api import ArmillarySphere, Planisphere

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

# Constants for static visit map generation
NSIDE = 32
RENDER_DPI = 150

COLOR_FG = "#E5E5E5"
COLOR_BG = "black"

FONT_SIZE_BODY = 13
FONT_SIZE_LABELS = 12
FONT_SIZE_TICKS = 11
GRATICULE_SPACING_DEG = 30
GRATICULE_LINEWIDTH = 1.2


def get_expected_exposures(
    dayobs_start: int,
    dayobs_end: int,
) -> dict:
    """Retrieve the expected exposures for Simonyi for a specified range
    of observation nights.

    Parameters
    ----------
    dayobs_start : `int`
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayobs_end : `int`
        The ending observation day (as an integer, e.g., YYYYMMDD).

    Returns
    -------
    result : `dict`
        Result dictionary with key:
        ``"sum"``
            Sum of all expected exposures in the range (`int`).
    """

    logger.info(f"Getting expected exposures for dayobs_start: {dayobs_start}, dayobs_end: {dayobs_end}.")

    expected_exposures_list = []

    try:
        # Convert to datetime objects
        start_date = datetime.strptime(str(dayobs_start), "%Y%m%d")
        end_date = datetime.strptime(str(dayobs_end), "%Y%m%d")

        # Loop through range of dayobs
        current_date = start_date
        while current_date <= end_date:
            dayobs = int(current_date.strftime("%Y%m%d"))
            try:
                # Can only reach sims <60 days from current date
                expected_exposures = fetch_sim_stats_for_night(day_obs=dayobs, max_simulation_age=60)
                visits = expected_exposures.get("nominal_visits", 0)
                expected_exposures_list.append(visits)
                logger.info(f"dayobs {dayobs}: {visits} expected exposures")
            except Exception as e:
                logger.warning(f"Failed to fetch expected exposures for {dayobs}: {e}")
                raise

            current_date += timedelta(days=1)

        # Sum expected values together for one total over queried range
        sum_expected_exposures = sum(expected_exposures_list)
        logger.info(f"Sum of expected exposures in range: {sum_expected_exposures}")

        return {"sum": sum_expected_exposures}

    except Exception as e:
        logger.error(f"Error in getting expected exposures from rubin_sim: {e}", exc_info=True)
        raise


# The following functions are used for preparing the visit
# data and building the interactive visit map using uranography.
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
                <div><strong>Para Angle:</strong> @paraAngle\u00b0</div>
                <div><strong>azimulth, Altitude:</strong> @azimuth\u00b0, @altitude\u00b0</div>
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


#######################################################
## The following functions are used for generating the static visit map image.
def _add_dec_labels(ax) -> None:
    """Add declination labels to the healpy sky axes.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the declination labels to.
    """
    for dec_deg in (-60, -30, 0, 30, 60):
        hp.projtext(
            30,
            dec_deg,
            f"{dec_deg}°",
            lonlat=True,
            color=COLOR_FG,
            fontsize=FONT_SIZE_LABELS,
            ha="left",
            va="center",
        )


def _add_ra_labels(ax) -> None:
    """Add right ascension labels to the healpy sky axes.
    Labels are added every 60 degrees, starting at 0 and
    ending at 300.
    However, the graticules are drawn every 30 degrees,
    so there will be graticules without labels in between.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the right ascension labels to.
    """
    for ra_deg in (0, 60, 120, 180, 240, 300):
        hp.projtext(
            ra_deg,
            -8,
            f"{ra_deg}°",
            lonlat=True,
            color=COLOR_FG,
            fontsize=FONT_SIZE_LABELS,
            ha="center",
            va="top",
        )


def _add_graticules(ax) -> None:
    """Draw graticule grid lines and RA/Dec labels on the
    main healpy sky axis.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the graticules and labels to.
    """
    plt.sca(ax)
    hp.graticule(
        dpar=GRATICULE_SPACING_DEG,
        dmer=GRATICULE_SPACING_DEG,
        color=COLOR_FG,
        lw=GRATICULE_LINEWIDTH,
    )
    _add_dec_labels(ax)
    _add_ra_labels(ax)


def _style_text(fig) -> None:
    """Apply foreground colour to all text in the figure.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure containing the text objects to style.
    """
    for text in fig.findobj(match=plt.Text):
        text.set_color(COLOR_FG)


def _style_axes(fig, main_ax) -> None:
    """Apply dark theme styling to axes chrome and sky-axis outlines.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure containing the axes to style.
    main_ax : `matplotlib.axes.Axes`
        The primary sky map axes (the one containing the image).
    """
    for ax in fig.axes:
        ax.set_facecolor(COLOR_BG)
        ax.tick_params(colors=COLOR_FG)

        for spine in ax.spines.values():
            spine.set_color(COLOR_FG)

        if ax is main_ax:
            for line in ax.lines:
                line.set_color(COLOR_FG)
                line.set_linewidth(GRATICULE_LINEWIDTH)


def _style_figure(fig, main_ax) -> None:
    """Apply full dark theme to the figure.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure to style.
    main_ax : `matplotlib.axes.Axes`
        The primary sky map axes (the one containing the image).
    """
    fig.patch.set_facecolor(COLOR_BG)
    _style_text(fig)
    _style_axes(fig, main_ax)


def _compute_nvisits_bundle(map_data) -> maf.MetricBundle:
    """Run the Nvisits count metric over the provided visit data.

    Parameters
    ----------
    map_data : `array-like`
        Structured visit records containing ``ra``, ``dec``, ``sky_rotation``,
        and ``mjd`` columns.

    Returns
    -------
    bundle : `maf.MetricBundle`
        Executed metric bundle with populated metric values.
    """
    m_nvis = maf.CountMetric(col="obs_start_mjd", metric_name="Nvisits")
    slicer = maf.HealpixSlicer(
        nside=NSIDE,
        lon_col="s_ra",
        lat_col="s_dec",
        rot_sky_pos_col_name="sky_rotation",
    )
    bundle = maf.MetricBundle(
        m_nvis,
        slicer,
        "",
        plot_funcs=[maf.HealpixSkyMap()],
        plot_dict={
            "title": "",
            "percentile_clip": 98,
            "n_ticks": 7,
            "extend": "max",
            "cmap": "viridis",
            "bgcolor": COLOR_BG,
            "badcolor": COLOR_BG,
            "fontsize": FONT_SIZE_BODY,
            "labelsize": FONT_SIZE_TICKS,
        },
    )
    group = maf.MetricBundleGroup({"nvisits": bundle}, None, save_early=False)
    group.run_current("", map_data)
    return bundle


def build_static_visit_map(visits) -> bytes:
    """Build the primary static visit map.

    Parameters
    ----------
    visits : `pandas.DataFrame`
        Visit records used to generate the map.

    Returns
    -------
    png_bytes : `bytes`
        Static visit map image as PNG bytes.
    """

    map_data = visits[visits["science_program"].isin(SCIENCE_PROGRAMS)] if not visits.empty else visits

    if map_data.empty:
        logger.warning("No science visits available for static map generation")
        return None

    bundle = _compute_nvisits_bundle(map_data.to_records())

    plot = bundle.plot()
    fig = plt.figure(plot["SkyMap"])
    main_ax = next((ax for ax in fig.axes if ax.images), None)

    _style_figure(fig, main_ax)

    if main_ax is not None:
        _add_graticules(main_ax)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=RENDER_DPI, bbox_inches="tight", facecolor=COLOR_BG)
    plt.close(fig)
    return buf.getvalue()
