import copy
import logging
from datetime import datetime, timedelta
from io import BytesIO

import healpy as hp
import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt
import pandas as pd
import rubin_sim.maf as maf
import uranography
from bokeh.models.ui.ui_element import UIElement
from matplotlib import cm
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator
from rubin_nights import rubin_sim_addons as rn_sim
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
ALPHA_BACKGROUND_CAP = 0.7
VMAX_PERCENTILE = 95
RENDER_DPI = 150
COLOR_FG = "white"
COLOR_BG = "black"
COLOR_DEC_LABEL = "black"  # projected onto the map face, so inverted


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
    """Get configuration parameters for visit map rendering
    based on theme and mode.
    Parameters
    ----------
    theme : `str`, optional
        Theme for the visit map, either "DARK" or "LIGHT".
        Default is "DARK".
    applet_mode : `bool`, optional
        Whether the visit map is being rendered in applet mode
        (with simplified controls and smaller size).
        Default is False (full mode).
    Returns
    -------
    config : `dict`
        Dictionary containing configuration parameters for
        visit map rendering, including:
        - "map_classes": List of uranography map classes to
        use (e.g., ArmillarySphere, Planisphere).
        - "figure_kwargs": Dictionary of keyword arguments
        for figure creation (e.g., size, background color).
        - "visit_fill_colors": List of colors for filling
        visit patches, based on the theme.
        - "horizon_color": Color for the horizon line, based on the theme.
        - "star_size": Size of the sun and moon markers,
        based on the profile.
        - "horizon_thickness": Line width for horizon lines,
        based on the profile.
        - "show_extra_controls": Whether to show extra controls
        like zenith
        button and coordinate system selector, based on the profile.
        - "control_styles": Dictionary of styles for interactive controls
        (e.g., sliders, buttons), with colors based on the theme.
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
        Theme for the visit map, either "DARK" or "LIGHT". Default is "DARK".
    Returns
    -------
    viewable : `UIElement` or `None`
        A Bokeh UIElement containing the interactive visit map,
        or None if there were no valid visits to plot.
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


# The following functions are used for generating the static visit map image.
def _get_footprint_background(nside: int = NSIDE) -> npt.NDArray:
    """Get the background alpha values for the footprint,
    where 0 means no coverage and 1 means full coverage.
    This is used to set the alpha values for the background of
    the visit map, so that areas with no coverage are fully
    transparent and areas with coverage are more opaque (up to a cap).
    Parameters
    ----------
    nside : `int`, optional
        The nside parameter for the healpix footprint. Default is NSIDE (32).
    Returns
    -------
    background : `np.ndarray`
        An array of alpha values for the footprint background.
    """
    fp, _ = get_current_footprint(nside=nside)
    bg = np.where(fp["r"] == 0, np.nan, fp["r"])
    return np.where(bg > 1, 1.0, bg)


def _add_dec_labels(ax) -> None:
    """Add declination labels to the visit map axes.
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
            color=COLOR_DEC_LABEL,
            fontsize=12,
            ha="left",
            va="center",
        )


def _render_healpy_laea(
    hp_array: npt.NDArray,
    alpha: npt.NDArray,
    vmax: float,
    vmin: float | None = None,
) -> Figure:
    """Render a healpy map in an Aitoff projection with a specified
    colormap and alpha values.
    Parameters
    ----------
    hp_array : `np.ndarray`
        The healpy map values to render (e.g., number of visits per pixel).
    alpha : `np.ndarray`
        The alpha values for each pixel, where 0 means fully
        transparent and 1 means fully opaque.
    vmax : `float`
        The maximum value for the colormap scaling (e.g., the
        value that corresponds to the maximum color intensity).
    vmin : `float` or `None`, optional
        The minimum value for the colormap scaling.
        If None, the colormap will be scaled from 0 to vmax. Default is None.
    Returns
    -------
    fig : `matplotlib.figure.Figure`
        The rendered healpy map figure.
    """
    cmap = copy.copy(cm.viridis)
    extend = "both" if vmin is not None else "max"

    fig = plt.figure(figsize=(8, 6))
    hp.azeqview(
        hp_array,
        alpha=alpha,
        rot=(0, -90, 0),
        lamb=True,
        reso=17.5,
        min=vmin,
        max=vmax,
        title="",
        cbar=False,
        cmap=cmap,
        fig=fig.number,
    )
    hp.graticule(dpar=30, dmer=30, color=COLOR_FG, lw=2)

    ax = plt.gca()
    _add_dec_labels(ax)

    for text in ax.texts:
        text.set_color(COLOR_FG)
        text.set_fontsize(12)
    ax.text(
        0.5,
        -0.03,
        "180°",
        transform=ax.transAxes,
        ha="center",
        va="top",
        color=COLOR_FG,
        fontsize=10,
        clip_on=False,
    )
    fig.patch.set_facecolor(COLOR_BG)

    im = ax.get_images()[0]
    cbar = plt.colorbar(
        im,
        shrink=0.45,
        aspect=30,
        pad=0.05,
        orientation="horizontal",
        extendrect=False,
        extend=extend,
    )
    cbar.locator = MaxNLocator(nbins=3)
    cbar.update_ticks()
    cbar.set_label("Number of visits", color=COLOR_FG, fontsize=16)
    cbar.outline.set_edgecolor(COLOR_FG)
    cbar.ax.xaxis.set_tick_params(labelsize=12, color=COLOR_FG)
    plt.setp(cbar.ax.get_xticklabels(), color=COLOR_FG, fontsize=12)

    return fig


def _compute_nvisits_map(map_data, nside: int = NSIDE) -> npt.NDArray:
    """Compute the number of visits per healpix pixel for the given map data.
    Parameters
    ----------
    map_data : `pandas.DataFrame`
        The map data containing the observations.
    nside : `int`, optional
        The healpix nside parameter. Default is NSIDE.
    Returns
    -------
    nvisits : `np.ndarray`
        An array of the number of visits per healpix pixel.
    """
    m_nvis = maf.CountMetric(col="obs_start_mjd", metric_name="Nvisits")
    slicer = maf.HealpixSlicer(
        nside=nside,
        lon_col="s_ra",
        lat_col="s_dec",
        rot_sky_pos_col_name="sky_rotation",
        verbose=False,
    )
    bundle = maf.MetricBundle(m_nvis, slicer, "")
    group = maf.MetricBundleGroup({"nvisits": bundle}, None, save_early=False)
    group.run_current("", map_data)
    return bundle.metric_values.filled(np.nan)


def build_static_visit_map(map_data) -> bytes:
    """Build a static visit map image as a PNG byte string
    for the given map data and observation date range.
    Parameters
    ----------
    map_data : `pandas.DataFrame`
        The map data containing the observations.
    Returns
    -------
    image_bytes : `bytes`
        The PNG image data as a byte string.
    """

    mval = _compute_nvisits_map(map_data)
    background = _get_footprint_background()

    alpha = np.where(np.isnan(background), 0.0, background)
    alpha = np.where(alpha > ALPHA_BACKGROUND_CAP, ALPHA_BACKGROUND_CAP, alpha)
    alpha = np.where(mval > 0, 1.0, alpha)
    vmax = np.nanpercentile(mval, VMAX_PERCENTILE)

    fig = _render_healpy_laea(mval, alpha=alpha, vmax=vmax)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=RENDER_DPI, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()
