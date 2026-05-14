import logging
from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial

import bokeh
import colorcet
import healpy as hp
import numpy as np
import pandas as pd
import uranography
from bokeh.models.ui.ui_element import UIElement
from rubin_nights import rubin_sim_addons as rn_sim
from rubin_scheduler.scheduler.utils import get_current_footprint
from rubin_sim.sim_archive import fetch_sim_stats_for_night
from schedview import band_column
from schedview.collect.visits import NIGHT_STACKERS
from schedview.compute.camera import LsstCameraFootprintPerimeter
from schedview.compute.maf import compute_hpix_metric_in_bands
from schedview.compute.visits import add_coords_tuple
from schedview.plot.visit_skymaps import VisitMapBuilder
from uranography.api import ArmillarySphere, Planisphere, make_zscale_linear_cmap

from lsst.utils.plotting import get_multiband_plot_colors

logger = logging.getLogger(__name__)


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


def my_map_visits_over_hpix(
    visits,
    conditions,
    map_hpix,
    plot=None,
    scale_limits=None,
    palette=colorcet.blues,
    map_class=Planisphere,
    prerender_hpix=True,
):
    """Plot visit locations over a healpix map.

    Parameters
    ----------
    visits : `pd.DataFrame`
        The table of visits to plot, with columns matching the opsim database
        definitions.
    conditions : `rubin_scheduler.scheduler.features.Conditions`
        An instance of a rubin_scheduler conditions object.
    map_hpix : `numpy.array`
        An array of healpix values
    plot : `bokeh.models.plots.Plot` or `None`
        The bokeh plot on which to make the plot. None creates a new plot.
        None by default.
    scale_limits : `list` of `float` or `None`
        The scale limits for the healpix values. If None, use zscale to set
        the scale.
    palette : `str`
        The bokeh palette to use for the healpix map.
    map_class : `Planisphere` or `Armillary`, optional
        The class of map to use.  Defaults to uranography.Planisphere.
    prerender_hpix : `bool`, optional
        Pre-render the healpix map? Defaults to `True`.

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map.
    """
    camera_perimeter = LsstCameraFootprintPerimeter()

    if plot is None:
        plot = bokeh.plotting.figure(
            width=256,
            height=256,
            match_aspect=True,
        )

    sphere_map = map_class(mjd=conditions.mjd, plot=plot)

    if scale_limits is None:
        try:
            good_values = map_hpix[~map_hpix.mask]
        except AttributeError:
            good_values = map_hpix

        cmap = make_zscale_linear_cmap(good_values, palette=palette)
    else:
        cmap = bokeh.transform.linear_cmap("value", palette, scale_limits[0], scale_limits[1])

    if prerender_hpix:
        # Convert the healpix map into an image raster, and send that instead
        # the full healpix map (sent as one polygon for each healpixel).
        # An high nside, this should reduce the data sent to the browser.
        # However, it will not be responsive to controls.
        if not map_class == Planisphere:
            raise NotImplementedError()
        if not plot.width == plot.height:
            raise NotImplementedError()

        xsize = plot.width
        ysize = plot.height
        # For Lambert Azimuthal Equal Area, projection space is 4 radians wide
        # and high, so projection units per pixel is 4 radians/xsize.
        # reso is in units of arcmin, though.
        reso = 60 * np.degrees(4.0 / xsize)
        projector = hp.projector.AzimuthalProj(
            rot=sphere_map.laea_rot, xsize=xsize, ysize=ysize, reso=reso, lamb=True
        )
        map_raster = projector.projmap(map_hpix, partial(hp.vec2pix, hp.npix2nside(len(map_hpix))))

        # Set area outside projection to nan, not -inf, so bokeh does not
        # try coloring it.
        map_raster[np.isneginf(map_raster)] = np.nan

        reso_radians = np.radians(projector.arrayinfo["reso"] / 60)
        width_hpxy = reso_radians * map_raster.shape[0]
        height_hpxy = reso_radians * map_raster.shape[1]
        sphere_map.plot.image(
            [map_raster],
            x=-width_hpxy / 2,
            y=-height_hpxy / 2,
            dw=width_hpxy,
            dh=height_hpxy,
            color_mapper=cmap.transform,
            level="image",
        )
    else:
        sphere_map.add_healpix(map_hpix, nside=hp.npix2nside(len(map_hpix)), cmap=cmap)

    if len(visits) > 0:
        ras, decls = camera_perimeter(visits.fieldRA, visits.fieldDec, visits.rotSkyPos)

        perimeter_df = pd.DataFrame(
            {
                "ra": ras,
                "decl": decls,
            }
        )
        sphere_map.add_patches(
            perimeter_df, patches_kwargs={"fill_color": None, "line_color": "black", "line_width": 1}
        )

    sphere_map.decorate()

    sphere_map.add_marker(
        ra=np.degrees(conditions.sun_ra),
        decl=np.degrees(conditions.sun_dec),
        name="Sun",
        glyph_size=8,
        circle_kwargs={"color": "yellow", "fill_alpha": 1},
    )

    sphere_map.add_marker(
        ra=np.degrees(conditions.moon_ra),
        decl=np.degrees(conditions.moon_dec),
        name="Moon",
        glyph_size=8,
        circle_kwargs={"color": "orange", "fill_alpha": 0.8},
    )

    return plot


def my_create_hpix_visit_map_grid(hpix_maps, visits, conditions, **kwargs):
    """Create a grid of healpix maps with visits overplotted.

    Parameters
    ----------
    map_hpix : `numpy.array`
        An array of healpix values.
    visits : `pd.DataFrame`
        The table of visits to plot, with columns matching the opsim database
        definitions.
    conditions : `rubin_scheduler.scheduler.features.Conditions`
        An instance of a rubin_scheduler conditions object.

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map.
    """
    visit_map = {}
    for band in hpix_maps:
        visit_map[band] = my_map_visits_over_hpix(
            visits.query(f"filter == '{band}'"), conditions, hpix_maps[band], **kwargs
        )
        visit_map[band].title = band

    # Convert the dictionary of maps into a list of lists,
    # corresponding to the rows of the grid.
    num_cols = 3
    map_lists = []
    for band_idx, band in enumerate(hpix_maps):
        if band_idx % num_cols == 0:
            map_lists.append([visit_map[band]])
        else:
            map_lists[-1].append(visit_map[band])

    map_grid = bokeh.layouts.gridplot(map_lists, sizing_mode="scale_both")

    return map_grid


def my_create_metric_visit_map_grid(
    metric, metric_visits, visits, observatory, nside=32, use_matplotlib=False, **kwargs
) -> UIElement | None:
    """Create a grid of maps of metric values with visits overplotted.

    Parameters
    ----------
    metric : `numpy.array`
        An array of healpix values.
    metric_visits : `pd.DataFrame`
        The visits to use to compute the metric.
    visits : `pd.DataFrame`
        The table of visits to plot, with columns matching the opsim database
        definitions.
    observatory : `ModelObservatory`
        The model observotary to use.
    nside : `int`
        The nside with which to compute the metric.
    use_matplotlib: `bool`
        Use matplotlib instead of bokeh? Defaults to `False`.

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map or `None` if no visits are provided.
    """

    if len(metric_visits):
        metric_hpix = compute_hpix_metric_in_bands(metric_visits, metric, nside=nside)
    else:
        metric_hpix = {b: np.zeros(hp.nside2npix(nside)) for b in visits[band_column(visits)].unique()}

    if len(visits):
        map_grid = my_create_hpix_visit_map_grid(
            metric_hpix, visits, observatory.return_conditions(), **kwargs
        )

        bokeh.io.show(map_grid)
        return map_grid

    return None


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
    profile_name = "applet" if applet_mode else "full"

    theme_config = THEMES[theme]
    profile_config = deepcopy(VISIT_MAP_PROFILES[profile_name])

    figure_kwargs = deepcopy(profile_config["figure_kwargs"])
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
    ## TODO: check related visitmap tickets to address them
    # in this PR, such as:
    # - DONE https://rubinobs.atlassian.net/browse/OSW-1468 Visit Maps applet:
    # Scale down depth and horizon line thickness as well as
    # sun and moon markers to match plot size
    # - DONE https://rubinobs.atlassian.net/browse/OSW-1465 Visit Maps:
    # Change tooltip format
    # - NOT_YET https://rubinobs.atlassian.net/browse/OSW-1976 Visit Maps:
    # provide a way to click on the the app on the front page
    # to go to the vist maps page
    # - DONE https://rubinobs.atlassian.net/browse/OSW-1464 Visit Maps: show
    # the exposures at the current time (not end of the night)
    # if the night is in progress
    # - DONE https://rubinobs.atlassian.net/browse/OSW-1463 Visit Maps: Format
    # the dayobs label as 2025-11-13

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
