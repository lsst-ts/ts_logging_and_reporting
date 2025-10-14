from collections import defaultdict

import logging
import numpy as np
import pandas as pd
import colorcet
from functools import partial

from datetime import datetime, timedelta
from astropy.time import Time
from rubin_scheduler.scheduler.model_observatory.model_observatory import ModelObservatory

from rubin_scheduler.scheduler.utils import get_current_footprint
from uranography.api import ArmillarySphere, Planisphere, make_zscale_linear_cmap

import schedview.compute.astro
from schedview import band_column
from schedview.collect import load_bright_stars
from schedview.compute.camera import LsstCameraFootprintPerimeter
from schedview.compute.footprint import find_healpix_area_polygons
from schedview.plot import PLOT_BAND_COLORS as LIGHT_BAND_COLORS
import bokeh
import healpy as hp
from schedview.compute.maf import compute_hpix_metric_in_bands
from bokeh.models.ui.ui_element import UIElement
from bokeh.plotting import figure
from schedview.plot.footprint import add_footprint_outlines_to_skymaps, add_footprint_to_skymaps

from rubin_sim.sim_archive import fetch_sim_stats_for_night


logger = logging.getLogger(__name__)

BAND_HATCH_PATTERNS = dict(
    u="dot",
    g="ring",
    r="horizontal_line",
    i="vertical_line",
    z="right_diagonal_line",
    y="left_diagonal_line",
)
BAND_HATCH_SCALES = dict(u=6, g=6, r=6, i=6, z=12, y=12)
VISIT_TOOLTIPS = (
    "@observationId: @start_timestamp{%F %T} UTC<br>"
    + "(mjd=@observationStartMJD{00000.0000}, "
    + "LST=@observationStartLST\u00b0): "
    + "@observation_reason (@science_program),<br>"
    + "in @band at \u03b1,\u03b4=@fieldRA\u00b0,@fieldDec\u00b0;<br>"
    + "q=@paraAngle\u00b0; a,A=@azimuth\u00b0,@altitude\u00b0"
)


VISIT_COLUMNS = [
    "day_obs",
    "observationId",
    "start_timestamp",
    "observationStartMJD",
    "observationStartLST",
    "band",
    "fieldRA",
    "fieldDec",
    "rotSkyPos",
    "paraAngle",
    "azimuth",
    "altitude",
    "observation_reason",
    "science_program",
]

NSIDE_LOW = 8

# Dark theme band colors
DARK_BAND_COLORS = {
    "u": "#3eb7ff",
    "g": "#30c39f",
    "r": "#ff7e00",
    "i": "#2af5ff",
    "z": "#a7f9c1",
    "y": "#fdc900",
}

THEMES = {
    "LIGHT": {
        "PLOT_BAND_COLORS": LIGHT_BAND_COLORS,
        "BACKGROUND_COLOR": "#FFFFFF",
        "HORIZON_COLOR": "#000000",
    },
    "DARK": {
        "PLOT_BAND_COLORS": DARK_BAND_COLORS,
        "BACKGROUND_COLOR": "#262626",
        "HORIZON_COLOR": "#E5E5E5",
    },
}


def _get_slider_callback_code():
    """Get JavaScript code for MJD slider callback.
    Adds night-reset behavior and manages renderer visibility.
    Returns
    -------
    callback_code : `str`
    """
    callback_code = """
    const mjd_val = mjd_slider.value;
    let current_night_idx = null;

    // Determine which night interval the current mjd is in
    for (let i = 0; i < mjd_starts.length; i++) {
        if (mjd_val >= mjd_starts[i] && mjd_val <= mjd_ends[i]) {
            current_night_idx = i;
            break;
        }
    }

    // Update renderer visibility for all nights
    for (let i = 0; i < night_patch_renderers.length; i++) {
        const renderers_for_night = night_patch_renderers[i];
        const is_active = (i === current_night_idx);

        for (let j = 0; j < renderers_for_night.length; j++) {
            renderers_for_night[j].visible = is_active;
        }
    }

    // If outside all nights, hide everything
    if (current_night_idx === null) {
        for (let i = 0; i < sources.length; i++) {
            const band_sources = sources[i];
            for (let j = 0; j < band_sources.length; j++) {
                const cds = band_sources[j];
                const data = cds.data;
                for (let k = 0; k < data['mjd'].length; k++) {
                    data['fill_alpha'][k] = 0.0;
                    data['line_alpha'][k] = 0.0;
                }
                cds.change.emit();
            }
        }

        day_label.text = "";
        _hide_all_celestial_markers(all_sun_markers, all_moon_markers);
        return;
    }

    // Otherwise, update visits only for the current night
    for (let i = 0; i < sources.length; i++) {
        const band_sources = sources[i];
        for (let j = 0; j < band_sources.length; j++) {
            const cds = band_sources[j];
            const data = cds.data;
            const is_current_night = (i === current_night_idx);

            for (let k = 0; k < data['mjd'].length; k++) {
                if (!is_current_night) {
                    data['fill_alpha'][k] = 0.0;
                    data['line_alpha'][k] = 0.0;
                    continue;
                }
                if (mjd_val >= data['mjd'][k]) {
                    data['fill_alpha'][k] = 0.5;
                    data['line_alpha'][k] = Math.max(
                        0,
                        1 - (mjd_val - data['mjd'][k]) / scale
                    );
                } else {
                    data['fill_alpha'][k] = 0.0;
                    data['line_alpha'][k] = 0.0;
                }
            }
            cds.change.emit();
        }
    }

    // Update sun/moon visibility for current night
    const current_day = day_obs_list[current_night_idx];
    const idx = current_night_idx;
    _update_celestial_markers(all_sun_markers, all_moon_markers, idx);
    day_label.text = "Night: " + current_day;


    // Helper functions
    function _hide_all_celestial_markers(sun_markers, moon_markers) {
        for (let s = 0; s < sun_markers.length; s++) {
            for (let m = 0; m < sun_markers[s].length; m++) {
                sun_markers[s][m].glyph.fill_alpha = 0.0;
                moon_markers[s][m].glyph.fill_alpha = 0.0;
                sun_markers[s][m].data_source.change.emit();
                moon_markers[s][m].data_source.change.emit();
            }
        }
    }

    function _update_celestial_markers(sun_markers, moon_markers, night_idx) {
        for (let s = 0; s < sun_markers.length; s++) {
            for (let m = 0; m < sun_markers[s].length; m++) {
                const active = (m === night_idx);
                sun_markers[s][m].glyph.fill_alpha = active ? 1.0 : 0.0;
                moon_markers[s][m].glyph.fill_alpha = active ? 0.8 : 0.0;
                sun_markers[s][m].data_source.change.emit();
                moon_markers[s][m].data_source.change.emit();
            }
        }
    }
    """
    return callback_code


def _initialize_visit_alphas(night_renderers, night_patch_renderers, mjd_value, conditions_list, fade_scale):
    """Initialize visit alpha values and renderer visibility
    for the current slider value.
    This runs server-side to set up the initial state.

    Parameters
    ----------
    night_renderers : list
        List of night renderers (ColumnDataSources)
    night_patch_renderers : list of list
        List of lists of patch renderers, grouped by night
    mjd_value : float
        Current MJD slider value
    conditions_list : list
        List of Conditions objects
    fade_scale : float
        Time scale for fading

    Returns
    -------
    current_night_idx : int or None
        Index of the current night, or None if outside all nights
    """
    # Determine which night the current mjd belongs to
    current_night_idx = None
    for i, cond in enumerate(conditions_list):
        if cond.sun_n12_setting <= mjd_value <= cond.sun_n12_rising:
            current_night_idx = i
            break

    # Set renderer visibility based on current night
    for night_idx, renderers_for_night in enumerate(night_patch_renderers):
        is_active = night_idx == current_night_idx
        for renderer in renderers_for_night:
            renderer.visible = is_active

    # Update alpha values for all nights
    for night_idx, band_sources in enumerate(night_renderers):
        for cds in band_sources:
            # Make a plain Python dict copy
            data = {k: list(v) for k, v in cds.data.items()}

            # Only show visits for the current night
            if night_idx != current_night_idx:
                data["fill_alpha"] = [0.0] * len(data["mjd"])
                data["line_alpha"] = [0.0] * len(data["mjd"])
            else:
                for k, mjd in enumerate(data["mjd"]):
                    if mjd <= mjd_value:
                        data["fill_alpha"][k] = 0.5
                        data["line_alpha"][k] = max(0, 1 - (mjd_value - data["mjd"][k]) / fade_scale)
                    else:
                        data["fill_alpha"][k] = 0.0
                        data["line_alpha"][k] = 0.0

            # Reassign to trigger update
            cds.data = data

    return current_night_idx


def _add_visit_patches(visits, unique_nights, spheremaps, camera_perimeter, hatch, theme="LIGHT"):
    """Add visit patches for each night and band.
    Returns list of lists of ColumnDataSources and
    list of all patch renderers grouped by night.

    Parameters
    ----------
    visits : `pd.DataFrame`
        Must contain 'day_obs', 'observationStartMJD',
        'fieldRA', 'fieldDec', 'band'.
    unique_nights : `list`
        List of unique nights in visits.
    spheremaps : `list`
        List of spheremap instances to add patches to.
    camera_perimeter : `callable`
        Camera footprint perimeter function.
    hatch : `bool`
        Use hatching patterns for bands instead of solid colors.
    theme : `str`
        Theme to use, either "LIGHT" or "DARK".

    Returns
    -------
    night_renderers : `list` of `list` of `ColumnDataSource`
        List of lists of ColumnDataSources, one list per night.
    night_patch_renderers : `list` of `list` of `GlyphRenderer`
        List of lists of patch renderers, grouped by night.
    """
    night_renderers = []
    night_patch_renderers = []

    for night_idx, day_obs in enumerate(unique_nights):
        night_visits = visits[visits["day_obs"] == day_obs]
        band_renderers = []
        night_renderers_list = []

        for band in "ugrizy":
            band_visits = night_visits[night_visits[band_column(night_visits)] == band].copy()

            if band_visits.empty:
                continue

            # Calculate camera footprint positions
            ras, decls = camera_perimeter(band_visits.fieldRA, band_visits.fieldDec, band_visits.rotSkyPos)
            band_visits["ra"] = ras
            band_visits["decl"] = decls
            band_visits["mjd"] = band_visits.observationStartMJD.values
            band_visits["fill_alpha"] = [0.0] * len(band_visits)
            band_visits["line_alpha"] = [0.0] * len(band_visits)

            # Setup patch styling
            patches_kwargs = dict(
                fill_alpha="fill_alpha",
                line_alpha="line_alpha",
                fill_color=None if hatch else THEMES[theme]["PLOT_BAND_COLORS"][band],
                line_color="#ff00ff",
                line_width=2,
                name=f"visit_{night_idx}_{band}",
            )

            if hatch:
                patches_kwargs.update(
                    hatch_alpha="fill_alpha",
                    hatch_color=THEMES[theme]["PLOT_BAND_COLORS"][band],
                    hatch_pattern=BAND_HATCH_PATTERNS[band],
                    hatch_scale=BAND_HATCH_SCALES[band],
                )

            # Add patches to all spheremaps
            cds = spheremaps[0].add_patches(band_visits, patches_kwargs=patches_kwargs)
            for sm in spheremaps[1:]:
                sm.add_patches(data_source=cds, patches_kwargs=patches_kwargs)

            # Collect all renderers for this band across all spheremaps
            for sm in spheremaps:
                renderers = [r for r in sm.plot.renderers if getattr(r, "data_source", None) == cds]
                night_renderers_list.extend(renderers)

            band_renderers.append(cds)

        night_renderers.append(band_renderers)
        night_patch_renderers.append(night_renderers_list)

    # Create separate hover tools for each night
    for night_idx, renderers_for_night in enumerate(night_patch_renderers):
        if not renderers_for_night:
            continue

        hover = bokeh.models.HoverTool(
            renderers=renderers_for_night,
            mode="mouse",
            tooltips="""
            <div style="padding:5px; font-size:12px; line-height:1.2">
                <div><strong>Observation ID:</strong> @observationId</div>
                <div><strong>Start Timestamp:
                    </strong>
                    <span data-column="start_timestamp" data-format="%F %T"> @start_timestamp</span>
                     UTC
                </div>
                <div><strong>Night:</strong> @day_obs</div>
                <div><strong>Band:</strong> @band</div>
                <div><strong>RA:</strong> @fieldRA{0.000}</div>
                <div><strong>Dec:</strong> @fieldDec{0.000}</div>
                <div><strong>Start MJD:</strong> @observationStartMJD{0.0000}</div>
                <div><strong>LST:</strong> @observationStartLST\u00b0</div>
                <div><strong>Observation Reason:</strong> @observation_reason</div>
                <div><strong>Science Program:</strong> @science_program</div>
                <div><strong>q:</strong> @paraAngle\u00b0</div>
                <div><strong>a, A:</strong> @azimuth\u00b0, @altitude\u00b0</div>
            </div>
            """,
            tags=[night_idx],  # Tag with night index
        )

        # Add hover tool to all spheremaps
        for sm in spheremaps:
            sm.plot.add_tools(hover)

    return night_renderers, night_patch_renderers


def _setup_slider_callback(
    mjd_slider,
    night_renderers,
    all_sun_markers,
    all_moon_markers,
    dayobs_label,
    unique_nights,
    conditions_list,
    fade_scale,
    night_patch_renderers,
):
    """Setup JavaScript callback for MJD slider interaction.

    Parameters
    ----------
    night_patch_renderers : `list` of `list` of `GlyphRenderer`
        List of lists of patch renderers, grouped by night
    """
    callback_code = _get_slider_callback_code()

    mjd_slider.js_on_change(
        "value",
        bokeh.models.CustomJS(
            args=dict(
                mjd_slider=mjd_slider,
                sources=night_renderers,
                day_label=dayobs_label,
                day_obs_list=unique_nights,
                mjd_starts=[cond.sun_n12_setting for cond in conditions_list],
                mjd_ends=[cond.sun_n12_rising for cond in conditions_list],
                scale=fade_scale,
                all_sun_markers=all_sun_markers,
                all_moon_markers=all_moon_markers,
                night_patch_renderers=night_patch_renderers,  # Add grouped renderers
            ),
            code=callback_code,
        ),
    )


def plot_visit_skymaps(
    visits,
    footprint,
    conditions_list,
    hatch=False,
    fade_scale=2.0 / (24 * 60),
    camera_perimeter="LSST",
    show_stars=False,
    map_classes=[ArmillarySphere, Planisphere],
    footprint_outline=None,
    applet_mode=False,
    theme="LIGHT",
):
    """Multi-night visit plots with shared MJD slider.
    This is a modified version of `schedview.plot.visitmap.plot_visit_skymaps`
    to support multi-night data with added support
    for light/dark themes and applet mode.

    Parameters
    ----------
    visits : `pd.DataFrame`
        Must contain 'day_obs', 'observationStartMJD',
        'fieldRA', 'fieldDec', 'band'.
    footprint : `np.array` or `None`
        Healpix footprint.
    conditions_list : `list`
        List of nightly Conditions objects, one per night to plot.
    hatch : `bool`
        Use hatching patterns for bands instead of solid colors.
    fade_scale : `float`
        Time scale for fading visit markers.
    camera_perimeter : `str` or `callable`
        Camera footprint perimeter function.
    show_stars : `bool`
        Show bright stars on the map.
    map_classes : `list`
        List of spheremap classes to instantiate.
    footprint_outline : `object` or `None`
        Footprint outline polygons.
    applet_mode : `bool`
        If `True`, uses compact fixed sizing for dashboard (380x220).
        If `False`, uses responsive full-size mode for both maps.
    theme : `str`
        Theme to use, either "LIGHT" or "DARK".

    Returns
    -------
    figure : `bokeh.models.plots.Plot`
        The plot with the map(s).
    """

    visits = visits[VISIT_COLUMNS]
    # Initialize spheremaps
    reference_conditions = conditions_list[0]

    if camera_perimeter == "LSST":
        camera_perimeter = LsstCameraFootprintPerimeter()

    # Configure figure sizing based on mode
    if applet_mode:
        fig = figure(width=340, height=220, match_aspect=True)
        fig.background_fill_color = THEMES[theme]["BACKGROUND_COLOR"]
        fig.border_fill_color = THEMES[theme]["BACKGROUND_COLOR"]
        spheremaps = [Planisphere(mjd=reference_conditions.mjd, plot=fig)]
    else:
        spheremaps = []
        for mc in map_classes:
            fig = figure(match_aspect=True)
            fig.background_fill_color = THEMES[theme]["BACKGROUND_COLOR"]
            fig.border_fill_color = THEMES[theme]["BACKGROUND_COLOR"]
            spheremaps.append(mc(mjd=reference_conditions.mjd, plot=fig))

    # Setup shared MJD slider
    if "mjd" not in spheremaps[0].sliders:
        spheremaps[0].add_mjd_slider()

    mjd_start = min(cond.sun_n12_setting for cond in conditions_list)
    mjd_end = max(cond.sun_n12_rising for cond in conditions_list)

    mjd_slider = spheremaps[0].sliders["mjd"]
    mjd_slider.start = mjd_start
    mjd_slider.end = mjd_end
    mjd_slider.value = mjd_end

    # style the sliders according to the theme
    for slider_key in spheremaps[0].sliders:
        spheremaps[0].sliders[slider_key].styles = {"color": THEMES[theme]["HORIZON_COLOR"]}

    # Share slider across all spheremaps
    for sm in spheremaps[1:]:
        sm.sliders["mjd"] = mjd_slider

    # Add footprints
    if footprint_outline is not None:
        add_footprint_outlines_to_skymaps(
            footprint_outline, spheremaps, line_width=5, colormap=defaultdict(lambda: "gray")
        )
    if footprint is not None:
        add_footprint_to_skymaps(footprint, spheremaps)

    # Add visit patches per night and band
    unique_nights = sorted(visits["day_obs"].unique())
    night_renderers, night_patch_renderers = _add_visit_patches(
        visits,
        unique_nights,
        spheremaps,
        camera_perimeter,
        hatch,
        theme=theme,
    )

    # Add sun, moon, stars, and horizon markers
    all_sun_markers, all_moon_markers = _add_celestial_objects(
        conditions_list,
        spheremaps,
        show_stars,
        theme=theme,
    )

    # Create night label and use theme colors
    dayobs_label = bokeh.models.Div(
        text=f"Night: {unique_nights[0]}",
        width=150,
        styles={"color": THEMES[theme]["HORIZON_COLOR"], "padding": "0px"},
    )

    # Setup JavaScript callback for slider interaction
    _setup_slider_callback(
        mjd_slider,
        night_renderers,
        all_sun_markers,
        all_moon_markers,
        dayobs_label,
        unique_nights,
        conditions_list,
        fade_scale,
        night_patch_renderers,
    )

    # Layout based on mode
    if applet_mode:
        # Applet mode: single map with slider and label below
        # Use column layout with all controls below the map
        fig = bokeh.layouts.column(
            spheremaps[0].figure,
            dayobs_label,
        )
    else:
        # Full mode: side-by-side maps with controls below
        # Maps in a row, controls naturally appear below each map
        if len(spheremaps) == 1:
            # Single map in full mode (planisphere only)
            fig = bokeh.layouts.column(
                spheremaps[0].figure,
                mjd_slider,
                dayobs_label,
            )
        else:
            # Multiple maps side by side
            # ArmillarySphere sliders will appear below its map automatically
            # We only add the shared MJD slider and label for the whole layout
            row_plots = bokeh.layouts.row([sm.figure for sm in spheremaps])
            fig = bokeh.layouts.column(
                row_plots,
                dayobs_label,
            )

    callback_code = _get_slider_callback_code()

    # Initialize visit alphas and renderer visibility for
    # the current slider value and get current night index
    current_night_idx = _initialize_visit_alphas(
        night_renderers,
        night_patch_renderers,  # Pass the renderers
        mjd_slider.value,
        conditions_list,
        fade_scale,
    )

    # Set the correct initial night label
    if current_night_idx is not None:
        initial_night = unique_nights[current_night_idx]
        dayobs_label.text = f"Night: {initial_night}"
    else:
        dayobs_label.text = ""

    # Trigger callback on document ready to show visits on initial render
    trigger_initial_callback = bokeh.models.CustomJS(
        args=dict(
            mjd_slider=mjd_slider,
            sources=night_renderers,
            day_label=dayobs_label,
            day_obs_list=unique_nights,
            mjd_starts=[cond.sun_n12_setting for cond in conditions_list],
            mjd_ends=[cond.sun_n12_rising for cond in conditions_list],
            scale=fade_scale,
            all_sun_markers=all_sun_markers,
            all_moon_markers=all_moon_markers,
            night_patch_renderers=night_patch_renderers,
        ),
        code="setTimeout(function() { " + callback_code + " }, 100);",
    )

    # Add to the first figure to trigger on document ready
    if hasattr(fig, "children") and len(fig.children) > 0:
        first_fig = fig.children[0]
    else:
        first_fig = fig

    if hasattr(first_fig, "js_on_event"):
        first_fig.js_on_event("document_ready", trigger_initial_callback)

    # Decorate maps
    for sm in spheremaps:
        sm.decorate()

    return fig


def _add_celestial_objects(conditions_list, spheremaps, show_stars, theme="LIGHT"):
    """Add sun, moon, stars, and horizon to spheremaps.
    Returns lists of lists of sun and moon renderers, one list per spheremap,
    each containing one renderer per night.

    Parameters
    ----------
    conditions_list : `list`
        List of nightly Conditions objects, one per night to plot.
    spheremaps : `list`
        List of spheremap instances to add markers to.
    show_stars : `bool`
        Show bright stars on the map.
    theme : `str`
        Theme to use, either "LIGHT" or "DARK".

    Returns
    -------
    all_sun_markers : `list` of `list` of `GlyphRenderer`
        List of lists of sun renderers, one list per spheremap,
        each containing one renderer per night.
    all_moon_markers : `list` of `list` of `GlyphRenderer`
        List of lists of moon renderers, one list per spheremap,
        each containing one renderer per night.
    """

    # Convert celestial coordinates to degrees
    sun_ras_deg = [np.degrees(c.sun_ra) for c in conditions_list]
    sun_decs_deg = [np.degrees(c.sun_dec) for c in conditions_list]
    moon_ras_deg = [np.degrees(c.moon_ra) for c in conditions_list]
    moon_decs_deg = [np.degrees(c.moon_dec) for c in conditions_list]

    all_sun_markers = []
    all_moon_markers = []

    for sm_idx, sm in enumerate(spheremaps):
        sun_markers = []
        moon_markers = []

        # Add sun and moon markers for each night
        for night_idx, (sun_ra, sun_dec, moon_ra, moon_dec) in enumerate(
            zip(sun_ras_deg, sun_decs_deg, moon_ras_deg, moon_decs_deg)
        ):
            # Add sun marker
            n_renderers_before = len(sm.plot.renderers)
            sm.add_marker(
                sun_ra,
                sun_dec,
                name=f"Sun_{sm_idx}_{night_idx}",
                glyph_size=15,
                circle_kwargs={
                    "color": "yellow",
                    "fill_alpha": 1.0 if night_idx == 0 else 0.0,
                    "line_alpha": 0.0,
                },
            )
            sun_markers.append(sm.plot.renderers[n_renderers_before])

            # Add moon marker
            n_renderers_before = len(sm.plot.renderers)
            sm.add_marker(
                moon_ra,
                moon_dec,
                name=f"Moon_{sm_idx}_{night_idx}",
                glyph_size=15,
                circle_kwargs={
                    "color": "orange",
                    "fill_alpha": 0.8 if night_idx == 0 else 0.0,
                    "line_alpha": 0.0,
                },
            )
            moon_markers.append(sm.plot.renderers[n_renderers_before])

        # Add stars (once per spheremap)
        if show_stars:
            star_data = load_bright_stars()[["name", "ra", "decl", "Vmag"]]
            star_data["glyph_size"] = 15 - (15.0 / 3.5) * star_data["Vmag"]
            star_data = star_data.query("glyph_size>0")
            sm.add_stars(star_data, mag_limit_slider=False, star_kwargs={"color": "yellow"})

        # Add horizon lines
        sm.add_horizon(line_kwargs={"line_width": 6, "color": THEMES[theme]["HORIZON_COLOR"]})
        sm.add_horizon(zd=70, line_kwargs={"color": "red", "line_width": 2})

        all_sun_markers.append(sun_markers)
        all_moon_markers.append(moon_markers)

    return all_sun_markers, all_moon_markers


def create_visit_skymaps(
    visits,
    nside=32,
    observatory=None,
    timezone="Chile/Continental",
    planisphere_only=False,
    applet_mode=False,
    theme="LIGHT",
):
    """Prepare data for multi-night SphereMap plotting.
    This is a modified version of
    `schedview.plot.visitmap.create_visit_skymaps`
    to support multi-night data with added support
    for light/dark themes and applet mode.

    Parameters
    ----------
    visits : `pd.DataFrame`
        Must contain 'day_obs', 'observationStartMJD',
        'fieldRA', 'fieldDec', 'band'.
    nside : `int`
        Healpix nside for footprint.
    observatory : `ModelObservatory` or `None`
        The model observotary to use. If None, a default will be created.
    timezone : `str`
        Timezone for night calculations. Default is Chile/Continental.
    planisphere_only : `bool`
        If `True`, only create planisphere map (no armillary sphere).
        Default is `False`.
    applet_mode : `bool`
        If `True`, uses compact fixed sizing for dashboard (380x220).
        If `False`, uses responsive full-size mode for both maps.
    theme : `str`
        Theme to use, either "LIGHT" or "DARK". Default is "LIGHT".

    Returns
    -------
    figure : `bokeh.models.plots.Plot`
        The plot with the map(s).
    data : `dict`
        The data used to create the plot(s).
    """

    # Prepare observatory and conditions per night
    if observatory is None:
        observatory = ModelObservatory(nside=nside, init_load_length=1)
        observatory.sky_model.load_length = 1

    unique_nights = sorted(visits["day_obs"].unique())

    conditions_list = []
    for day_obs in unique_nights:
        night_date = datetime.strptime(str(day_obs), "%Y%m%d").date()
        night_events = schedview.compute.astro.night_events(
            night_date=night_date, site=observatory.location, timezone=timezone
        )
        end_time = Time(night_events.loc["sunrise", "UTC"])
        observatory.mjd = end_time.mjd
        conditions_list.append(observatory.return_conditions())

    # Footprint outline
    footprint_regions = get_current_footprint(nside)[1]
    footprint_regions[np.isin(footprint_regions, ["bulgy", "lowdust"])] = "WFD"
    footprint_regions[
        np.isin(footprint_regions, ["LMC_SMC", "dusty_plane", "euclid_overlap", "nes", "scp", "virgo"])
    ] = "other"
    footprint_outline = find_healpix_area_polygons(footprint_regions)
    tiny_loops = footprint_outline.groupby(["region", "loop"]).count().query("RA<10").index
    footprint_outline = footprint_outline.drop(tiny_loops)

    data = {
        "visits": visits,
        "footprint": None,
        "footprint_outline": footprint_outline,
        "conditions_list": conditions_list,
        "applet_mode": applet_mode,
        "theme": theme,
    }

    # Call plotting function
    if applet_mode or planisphere_only:
        vmap = plot_visit_skymaps(map_classes=[Planisphere], **data)
    else:
        vmap = plot_visit_skymaps(map_classes=[ArmillarySphere, Planisphere], **data)

    return vmap, data


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
    dayObsStart: int,
    dayObsEnd: int,
    # auth_token: str = None,
) -> dict:
    """
    Retrieve the expected exposures for Simonyi for a specified range
    of observation nights.

    Parameters
    ----------
    dayObsStart : int
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayObsEnd : int
        The ending observation day (as an integer, e.g., YYYYMMDD).

    Returns
    -------
    result : dict
        "nightly": [int, ...],  # expected exposures per dayObs
        "sum": int              # sum of all expected exposures in the range
    """

    logger.info(
        f"Getting expected exposures for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd}."
    )

    expected_exposures_list = []

    try:
        # Convert to datetime objects
        start_date = datetime.strptime(str(dayObsStart), "%Y%m%d")
        end_date = datetime.strptime(str(dayObsEnd), "%Y%m%d")

        # Loop through range of dayobs
        current_date = start_date
        while current_date <= end_date:
            dayObs = int(current_date.strftime("%Y%m%d"))
            try:
                expected_exposures = fetch_sim_stats_for_night(dayObs)
                visits = expected_exposures.get("nominal_visits", 0)
                expected_exposures_list.append(visits)
                logger.info(f"DayObs {dayObs}: {visits} expected exposures")
            except Exception as e:
                logger.warning(f"Failed to fetch expected exposures for {dayObs}: {e}")
                expected_exposures_list.append(0)

            current_date += timedelta(days=1)

        # Sum expected values together for one total over queried range
        sum_expected_exposures = sum(expected_exposures_list)
        logger.info(f"Sum of expected exposures in range: {sum_expected_exposures}")

        return {
            "nightly": expected_exposures_list,
            "sum": sum_expected_exposures,
        }


    except Exception as e:
        logger.error(
            f"Error in getting expected exposures from rubin_sim: {e}",
            exc_info=True)
        return {
            "nightly": [],
            "sum": 0,
        }
