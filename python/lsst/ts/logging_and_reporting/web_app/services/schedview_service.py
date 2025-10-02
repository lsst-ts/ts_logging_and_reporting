from collections import defaultdict

import numpy as np
import pandas as pd
import colorcet
from functools import partial

from datetime import datetime
from astropy.time import Time
from rubin_scheduler.scheduler.model_observatory.model_observatory import ModelObservatory
from rubin_scheduler.scheduler.schedulers import CoreScheduler  # noqa F401

# Imported to help sphinx make the link
from rubin_scheduler.scheduler.utils import get_current_footprint
from uranography.api import ArmillarySphere, Planisphere, make_zscale_linear_cmap

import schedview.compute.astro
from schedview import band_column
from schedview.collect import load_bright_stars
from schedview.compute.camera import LsstCameraFootprintPerimeter
from schedview.compute.footprint import find_healpix_area_polygons
from schedview.plot import PLOT_BAND_COLORS
import bokeh
import healpy as hp
from schedview.compute.maf import compute_hpix_metric_in_bands
from bokeh.models.ui.ui_element import UIElement
from schedview.plot.footprint import add_footprint_outlines_to_skymaps, add_footprint_to_skymaps

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
    "@observationId: @start_timestamp{%F %T} UTC (mjd=@observationStartMJD{00000.0000}, "
    + "LST=@observationStartLST\u00b0): "
    + "@observation_reason (@science_program), "
    + "in @band at \u03b1,\u03b4=@fieldRA\u00b0,@fieldDec\u00b0; "
    + "q=@paraAngle\u00b0; a,A=@azimuth\u00b0,@altitude\u00b0"
)


VISIT_COLUMNS = [
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
):
    """
    Multi-night visit plots with shared MJD slider.

    Parameters
    ----------
    visits : pd.DataFrame
        Must contain 'day_obs', 'observationStartMJD',
        'fieldRA', 'fieldDec', 'band'
    footprint : np.array or None
        Healpix footprint
    conditions_list : list
        List of nightly Conditions objects, one per night to plot
    """
    reference_conditions = conditions_list[0]
    spheremaps = [mc(mjd=reference_conditions.mjd) for mc in map_classes]


    if camera_perimeter == "LSST":
        camera_perimeter = LsstCameraFootprintPerimeter()

    # Shared MJD slider
    if "mjd" not in spheremaps[0].sliders:
        spheremaps[0].add_mjd_slider()

    mjd_start = min(cond.sun_n12_setting for cond in conditions_list)
    mjd_end   = max(cond.sun_n12_rising  for cond in conditions_list)
    mjd_slider = spheremaps[0].sliders["mjd"]
    mjd_slider.start = mjd_start
    mjd_slider.end   = mjd_end
    mjd_slider.value = mjd_end if len(spheremaps) == 1 else mjd_start

    for sm in spheremaps[1:]:
        sm.sliders["mjd"] = mjd_slider

    # Add footprint
    if footprint_outline is not None:
        add_footprint_outlines_to_skymaps(
            footprint_outline, spheremaps, line_width=5, colormap=defaultdict(lambda: "gray")
        )
    if footprint is not None:
        add_footprint_to_skymaps(footprint, spheremaps)

    # JS transforms for fading
    past_future_js = """
        const result = new Array(xs.length);
        for (let i = 0; i < xs.length; i++) {
            result[i] = (mjd_slider.value >= xs[i]) ? past_value : future_value;
        }
        return result;
    """
    past_future_transform = bokeh.models.CustomJSTransform(
        args=dict(mjd_slider=mjd_slider, past_value=0.5, future_value=0.0),
        v_func=past_future_js,
    )

    recent_js = """
        const result = new Array(xs.length);
        for (let i = 0; i < xs.length; i++) {
            if (mjd_slider.value < xs[i]) result[i] = 0;
            else result[i] = Math.max(0, max_value * (1 - (mjd_slider.value - xs[i])/scale));
        }
        return result;
    """
    recent_transform = bokeh.models.CustomJSTransform(
        args=dict(mjd_slider=mjd_slider, max_value=1.0, scale=fade_scale),
        v_func=recent_js,
    )

    # Prepare renderers per night and band
    unique_nights = sorted(visits["day_obs"].unique())
    night_renderers = []

    for night_idx, day_obs in enumerate(unique_nights):
        night_visits = visits[visits["day_obs"] == day_obs]
        band_renderers = []

        for band in "ugrizy":
            band_visits = night_visits[night_visits[band_column(night_visits)] == band].copy()
            if band_visits.empty:
                continue

            ras, decls = camera_perimeter(band_visits.fieldRA, band_visits.fieldDec, band_visits.rotSkyPos)
            band_visits["ra"] = ras
            band_visits["decl"] = decls
            band_visits["mjd"] = band_visits.observationStartMJD.values

            patches_kwargs = dict(
                fill_alpha=bokeh.transform.transform("mjd", past_future_transform),
                line_alpha=bokeh.transform.transform("mjd", recent_transform),
                line_color="#ff00ff",
                line_width=2,
                name=f"visit_{night_idx}_{band}"
            )

            if hatch:
                patches_kwargs.update(
                    fill_alpha=0,
                    hatch_alpha=bokeh.transform.transform("mjd", past_future_transform),
                    hatch_color=PLOT_BAND_COLORS[band],
                    hatch_pattern=BAND_HATCH_PATTERNS[band],
                    hatch_scale=BAND_HATCH_SCALES[band],
                )
            else:
                patches_kwargs["fill_color"] = PLOT_BAND_COLORS[band]

            # Add patches (returns CDS)
            _ = spheremaps[0].add_patches(band_visits, patches_kwargs=patches_kwargs)
            renderer = spheremaps[0].plot.select({"name": patches_kwargs["name"]})[0]

            # propagate to other maps
            for sm in spheremaps[1:]:
                _ = sm.add_patches(data_source=renderer.data_source, patches_kwargs=patches_kwargs)
                renderer = sm.plot.select({"name": patches_kwargs["name"]})[0]

            # Add hover tools
            for sm in spheremaps:
                hover = bokeh.models.HoverTool(
                    renderers=[sm.plot.select({"name": patches_kwargs["name"]})[0]],
                    tooltips=VISIT_TOOLTIPS,
                    formatters={"@start_timestamp": "datetime"}
                )
                sm.plot.add_tools(hover)

            band_renderers.append(renderer)
        night_renderers.append(band_renderers)

    # Sun, Moon, stars, horizon
    for sm in spheremaps:
        sm.add_marker(np.degrees(reference_conditions.sun_ra), np.degrees(reference_conditions.sun_dec),
                      name="Sun", glyph_size=15, circle_kwargs={"color":"yellow","fill_alpha":1})
        sm.add_marker(np.degrees(reference_conditions.moon_ra), np.degrees(reference_conditions.moon_dec),
                      name="Moon", glyph_size=15, circle_kwargs={"color":"orange","fill_alpha":0.8})
        if show_stars:
            star_data = load_bright_stars()[["name","ra","decl","Vmag"]]
            star_data["glyph_size"] = 15 - (15.0/3.5)*star_data["Vmag"]
            star_data = star_data.query("glyph_size>0")
            sm.add_stars(star_data, mag_limit_slider=False, star_kwargs={"color":"yellow"})
        sm.add_horizon()
        sm.add_horizon(zd=70, line_kwargs={"color":"red","line_width":2})

    # Text label showing current night based on MJD
    # Initial value is first night
    if len(spheremaps) == 1:
        dayobs_label = bokeh.models.Div(text=f"Night: {unique_nights[-1]}", width=150)
    else:
        dayobs_label = bokeh.models.Div(text=f"Night: {unique_nights[0]}", width=150)

    # Callback to update label on slider change
    callback_code = """
    const mjd_val = mjd_slider.value;
    let current_day = day_obs_list[0];
    for (let i = 0; i < mjd_starts.length; i++){
        if (mjd_val >= mjd_starts[i] && mjd_val <= mjd_ends[i]){
            current_day = day_obs_list[i];
        }
    }
    day_label.text = "Night: " + current_day;
    """

    mjd_slider.js_on_change(
        "value",
        bokeh.models.CustomJS(
            args=dict(
                mjd_slider=mjd_slider,
                day_label=dayobs_label,
                day_obs_list=unique_nights,
                mjd_starts=[cond.sun_n12_setting for cond in conditions_list],
                mjd_ends=[cond.sun_n12_rising for cond in conditions_list],
            ),
            code=callback_code
        )
    )

    # force_refresh = bokeh.models.CustomJS(
    #     args=dict(plots=[sm.plot for sm in spheremaps]),
    #     code="""
    #     for (let i = 0; i < plots.length; i++) {
    #         plots[i].change.emit();
    #     }
    #     """
    # )
    # mjd_slider.js_on_change("value", force_refresh)

    update_alpha = bokeh.models.CustomJS(
        args=dict(sources=[r.data_source for night in night_renderers for r in night],
                mjd_slider=mjd_slider, scale=fade_scale),
        code="""
        const mjd = mjd_slider.value;
        for (let s = 0; s < sources.length; s++) {
            const data = sources[s].data;
            const mjds = data['mjd'];
            const fill = data['fill_alpha'];
            const line = data['line_alpha'];
            for (let i = 0; i < mjds.length; i++) {
                if (mjd >= mjds[i]) {
                    fill[i] = 0.5;
                    line[i] = Math.max(0, 1.0 * (1 - (mjd - mjds[i]) / scale));
                } else {
                    fill[i] = 0.0;
                    line[i] = 0.0;
                }
            }
            sources[s].change.emit();
        }
        """
    )
    mjd_slider.js_on_change("value", update_alpha)

    # Set initial visibility
    for i, band_renderers in enumerate(night_renderers):
        visible = (i == 0)
        for r in band_renderers:
            r.visible = visible

    # Decorate maps
    for sm in spheremaps:
        sm.decorate()

    # Layout: row of plots + MJD slider + dayobs label
    row_plots = bokeh.layouts.row([sm.figure for sm in spheremaps])
    if len(spheremaps) == 1:
        row_plots.width = 350
        row_plots.height = 250
        # return bokeh.layouts.column(row_plots, mjd_slider, dayobs_label)

    return bokeh.layouts.column(row_plots, dayobs_label)

def create_visit_skymaps(
    visits,
    nside=32,
    observatory=None,
    timezone="Chile/Continental",
    planisphere_only=False,
):
    """
    Prepare data for multi-night SphereMap plotting.
    Returns figure and data dict.
    """

    # if isinstance(visits, str):
    #     visits = read_opsim(visits)

    # Filter by day_obs
    # visits = visits.query(f"day_obs >=
    # {start_dayobs} and day_obs <= {end_dayobs}")

    # Prepare observatory and conditions per night
    if observatory is None:
        observatory = ModelObservatory(nside=nside, init_load_length=1)
        observatory.sky_model.load_length = 1

    unique_nights = sorted(visits["day_obs"].unique())
    # print(unique_nights)
    conditions_list = []
    for day_obs in unique_nights:
        night_date = datetime.strptime(str(day_obs), '%Y%m%d').date()
        night_events = schedview.compute.astro.night_events(night_date=night_date, site=observatory.location,
                                                            timezone=timezone)
        # start_time = Time(night_events.loc["sunset","UTC"])
        end_time = Time(night_events.loc["sunrise","UTC"])
        observatory.mjd = end_time.mjd
        conditions_list.append(observatory.return_conditions())

    # Footprint outline
    footprint_regions = get_current_footprint(nside)[1]
    footprint_regions[np.isin(footprint_regions, ["bulgy","lowdust"])] = "WFD"
    footprint_regions[np.isin(footprint_regions,
                              ["LMC_SMC","dusty_plane","euclid_overlap","nes","scp","virgo"])
                              ] = "other"
    footprint_outline = find_healpix_area_polygons(footprint_regions)
    tiny_loops = footprint_outline.groupby(["region","loop"]).count().query("RA<10").index
    footprint_outline = footprint_outline.drop(tiny_loops)

    data = {
        "visits": visits,
        "footprint": None,
        "footprint_outline": footprint_outline,
        "conditions_list": conditions_list,
    }

    # Call plotting function
    if planisphere_only:
        vmap = plot_visit_skymaps(map_classes=[Planisphere], **data)
    else:
        vmap = plot_visit_skymaps(**data)

    return vmap, data



def get_previous_dayobs(dayobs: int) -> int:
    from datetime import datetime, timedelta
    dayobs_dt = datetime.strptime(str(dayobs), '%Y%m%d')
    previous_dayobs_dt = dayobs_dt - timedelta(days=1)
    previous_dayobs = int(previous_dayobs_dt.strftime('%Y%m%d'))
    return previous_dayobs


def query_visits_between_twilights(visits, almanac):
    df_almanac = pd.DataFrame(almanac)

    df_almanac.rename(columns={'dayobs': 'day_obs'}, inplace=True)
    df_almanac['day_obs'] = df_almanac['day_obs'].apply(get_previous_dayobs)

    df_almanac["evening_twilight_mjd"] = Time(df_almanac["twilight_evening"].to_list()).mjd
    df_almanac["morning_twilight_mjd"] = Time(df_almanac["twilight_morning"].to_list()).mjd

    df_merged = visits.merge(df_almanac, on="day_obs", how="left")
    df_filtered = df_merged[
        (df_merged["observationStartMJD"] >= df_merged["evening_twilight_mjd"]) &
        (df_merged["observationStartMJD"] <= df_merged["morning_twilight_mjd"])
    ].copy()

    first_evening_twilight = df_almanac['evening_twilight_mjd'].min()
    last_morning_twilight = df_almanac['morning_twilight_mjd'].max()

    return df_filtered, first_evening_twilight, last_morning_twilight


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
    map_class : `class`, optional
        The class of map to use.  Defaults to uranography.Planisphere.
    prerender_hpix : `bool`, optional
        Pre-render the healpix map? Defaults to True

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map
    """
    camera_perimeter = LsstCameraFootprintPerimeter()

    if plot is None:
        plot = bokeh.plotting.figure(
            width=256, height=256,
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

    Notes
    -----
    Additional keyword args are passed to map_visits_over_hpix.

    Parameters
    ----------
    map_hpix : `numpy.array`
        An array of healpix values
    visits : `pd.DataFrame`
        The table of visits to plot, with columns matching the opsim database
        definitions.
    conditions : `rubin_scheduler.scheduler.features.Conditions`
        An instance of a rubin_scheduler conditions object.

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map
    """
    # from schedview.plot.survey import map_visits_over_hpix
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
        An array of healpix values
    metric_visits : `pd.DataFrame`
        The visits to use to compute the metric
    visits : `pd.DataFrame`
        The table of visits to plot, with columns matching the opsim database
        definitions.
    observatory : `ModelObservatory`
        The model observotary to use.
    nside : `int`
        The nside with which to compute the metric.
    use_matplotlib: `bool`
        Use matplotlib instead of bokeh? Defaults to False.

    Returns
    -------
    plot : `bokeh.models.plots.Plot`
        The plot with the map
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
    else:
        print("No visits")

    return None
