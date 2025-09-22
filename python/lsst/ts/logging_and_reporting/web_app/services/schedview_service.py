from collections import defaultdict

import numpy as np
import pandas as pd
from datetime import datetime
from astropy.time import Time
from rubin_scheduler.scheduler.model_observatory.model_observatory import ModelObservatory
from rubin_scheduler.scheduler.schedulers import CoreScheduler  # noqa F401

# Imported to help sphinx make the link
from rubin_scheduler.scheduler.utils import get_current_footprint
from uranography.api import ArmillarySphere, Planisphere

import schedview.compute.astro
from schedview import band_column
from schedview.collect import load_bright_stars
from schedview.compute.camera import LsstCameraFootprintPerimeter
from schedview.compute.footprint import find_healpix_area_polygons
from schedview.plot import PLOT_BAND_COLORS

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
    Plot visits across multiple nights on a
    single SphereMap with a night slider.

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
    import bokeh

    reference_conditions = conditions_list[0]
    # print(reference_conditions)
    spheremaps = [mc(mjd=reference_conditions.mjd) for mc in map_classes]

    if camera_perimeter == "LSST":
        camera_perimeter = LsstCameraFootprintPerimeter()

    # MJD slider for fading
    if "mjd" not in spheremaps[0].sliders:
        spheremaps[0].add_mjd_slider()
    spheremaps[0].sliders["mjd"].start = reference_conditions.sun_n12_setting
    spheremaps[0].sliders["mjd"].end = reference_conditions.sun_n12_rising
    for sm in spheremaps[1:]:
        sm.sliders["mjd"] = spheremaps[0].sliders["mjd"]

    # Footprint / outline
    if footprint_outline is not None:
        add_footprint_outlines_to_skymaps(
            footprint_outline, spheremaps, line_width=5, colormap=defaultdict(np.array(["gray"]).item)
        )
    if footprint is not None:
        add_footprint_to_skymaps(footprint, spheremaps)

    # Fading transforms
    past_future_js = """
        const result = new Array(xs.length)
        for (let i = 0; i < xs.length; i++) {
            if (mjd_slider.value >= xs[i]) { result[i] = past_value }
            else { result[i] = future_value }
        }
        return result
    """
    past_future_transform = bokeh.models.CustomJSTransform(
        args=dict(mjd_slider=spheremaps[0].sliders["mjd"], past_value=0.5, future_value=0.0),
        v_func=past_future_js,
    )

    recent_js = """
        const result = new Array(xs.length)
        for (let i = 0; i < xs.length; i++) {
            if (mjd_slider.value < xs[i]) { result[i] = 0 }
            else { result[i] = Math.max(0, max_value * (1 - (mjd_slider.value - xs[i]) / scale)) }
        }
        return result
    """
    recent_transform = bokeh.models.CustomJSTransform(
        args=dict(mjd_slider=spheremaps[0].sliders["mjd"], max_value=1.0, scale=fade_scale),
        v_func=recent_js,
    )

    # Group visits per night
    unique_nights = sorted(visits["day_obs"].unique())
    # print(unique_nights)
    night_renderers = []

    for night_idx, day_obs in enumerate(unique_nights):
        night_visits = visits[visits["day_obs"] == day_obs]
        band_renderers = []

        for band in "ugrizy":
            visit_columns = [
                "filter" if (c == "band" and "band" not in night_visits.columns) else c
                for c in VISIT_COLUMNS
            ]
            band_visits = night_visits.loc[night_visits
                                           [band_column(night_visits)] == band, visit_columns].copy()
            if band_visits.empty:
                continue

            ras, decls = camera_perimeter(band_visits.fieldRA, band_visits.fieldDec, band_visits.rotSkyPos)
            band_visits = band_visits.assign(ra=ras, decl=decls, mjd=band_visits.observationStartMJD.values)

            patches_kwargs = dict(
                fill_alpha=bokeh.transform.transform("mjd", past_future_transform),
                line_alpha=bokeh.transform.transform("mjd", recent_transform),
                line_color="#ff00ff",
                line_width=2,
                name=f"visit_patches_{night_idx}_{band}",
            )

            if hatch:
                patches_kwargs.update(
                    dict(
                        fill_alpha=0,
                        hatch_alpha=bokeh.transform.transform("mjd", past_future_transform),
                        hatch_color=PLOT_BAND_COLORS[band],
                        hatch_pattern=BAND_HATCH_PATTERNS[band],
                        hatch_scale=BAND_HATCH_SCALES[band],
                    )
                )
            else:
                patches_kwargs.update(dict(fill_color=PLOT_BAND_COLORS[band]))

            # Add patches (returns CDS)
            _ = spheremaps[0].add_patches(band_visits, patches_kwargs=patches_kwargs)

            # Retrieve the GlyphRenderer from the plot using the unique name
            renderer = spheremaps[0].plot.select({"name": patches_kwargs["name"]})[0]

            # propagate to other maps
            for sm in spheremaps[1:]:
                _ = sm.add_patches(data_source=renderer.data_source, patches_kwargs=patches_kwargs)
                # retrieve the renderer for this spheremap too
                renderer_other = sm.plot.select({"name": patches_kwargs["name"]})[0]
                renderer = renderer_other  # for hover tool

            # Add hover tool to each map using the renderer
            for sm in spheremaps:
                hover_tool = bokeh.models.HoverTool(
                    renderers=[sm.plot.select({"name": patches_kwargs["name"]})[0]],
                    tooltips=VISIT_TOOLTIPS,
                    formatters={"@start_timestamp": "datetime"},
                )
                sm.plot.add_tools(hover_tool)

            band_renderers.append(renderer)

        night_renderers.append(band_renderers)

    # Sun, Moon, horizon, stars
    spheremap = spheremaps[0]
    spheremap.add_marker(np.degrees(reference_conditions.sun_ra), np.degrees(reference_conditions.sun_dec),
                         name="Sun", glyph_size=15, circle_kwargs={"color":"yellow","fill_alpha":1})
    spheremap.add_marker(np.degrees(reference_conditions.moon_ra), np.degrees(reference_conditions.moon_dec),
                         name="Moon", glyph_size=15, circle_kwargs={"color":"orange","fill_alpha":0.8})

    if show_stars:
        star_data = load_bright_stars()[["name","ra","decl","Vmag"]]
        star_data["glyph_size"] = 15 - (15.0/3.5)*star_data["Vmag"]
        star_data = star_data.query("glyph_size>0")
        spheremap.add_stars(star_data, mag_limit_slider=False, star_kwargs={"color":"yellow"})

    spheremap.add_horizon()
    spheremap.add_horizon(zd=70, line_kwargs={"color":"red", "line_width":2})

    # # Night slider
    # slider = bokeh.models.Slider(start=0, end=len(unique_nights)-1,
    # value=0, step=1, title="Night")

    # night_mjds = [cond.mjd for cond in conditions_list]

    # callback_code = """
    # for (let i = 0; i < sources.length; i++){
    #     let band_renderers = sources[i];
    #     for (let j = 0; j < band_renderers.length; j++){
    #         band_renderers[j].visible = (i == slider.value)
    #     }
    # }
    # // update the mjd slider to the selected night's MJD
    # mjd_slider.value = night_mjds[slider.value]
    # """
    # slider.js_on_change(
    #     "value",
    #     bokeh.models.CustomJS(
    #         args=dict(
    #             sources=night_renderers,
    #             slider=slider,
    #             mjd_slider=spheremaps[0].sliders["mjd"],
    #             night_mjds=night_mjds,
    #         ),
    #         code=callback_code,
    #     )
    # )

    # # callback_code = """
    # # for (let i = 0; i < sources.length; i++){
    # #     let band_renderers = sources[i];
    # #     for (let j = 0; j < band_renderers.length; j++){
    # #         band_renderers[j].visible = (i == slider.value)
    # #     }
    # # }
    # # """
    # # slider.js_on_change("value",
    # # bokeh.models.CustomJS(args=dict(sources=night_renderers,
    # slider=slider),
    # # code=callback_code))

    # Night selector (dropdown instead of slider)
    day_obs_labels = [str(n) for n in unique_nights]
    selector = bokeh.models.Select(title="Night", value=day_obs_labels[0], options=day_obs_labels, width=300)

    mjd_starts = {str(day_obs): cond.sun_n12_setting for day_obs, cond in zip(unique_nights, conditions_list)}
    mjd_ends   = {str(day_obs): cond.sun_n12_rising  for day_obs, cond in zip(unique_nights, conditions_list)}

    callback_code = """
    const selected = selector.value;
    for (let i = 0; i < sources.length; i++) {
        const visible = (day_obs_list[i] === selected);
        let band_sources = sources[i];
        for (let j = 0; j < band_sources.length; j++) {
            band_sources[j].visible = visible;
        }
    }
    // update MJD slider bounds for the selected night
    if (mjd_starts[selected] !== undefined && mjd_ends[selected] !== undefined) {
        mjd_slider.start = mjd_starts[selected];
        mjd_slider.end = mjd_ends[selected];
        mjd_slider.value = mjd_starts[selected];
    }
    """

    selector.js_on_change(
        "value",
        bokeh.models.CustomJS(
            args=dict(
                sources=night_renderers,
                selector=selector,
                day_obs_list=day_obs_labels,
                mjd_slider=spheremaps[0].sliders["mjd"],
                mjd_starts=mjd_starts,
                mjd_ends=mjd_ends,
            ),
            code=callback_code,
        ),
    )

    # Set initial visibility
    for i, band_renderers in enumerate(night_renderers):
        visible = (i == 0)
        for r in band_renderers:
            r.visible = visible

    # Decorate maps
    for sm in spheremaps:
        sm.decorate()

    # return bokeh.layouts.column(spheremap.figure, slider)
    return bokeh.layouts.column(bokeh.layouts.row([sm.figure for sm in spheremaps]), selector)


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
