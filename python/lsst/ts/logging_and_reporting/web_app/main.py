import base64
import logging
import re
from datetime import datetime, timedelta
from typing import List

from bokeh.embed import json_item
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from rubin_scheduler.scheduler.model_observatory import ModelObservatory

from lsst.ts.logging_and_reporting.exceptions import BaseLogrepError, ConsdbQueryError
from lsst.ts.logging_and_reporting.utils import (
    build_block_response,
    get_access_token,
    get_jira_hostname,
    make_json_safe,
)

from .. import __version__
from .services.almanac_service import get_almanac
from .services.consdb_service import (
    get_data_log,
    get_exposures,
    get_mock_exposures,
)
from .services.exposurelog_service import get_exposure_flags, get_exposurelog_entries
from .services.jira_service import get_block_ticket_summaries, get_jira_tickets
from .services.narrativelog_service import get_messages
from .services.nightreport_service import get_night_reports
from .services.rubin_nights_service import (
    get_context_feed,
    get_open_close_dome,
    get_time_accounting,
    get_visits,
)
from .services.scheduler_service import build_visit_maps_using_builder, get_expected_exposures
from .services.zephyr_service import get_test_cases

# Auth dependencies (instantiated once for reuse and testing)
rsp_auth = get_access_token()
jira_auth = get_access_token("jira")
zephyr_auth = get_access_token("zephyr")

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


app = FastAPI(root_path="/nightlydigest/api", docs_url="/docs", openapi_url="/openapi.json", redoc_url=None)

origins = [
    "http://localhost:5173",  # Vite
    "http://127.0.0.1:5173",  # just in case
    "http://nightlydigest-nginx-service",  # Kubernetes service name
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Change to your React app origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


logger.info("Starting FastAPI app")


@app.get("/version")
@app.get("/version/")
async def get_version():
    """Get the current version of the package."""
    return JSONResponse(status_code=200, content={"version": __version__})


@app.get("/health")
@app.get("/health/")
async def health():
    """Health check endpoint.
    Used by kubernetes readiness and liveness probes.
    """
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/mock-exposures")
async def read_exposures_from_mock_data(request: Request, dayObsStart: int, dayObsEnd: int, instrument: str):
    logger.info("Getting exposures from mock data")
    exposures = get_mock_exposures(dayObsStart, dayObsEnd, instrument)
    return {"exposures": exposures}


@app.get("/exposures")
async def read_exposures(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(f"Getting exposures for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        on_sky_exposures = [exp for exp in exposures if exp.get("can_see_sky")]
        total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
        total_on_sky_exposure_time = sum(exp["exp_time"] for exp in on_sky_exposures)

        open_dome_times = get_open_close_dome(dayObsStart, dayObsEnd, instrument, auth_token)

        exposures_df = get_time_accounting(
            dayObsStart,
            dayObsEnd,
            instrument,
            exposures,
            auth_token,
        )

        if not exposures_df.empty:
            exposures_dict = exposures_df[
                [
                    "exposure_id",
                    "exposure_name",
                    "exp_time",
                    "img_type",
                    "observation_reason",
                    "science_program",
                    "target_name",
                    "can_see_sky",
                    "band",
                    "obs_start",
                    "physical_filter",
                    "day_obs",
                    "seq_num",
                    "obs_end",
                    "overhead",
                    "zero_point_median",
                    "visit_id",
                    "overhead",
                    "pixel_scale_median",
                    "psf_sigma_median",
                    "visit_gap",
                ]
            ].to_dict(orient="records")

            exposures_safe_dict = make_json_safe(exposures_dict)

            exposures = jsonable_encoder(exposures_safe_dict)

        return {
            "exposures": exposures,
            "exposures_count": len(exposures),
            "sum_exposure_time": total_exposure_time,
            "on_sky_exposures_count": len(on_sky_exposures),
            "total_on_sky_exposure_time": total_on_sky_exposure_time,
            "open_dome_times": make_json_safe(open_dome_times.to_dict(orient="records")),
        }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /exposures: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/expected-exposures")
async def read_expected_exposures(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
):
    logger.info(f"Getting expected exposures for start: {dayObsStart}, end: {dayObsEnd} ")
    try:
        expected_exposures = get_expected_exposures(
            dayObsStart,
            dayObsEnd,
        )

        return {"sum_exposures": expected_exposures["sum"]}

    except Exception as e:
        logger.error(f"Error in /expected-exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data-log")
async def read_data_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(f"Getting data log for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        records = get_data_log(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return jsonable_encoder({"data_log": records})

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /data-log: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /data-log: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jira-tickets")
async def read_jira_tickets(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(jira_auth),
    jira_hostname: str = Depends(get_jira_hostname),
):
    logger.info(
        f"Getting jira tickets for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument, auth_token, jira_hostname)
        return {"issues": tickets}
    except BaseLogrepError as ble:
        logger.error(f"Jira API error in /jira-tickets: {ble}")
        raise HTTPException(status_code=502, detail="Jira API query failed")
    except Exception as e:
        logger.error(f"Error in /jira-tickets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/almanac")
async def read_almanac(request: Request, dayObsStart: int, dayObsEnd: int):
    logger.info(f"Getting almanac for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    try:
        almanac_info = get_almanac(dayObsStart, dayObsEnd)
        return {"almanac_info": almanac_info}
    except Exception as e:
        logger.error(f"Error in /almanac: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/narrative-log")
async def read_narrative_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Narrative Log records for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        records = get_messages(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        time_lost_to_weather = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == "weather")
        time_lost_to_faults = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == "fault")
        return {
            "narrative_log": records,
            "time_lost_to_weather": time_lost_to_weather,
            "time_lost_to_faults": time_lost_to_faults,
        }
    except Exception as e:
        logger.error(f"Error in /narrative-log: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exposure-flags")
async def read_exposure_flags(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Exposure Log flags for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        flags = get_exposure_flags(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return {
            "exposure_flags": flags,
        }
    except Exception as e:
        logger.error(f"Error in /exposure-flags: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exposure-entries")
async def read_exposure_entries(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Exposure Log entries for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        entries = get_exposurelog_entries(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return {
            "exposure_entries": entries,
        }
    except Exception as e:
        logger.error(f"Error in /exposure-entries: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/night-reports")
async def read_nightreport(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    auth_token: str = Depends(rsp_auth),
):
    try:
        records = get_night_reports(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "reports": records,
        }
    except Exception as e:
        logger.error(f"Error in /night-reports: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/context-feed")
async def read_context_feed(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    auth_token: str = Depends(rsp_auth),
):
    try:
        (efd_and_messages, cols) = get_context_feed(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "data": efd_and_messages,
            "cols": cols,
        }
    except Exception as e:
        logger.error(f"Error in /context-feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _build_multi_night_visit_map(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    appletMode: bool,
    auth_token: str,
):
    """Run blocking map generation logic outside the event loop."""
    visits = get_visits(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
    v_map = None

    if len(visits):
        v_map = build_visit_maps_using_builder(
            visits=visits,
            applet_mode=appletMode,
        )

    return v_map


@app.get("/multi-night-visit-maps")
async def multi_night_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    appletMode: bool = False,
    auth_token: str = Depends(rsp_auth),
):
    """Generate multi-night visit maps using Bokeh.
    Parameters
    ----------
    request : `Request`
        FastAPI request object.
    dayObsStart : `int`
        Start date in YYYYMMDD format.
    dayObsEnd : `int`
        End date in YYYYMMDD format.
    instrument : `str`
        Instrument name (e.g., 'lsstCam', 'latiss', etc.).
    appletMode : `bool`, optional
        If True, generate maps suitable for applet display. Default is False.
    auth_token : `str`
        Authentication token (injected by FastAPI dependency).

    Returns
    -------
    `dict`
        A dictionary containing the Bokeh JSON item for the interactive map.
    """
    logger.info(
        f"Getting multi night visit maps for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument} in appletMode: {appletMode}, "
    )

    try:
        v_map = await run_in_threadpool(
            _build_multi_night_visit_map,
            dayObsStart,
            dayObsEnd,
            instrument,
            appletMode,
            auth_token,
        )
        return {
            "interactive": json_item(v_map) if v_map is not None else None,
        }

    except Exception as e:
        logger.error(f"Error in /multi-night-visit-maps: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/survey-progress-map")
async def survey_progress_map(
    request: Request,
    dayObs: int,
    instrument: str,
):
    """Generate a survey progress map for a given night using Bokeh.

    Parameters
    ----------
    request : `Request`
        FastAPI request object.
    dayObs : `int`
        Date in YYYYMMDD format.
    instrument : `str`
        Instrument name (e.g., 'lsstCam', 'latiss', etc.).

    Returns
    -------
    `dict`
        A dictionary containing the Bokeh JSON item for
        the static survey progress map.
    """
    logger.info(f"Getting survey progress map for night: {dayObs} and instrument: {instrument}")
    try:
        import time

        import numpy as np
        from rubin_sim import maf
        from schedview.collect.visits import NIGHT_STACKERS, read_visits
        from schedview.plot.survey import create_metric_visit_map_grid

        observatory = ModelObservatory(init_load_length=1)

        dayobs_dt = datetime.strptime(str(dayObs), "%Y%m%d")

        start_time = time.perf_counter()

        visits = read_visits(dayobs_dt.date(), instrument.lower(), stackers=NIGHT_STACKERS, num_nights=50)

        visits["filter"] = visits["band"]

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.debug(f"read_visits() executed in {elapsed_time:.6f} seconds")

        s_map = None

        if len(visits):
            start_time = time.perf_counter()

            dayobs_visits = visits[visits["day_obs"] == dayObs]

            previous_day_obs_dt = dayobs_dt - timedelta(days=1)
            previous_day_obs = previous_day_obs_dt.strftime("%Y%m%d")

            previous_visits = visits[visits["day_obs"] == int(previous_day_obs)]

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            logger.debug(f"fetching previous night visits executed in {elapsed_time:.6f} seconds")

            if (
                len(dayobs_visits)
                and len(previous_visits)
                and not np.all(np.isnan(dayobs_visits["fiveSigmaDepth"]))
                and not np.all(np.isnan(previous_visits["fiveSigmaDepth"]))
            ):
                start_time = time.perf_counter()
                s_map = create_metric_visit_map_grid(
                    maf.CountMetric(col="fiveSigmaDepth", metric_name="Numbers of visits"),
                    previous_visits.loc[np.isfinite(previous_visits["fiveSigmaDepth"]), :],
                    visits.loc[np.isfinite(visits["fiveSigmaDepth"]), :],
                    observatory,
                    nside=32,
                    use_matplotlib=False,
                )
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                logger.debug(f"create_metric_visit_map_grid() executed in {elapsed_time:.6f} seconds")

        return {"static": json_item(s_map) if s_map is not None else {}}
    except Exception as e:
        logger.error(f"Error in /survey-progress-map: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/block-details")
async def read_block_details(
    request: Request,
    keys: List[str] = Query(..., alias="key"),
    zephyr_token: str = Depends(zephyr_auth),
    jira_token: str = Depends(jira_auth),
    jira_hostname: str = Depends(get_jira_hostname),
):
    """Retrieve BLOCK details from Zephyr/Jira for a list of keys.

    Parameters
    ----------
    request : `fastapi.Request`
        FastAPI request object.
    key : `list` [`str`]
        List of BLOCK keys (e.g., ``BLOCK-704`` or
        ``BLOCK-T123_a``) provided as query parameters.
    zephyr_token : `str`
        Authentication token for Zephyr (injected by FastAPI dependency).
    jira_token : `str`
        Authentication token for Jira (injected by FastAPI dependency).
    jira_hostname : `str`
        Authentication hostname for Jira (injected by FastAPI dependency).

    Returns
    -------
    `dict`
        A dictionary containing a ``data`` field that maps each valid BLOCK key
        to its associated summary, URL, and source (e.g., Zephyr or Jira).

    Raises
    ------
    HTTPException
        Raised with status code 500 if an unexpected error occurs while
        retrieving BLOCK details.
    """
    logger.info(f"Getting BLOCK details from Zephyr/Jira for: {keys}")
    try:
        ZEPHYR_BLOCK_RE = re.compile(r"^BLOCK-T\d+(?:_[A-Za-z0-9]+)?$")
        JIRA_BLOCK_RE = re.compile(r"^BLOCK-\d+$")

        zephyr_keys = []
        jira_keys = []

        # Remove duplicates
        key = list(dict.fromkeys(keys))

        # Sort keys by data source
        for k in key:
            if ZEPHYR_BLOCK_RE.match(k):
                zephyr_keys.append(k)
            elif JIRA_BLOCK_RE.match(k):
                jira_keys.append(k)

        zephyr_blocks = {}
        jira_blocks = {}

        errors = {}

        # Get BLOCK descriptions from Zephyr
        if zephyr_keys:
            try:
                logger.info(f"Getting Test Case BLOCK details from Zephyr for {zephyr_keys}")
                zephyr_blocks = await get_test_cases(
                    zephyr_keys,
                    zephyr_token=zephyr_token,
                    jira_token=jira_token,
                )
            except Exception as e:
                logger.error(f"Zephyr error in /block-details: {e}", exc_info=True)
                errors["zephyr"] = str(e)

        # Get Test Case BLOCK descriptions from Jira
        if jira_keys:
            try:
                logger.info(f"Getting BLOCK ticket summaries from Jira for {jira_keys}")
                jira_blocks = get_block_ticket_summaries(
                    jira_keys,
                    jira_token=jira_token,
                    jira_hostname=jira_hostname,
                )
            except Exception as e:
                logger.error(f"Jira error in /block-details: {e}", exc_info=True)
                errors["jira"] = str(e)

        # If both failed → hard fail
        if "zephyr" in errors and "jira" in errors:
            raise HTTPException(status_code=500, detail="Both Zephyr and Jira requests failed.")

        # Flesh out response dict with source type and URL
        data = build_block_response(zephyr_blocks, jira_blocks)

        return {
            "data": data,
            "errors": errors,
        }

    # Catch double service failures
    except HTTPException:
        raise

    # Catch other errors
    except Exception as e:
        logger.error(f"Error in /block-details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _prepare_static_visit_map_data(visits):
    """Prepare visit records for static map plotting."""
    return visits.dropna(subset=["s_ra", "s_dec", "sky_rotation", "obs_start_mjd"], axis=0).to_records()


def _encode_png_payload(image_bytes):
    """Serialize PNG bytes into a JSON-safe payload."""
    if image_bytes is None:
        return None

    return {
        "mime_type": "image/png",
        "data": base64.b64encode(image_bytes).decode("ascii"),
    }


def _build_visit_map_png(map_data, dayObsStart, dayObsEnd) -> bytes:
    """Build the primary static visit map and return PNG bytes."""
    from io import BytesIO

    import matplotlib.pyplot as plt
    from rubin_sim import maf

    m_nvis = maf.CountMetric(col="obs_start_mjd", metric_name="Nvisits")
    slicer = maf.HealpixSlicer(nside=32, lon_col="s_ra", lat_col="s_dec", rot_sky_pos_col_name="sky_rotation")
    constraint = ""
    nvisits = maf.MetricBundle(
        m_nvis,
        slicer,
        constraint,
        plot_funcs=[maf.HealpixSkyMap()],
        plot_dict={
            "title": f"Visits {dayObsStart} to {dayObsEnd}",
            "percentile_clip": 98,
            "n_ticks": 7,
            "extend": "max",
            "cmap": "viridis",
            "bgcolor": "black",
            "badcolor": "#9A9A9A",
            "fontsize": 13,
            "labelsize": 11,
        },
    )
    bundle_group = maf.MetricBundleGroup({"nvisits": nvisits}, None, save_early=False)
    if len(map_data) > 0:
        logger.debug("Running metric bundle group for primary static visit map generation")
        bundle_group.run_current(constraint, map_data)

    plot = nvisits.plot()
    fignum = plot["SkyMap"]
    fig = plt.figure(fignum)

    fg = "#E5E5E5"
    bg = "black"

    fig.patch.set_facecolor(bg)

    for text in fig.findobj(match=plt.Text):
        if text.get_text() == f"Visits {dayObsStart} to {dayObsEnd}":
            text.set_fontsize(16)
        text.set_color(fg)

    for ax in fig.axes:
        ax.title.set_color(fg)
        ax.xaxis.label.set_color(fg)
        ax.yaxis.label.set_color(fg)
        ax.tick_params(colors=fg)

        for spine in ax.spines.values():
            spine.set_color(fg)

        for line in ax.lines:
            line.set_linewidth(1.2)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=bg)
    plt.close(fig)
    return buf.getvalue()


def _build_secondary_visit_map_png(map_data, dayObsStart, dayObsEnd) -> bytes:
    """Build the secondary static visit map
    (using healpy) and return PNG bytes.
    """
    import copy
    from io import BytesIO

    import healpy as hp
    import matplotlib.pyplot as plt
    import numpy as np
    import numpy.typing as npt
    import rubin_sim.maf as maf
    from matplotlib import cm
    from matplotlib.figure import Figure
    from matplotlib.ticker import MaxNLocator
    from rubin_scheduler.scheduler.utils import get_current_footprint

    def hp_laea(
        hp_array: np.ndarray,
        alpha: np.ndarray | None = None,
        label: str | None = None,
        vmin: float | None = None,
        vmax: float | None = None,
    ) -> Figure:
        cmap = copy.copy(cm.viridis)
        # cmap.set_under("black")
        # cmap.set_bad("black")

        fig = plt.figure(
            figsize=(8, 6),
        )  # facecolor="black")

        hp.azeqview(
            hp_array,
            alpha=alpha,
            rot=(0, -90, 0),
            lamb=True,
            reso=17.5,
            min=vmin,
            max=vmax,
            title=label,
            cbar=False,
            cmap=cmap,
            fig=fig.number,
        )

        hp.graticule(color="black")

        ax = plt.gca()
        fig.patch.set_facecolor("#9A9A9A")

        im = ax.get_images()[0]
        extend = "max" if vmin is None else "both"

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
        cbar.outline.set_edgecolor("black")
        cbar.ax.xaxis.set_tick_params(color="black")
        plt.setp(cbar.ax.get_xticklabels(), color="black")

        return fig

    def get_background(nside: int = 32) -> npt.NDArray:
        fp, labels = get_current_footprint(nside=nside)
        bg_fp = np.where(fp["r"] == 0, np.nan, fp["r"])
        bg_fp = np.where(bg_fp > 1, 1, bg_fp)
        return bg_fp
        # return np.where(fp["r"] > 0, 1.0, np.nan)

    def hp_laea2(
        hp_array: np.ndarray,
        alpha: np.ndarray | None = None,
        label: str | None = None,
        vmin: float | None = None,
        vmax: float | None = None,
    ) -> Figure:
        cmap = copy.copy(cm.viridis)

        fig = plt.figure(figsize=(8, 6))
        hp.azeqview(
            hp_array,
            alpha=alpha,
            rot=(0, -90, 0),
            lamb=True,
            reso=17.5,
            min=vmin,
            max=vmax,
            # title=label,
            title="",
            cbar=False,
            cmap=cmap,
            fig=fig.number,
        )

        # hp.graticule(dpar=30, dmer=30, color="black") # light grey background
        hp.graticule(dpar=30, dmer=30, color="white", lw=2)  # dark grey background

        # _draw_ra_180_line()
        _add_dec_labels()

        ax = plt.gca()
        ax.set_title(label or "", color="white", fontsize=16)
        for text in ax.texts:
            text.set_color("white")
            text.set_fontsize(12)
        ax.xaxis.label.set_color("white")
        ax.yaxis.label.set_color("white")

        ax.text(
            0.5,
            -0.03,
            "180°",
            transform=ax.transAxes,
            ha="center",
            va="top",
            # color="black", # light grey background
            color="white",  # dark grey background
            fontsize=10,
            clip_on=False,
        )

        # fig.patch.set_facecolor("#9A9A9A") # light grey background
        fig.patch.set_facecolor("black")  # dark grey background

        im = ax.get_images()[0]
        extend = "max" if vmin is None else "both"

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
        cbar.set_label("Number of visits", color="white", fontsize=12)

        # light grey background
        # cbar.outline.set_edgecolor("black")
        # cbar.ax.xaxis.set_tick_params(color="black")
        # cbar.set_label("Number of visits", color="black", fontsize=12)
        # plt.setp(cbar.ax.get_xticklabels(), color="black")

        # dark grey background
        cbar.outline.set_edgecolor("white")
        cbar.ax.xaxis.set_tick_params(labelsize=12, color="white")
        cbar.set_label("Number of visits", color="white", fontsize=12)
        plt.setp(cbar.ax.get_xticklabels(), color="white", fontsize=12)

        return fig

    # def _add_radec_labels() -> None:
    #     # RA labels around the outer edge. Healpy expects degrees here.
    #     for ra_deg in range(0, 360, 30):
    #         ra_hours = (ra_deg / 15.0) % 24
    #         hp.projtext(
    #             ra_deg,
    #             -2,
    #             f"{ra_hours:.0f}h",
    #             lonlat=True,
    #             color="black",
    #             fontsize=8,
    #             ha="center",
    #             va="center",
    #         )

    #     # Dec labels on one meridian so they do not clutter the map.
    #     for dec_deg in (-75, -60, -45, -30, -15):
    #         hp.projtext(
    #             8,
    #             dec_deg,
    #             f"{dec_deg}°",
    #             lonlat=True,
    #             color="black",
    #             fontsize=8,
    #             ha="left",
    #             va="center",
    #         )
    def _add_dec_labels() -> None:
        # Put labels on a single meridian, away from the rim, so the
        # projection does not expand its bounds.
        label_ra_deg = 30

        for dec_deg in (-60, -30, 0, 30, 60):
            hp.projtext(
                label_ra_deg,
                dec_deg,
                f"{dec_deg}°",
                lonlat=True,
                color="black",
                fontsize=12,
                ha="left",
                va="center",
            )

    def _draw_ra_180_line() -> None:
        dec_vals = np.linspace(-90, 90, 721)
        ra_vals = np.full_like(dec_vals, 180.0, dtype=float)
        hp.projplot(
            ra_vals,
            dec_vals,
            lonlat=True,
            color="black",
            linewidth=0.8,
            alpha=0.8,
        )

    nside = 32
    nvisits = {}
    m_nvis = maf.CountMetric(col="obs_start_mjd", metric_name="Nvisits")
    s = maf.HealpixSlicer(
        nside=nside, lon_col="s_ra", lat_col="s_dec", rot_sky_pos_col_name="sky_rotation", verbose=False
    )
    constraint = ""
    nvisits = maf.MetricBundle(
        m_nvis,
        s,
        constraint,
        plot_funcs=[maf.HealpixSkyMap()],
        plot_dict={
            "title": f"Visits {dayObsStart} to {dayObsEnd}",
            "percentile_clip": 98,
            "n_ticks": 7,
            "extend": "max",
        },
    )
    g = maf.MetricBundleGroup({"nvisits": nvisits}, None, save_early=False)
    g.run_current(constraint, map_data)
    background = get_background(nside)
    mval = nvisits.metric_values.filled(np.nan)
    alpha = np.where(np.isnan(background), 0, background)
    # alpha = np.where(alpha > 0.2, 0.2, alpha)
    # alpha = np.where(mval > 0, 1, alpha)
    alpha = np.where(np.isnan(background), 0.0, 1)
    alpha = np.where(mval > 0, 1.0, alpha)
    vmax = np.nanpercentile(mval, 95)
    fig = hp_laea2(mval, alpha=alpha, label=f"Visits {dayObsStart} to {dayObsEnd}", vmin=None, vmax=vmax)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _build_third_visit_map_png(map_data) -> bytes:
    """Build a third version of the static visit map and return PNG bytes."""
    # Placeholder for additional map generation logic if needed
    from io import BytesIO

    import matplotlib.pyplot as plt
    import rubin_sim.maf as maf
    import schedview
    from rubin_scheduler.scheduler.model_observatory import ModelObservatory
    from schedview.plot.survey_skyproj import map_metric_in_laea_and_mcbryde

    observatory = ModelObservatory(init_load_length=1)

    fig = map_metric_in_laea_and_mcbryde(
        map_data,
        maf.CountMetric(col="band"),
        schedview.plot.survey_skyproj.map_count_healpix,
        observatory,
        save_early=False,
        sqlite=False,
        cmap="cividis_r",
        horizons=None,
        num_colors=8,
    )

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def prep_data(visits):
    import pandas as pd
    from rubin_nights import rubin_sim_addons as rn_sim

    # from schedview.compute.visits import add_coords_tuple
    from schedview.collect.visits import NIGHT_STACKERS

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
    # visits = add_coords_tuple(visits)
    return visits


@app.get("/static-visit-map")
async def static_visit_map(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Generating static map for the visits between start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
    )

    try:
        import time

        logger.debug("Getting visits for static map generation")
        start_time = time.perf_counter()

        v = get_visits(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)

        # We log the time taken to retrieve and process the visits separately
        # from the time taken to generate the plot, since data retrieval
        # and processing can be a significant portion of the total time
        # and we want to track it independently of the plotting time.
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.debug(f"get_visits() executed in {elapsed_time:.6f} seconds")

        map_data = prep_data(v)
        logger.debug(
            f"Number of visits retrieved: {len(v)}, number of visits with valid coordinates: {len(map_data)}"
        )

        map_data2 = _prepare_static_visit_map_data(v)
        logger.debug(f"Map data prepared for plotting with {len(map_data2)} valid records")

        if len(v) == 0 or len(map_data) == 0 or len(map_data2) == 0:
            logger.warning("No valid visit data available for static map generation")
            return {
                "visit_map": None,
                "coverage_map": None,
            }

        logger.debug("Starting static map generation")

        plotting_start_time = time.perf_counter()
        visit_map_png = _build_visit_map_png(map_data2, dayObsStart, dayObsEnd)
        secondary_map_png = _build_secondary_visit_map_png(map_data2, dayObsStart, dayObsEnd)
        # third_map_png = _build_third_visit_map_png(map_data)

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        plotting_elapsed_time = end_time - plotting_start_time
        logger.debug(f"Plotting elapsed time: {plotting_elapsed_time:.6f} seconds")
        logger.debug(f"Static map generation executed in {elapsed_time:.6f} seconds")

        return {
            "visit_map": _encode_png_payload(visit_map_png),
            "coverage_map": _encode_png_payload(secondary_map_png),
        }

    except Exception as e:
        logger.error(f"Error in /static-visit-map: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
