import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError, BaseLogrepError
from lsst.ts.logging_and_reporting.utils import get_access_token, make_json_safe

from .services.jira_service import get_jira_tickets
from .services.consdb_service import (
    get_mock_exposures,
    get_exposures,
    get_data_log,
)
from .services.almanac_service import get_almanac
from .services.narrativelog_service import get_messages
from .services.exposurelog_service import get_exposure_flags, get_exposurelog_entries
from .services.nightreport_service import get_night_reports
from .services.rubin_nights_service import get_time_accounting, get_open_close_dome, get_context_feed
from .. import __version__


logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


app = FastAPI()

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
    auth_token: str = Depends(get_access_token),
):
    logger.info(f"Getting exposures for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        on_sky_exposures = [exp for exp in exposures if exp.get("can_see_sky")]
        total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
        total_on_sky_exposure_time = sum(exp["exp_time"] for exp in on_sky_exposures)

        open_dome_times = get_open_close_dome(dayObsStart, dayObsEnd, instrument, auth_token)
        open_dome_hours = 0
        if not open_dome_times.empty:
            open_dome_hours = open_dome_times["open_hours"].sum()

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
            "open_dome_hours": open_dome_hours,
        }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /exposures: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data-log")
async def read_data_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
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
async def read_jira_tickets(request: Request, dayObsStart: int, dayObsEnd: int, instrument: str):
    logger.info(
        f"Getting jira tickets for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument)
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
):
    try:
        (efd_and_messages, cols) = get_context_feed(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "data": efd_and_messages,
            "cols": cols,
        }
    except Exception as e:
        logger.error(f"Error in /context-feed: {e}")

@app.get("/visit-maps")
async def get_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
):
    logger.info(
        f"Getting visit maps for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
    )
    try:
        import os
        import time
        # import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        # from astropy.time import Time
        from schedview.compute.visits import add_coords_tuple
        from schedview.collect.visits import read_visits, NIGHT_STACKERS
        from schedview.plot.visitmap import create_visit_skymaps
        from schedview.plot.survey import create_metric_visit_map_grid
        from rubin_scheduler.scheduler.model_observatory import ModelObservatory
        from rubin_sim import maf
        from bokeh.embed import json_item

        import io
        import base64

        def fig_to_base64(fig):
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)
            return base64.b64encode(buf.read()).decode("utf-8")
        # import rubin_nights.augment_visits as rn_aug
        # import rubin_nights.dayobs_utils as rn_dayobs

        # visits : `pandas.DataFrame` or `str`
        # If a `pandas.DataFrame`, it needs at least the following columns:

        # ``"fieldRA"``
        #     The visit R.A. in degrees (`float`).
        # ``"fieldDec"``
        #     The visit declination in degrees (`float`).
        # ``"observationStartMJD"``
        #     The visit start MJD (`float`).
        # ``"band"``
        #     The visit filter (`str`)


        # consdb = clients['consdb']
        os.environ["RUBIN_SIM_DATA_DIR"] = os.environ["RUBIN_DATA_PATH"]

        observatory = ModelObservatory(init_load_length=1)
        # # timezone = "Chile/Continental"

        # day_min = Time(
        # f"{rn_dayobs.day_obs_int_to_str(dayObsStart)}T12:00:00",
        # format='isot', scale='utc')
        # day_max = Time(
        # f"{rn_dayobs.day_obs_int_to_str(dayObsEnd)}T12:00:00",
        # format='isot', scale='utc')
        # visits = consdb.get_visits(instrument,
        # day_min, day_max, augment=False)
        # visits = rn_aug.augment_visits(visits,
        # "lsstcam", skip_rs_columns=True)
        # pandas.errors.UndefinedVariableError:
        # name 'observationStartMJD' is not defined

        dayobs_start_dt = datetime.strptime(str(dayObsStart), '%Y%m%d')
        # dayobs_end_dt = datetime.strptime(str(dayObsEnd), '%Y%m%d')
        start_time = time.perf_counter()
        visits = read_visits(dayobs_start_dt.date(),
                             instrument.lower(),
                             stackers = NIGHT_STACKERS,
                             num_nights=2)
        visits['filter'] = visits['band']
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"read_visits() executed in {elapsed_time:.6f} seconds")

        dayobs_visits = visits[visits['day_obs'] == dayObsStart]

        v_map = None
        s_map = None

        if len(dayobs_visits):
        #     coord_column = max(tuple(visits.columns).
        # index("s_ra"), tuple(visits.columns).index("s_dec")) + 1
        #     visits.insert(coord_column, "coords",
        # list(zip(visits["s_ra"], visits["s_dec"])))
            dayobs_visits = add_coords_tuple(dayobs_visits)

            start_time = time.perf_counter()
            # print(visits["day_obs"].unique())
            v_map, vmap_data = create_visit_skymaps(
                visits=dayobs_visits,
                night_date=datetime.strptime(str(dayObsStart), '%Y%m%d').date(),
                timezone="UTC",
                observatory=observatory,
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"create_visit_skymaps() executed in {elapsed_time:.6f} seconds")

            # conditions = observatory.return_conditions()

            start_time = time.perf_counter()

            previous_day_obs_dt = dayobs_start_dt - timedelta(days=1)
            previous_day_obs = previous_day_obs_dt.strftime('%Y%m%d')
            # print(previous_day_obs)
            previous_visits = visits[visits['day_obs'] == int(previous_day_obs)]
            # print(len(previous_visits))

            # previous_visits = read_visits(
            # previous_day_obs_dt.date(), instrument.lower(),
            # stackers = NIGHT_STACKERS, num_nights="2")
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"fetching previous night visits executed in {elapsed_time:.6f} seconds")

            if len(dayobs_visits) and len(previous_visits) \
                    and not np.all(np.isnan(dayobs_visits['fiveSigmaDepth'])) \
                    and not np.all(np.isnan(previous_visits['fiveSigmaDepth'])):
                start_time = time.perf_counter()
                # print(previous_visits.columns)
                # previous_visits['filter'] = previous_visits['band']
                # dayobs_visits['filter'] = dayobs_visits['band']
                # print(visits.columns)
                s_map = create_metric_visit_map_grid(
                    maf.CountMetric(col='fiveSigmaDepth', metric_name="Numbers of visits"),
                    previous_visits.loc[np.isfinite(previous_visits['fiveSigmaDepth']), :],
                    dayobs_visits.loc[np.isfinite(dayobs_visits['fiveSigmaDepth']), :],
                    observatory,
                    nside=32,
                    use_matplotlib=False
                )
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                print(f"create_metric_visit_map_grid() executed in {elapsed_time:.6f} seconds")

            return {
                "interactive": json_item(v_map) if v_map is not None else {},
                "static": json_item(s_map) if s_map is not None  else {}
                }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /visit-maps: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /visit-maps: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/new-visit-maps")
async def new_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
):
    logger.info(
        f"Getting visit maps for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
    )

    import os
    import time
    from datetime import datetime
    from schedview.collect.visits import read_visits, NIGHT_STACKERS
    from .services.schedview_service import query_visits_between_twilights


    os.environ["RUBIN_SIM_DATA_DIR"] = os.environ["RUBIN_DATA_PATH"]

    almanac_info = get_almanac(dayObsStart, dayObsEnd)

    dayobs_start_dt = datetime.strptime(str(dayObsStart), '%Y%m%d')
    dayobs_end_dt = datetime.strptime(str(dayObsEnd), '%Y%m%d')
    diff = dayobs_end_dt - dayobs_start_dt

    logger.debug(f"Number of nights: {diff.days}")

    start_time = time.perf_counter()
    visits = read_visits(
        dayobs_start_dt.date(),
        instrument.lower(),
        stackers = NIGHT_STACKERS,
        num_nights=diff.days)
    logger.debug(f"Number of visits: {len(visits)}")

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logger.debug(f"fetching visits executed in {elapsed_time:.6f} seconds")

    filtered_visits, first_twilight, last_twilight = query_visits_between_twilights(visits, almanac_info)
    logger.debug(f"Number of visits between twilights: {len(filtered_visits)}")


@app.get("/multi-night-visit-maps")
async def multi_night_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
):
    try:
        import os
        import time
        from datetime import datetime
        from schedview.compute.visits import add_coords_tuple
        from schedview.collect.visits import read_visits, NIGHT_STACKERS
        from .services.schedview_service import create_visit_skymaps
        from rubin_scheduler.scheduler.model_observatory import ModelObservatory
        from bokeh.embed import json_item

        os.environ["RUBIN_SIM_DATA_DIR"] = os.environ["RUBIN_DATA_PATH"]

        observatory = ModelObservatory(init_load_length=1)
        # timezone = "Chile/Continental"

        dayobs_start_dt = datetime.strptime(str(dayObsStart), '%Y%m%d')
        dayobs_end_dt = datetime.strptime(str(dayObsEnd), '%Y%m%d')
        diff = dayobs_end_dt - dayobs_start_dt

        logger.debug(f"Number of nights: {diff.days}")
        start_time = time.perf_counter()

        visits = read_visits(
            dayobs_start_dt.date(),
            instrument.lower(),
            stackers = NIGHT_STACKERS, num_nights=diff.days)
        # visits['filter'] = visits['band']
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"read_visits() executed in {elapsed_time:.6f} seconds")
        print(visits["day_obs"].unique())

        v_map = None

        if len(visits):
            visits = add_coords_tuple(visits)
            print(visits["day_obs"].unique())

            start_time = time.perf_counter()
            v_map, vmap_data = create_visit_skymaps(
                visits=visits,
                timezone="UTC",
                observatory=observatory,
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"create_visit_skymaps() executed in {elapsed_time:.6f} seconds")

        return {
            "interactive": json_item(v_map) if v_map is not None else {},
            "static": {}
            }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /visit-maps: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /visit-maps: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
