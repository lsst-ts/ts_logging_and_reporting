import logging
import pandas as pd
import numpy as np
from astropy.time import Time

import rubin_nights.dayobs_utils as rn_dayobs
import rubin_nights.rubin_scheduler_addons as rn_sch
import rubin_nights.augment_visits as rn_aug
from rubin_nights.observatory_status import get_dome_open_close


logger = logging.getLogger(__name__)

def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    else:
        return obj


def read_time_accounting(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    exposures: list,
    efd_client = None,
):
    logger.info(
        f"Getting time accounting data for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )

    day_min = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsStart)}T12:00:00", format='isot', scale='utc')
    day_max = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsEnd)}T12:00:00", format='isot', scale='utc')
    dome_open = get_dome_open_close(day_min, day_max, efd_client)

    if len(exposures) == 0:
        return [], dome_open['open_hours'].sum()

    exposures_df = pd.DataFrame(exposures)
    visits = rn_aug.augment_visits(exposures_df, "lsstcam", skip_rs_columns=True)
    wait_before_slew = 1.45
    settle = 2.0
    visits, slewing = rn_sch.add_model_slew_times(
        visits, efd_client, model_settle=wait_before_slew + settle, dome_crawl=False)
    max_scatter = 6
    valid_overhead = np.min([np.where(np.isnan(visits.slew_model.values), 0, visits.slew_model.values)
                                + max_scatter, visits.visit_gap.values], axis=0)
    visits['overhead'] = valid_overhead

    visits = visits.replace([np.inf, -np.inf], np.nan)
    visits = visits.where(pd.notnull(visits), None)

    visits_dict = visits[["exposure_id", "exposure_name", "exp_time", "img_type",
              "observation_reason", "science_program", "target_name", "can_see_sky",
              "band", "obs_start", "physical_filter", "day_obs", "seq_num",
              "obs_end", "overhead", "zero_point_median", "visit_id", "overhead",
              "pixel_scale_median", "psf_sigma_median"]].to_dict(orient="records")

    final_dict = make_json_safe(visits_dict)
    return final_dict, dome_open['open_hours'].sum()
