import logging
import traceback
from datetime import UTC, datetime, timedelta

from lsst.ts.logging_and_reporting.almanac import Almanac

logger = logging.getLogger(__name__)


def _as_utc_datetime(timestamp: str) -> datetime:
    """Parse an almanac timestamp string as a timezone-aware UTC datetime."""
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _compute_elapsed_twilight_hours(
    night_hours: float,
    twilight_evening_12deg: str,
    twilight_morning_12deg: str,
    now_utc: datetime | None = None,
) -> float:
    """Compute completed twilight hours for a night.

    Future nights contribute 0 hours, completed nights contribute the full
    ``night_hours``, and an in-progress night contributes the elapsed time
    since evening nautical twilight.
    """
    now_utc = now_utc or datetime.now(UTC)
    evening_twilight_utc = _as_utc_datetime(twilight_evening_12deg)
    morning_twilight_utc = _as_utc_datetime(twilight_morning_12deg)

    if now_utc <= evening_twilight_utc:
        return 0.0
    if now_utc >= morning_twilight_utc:
        return night_hours

    elapsed_hours = (now_utc - evening_twilight_utc).total_seconds() / 3600
    return min(elapsed_hours, night_hours)


def get_almanac(dayobs_start: int, dayobs_end: int) -> list:
    logger.info(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    try:
        # adding one day to the start and end dates as the Almanac adapter
        # considers only max_dayobs, which is exclused from the dayobs range
        start = datetime.strptime(str(dayobs_start), "%Y%m%d") + timedelta(days=1)
        end = datetime.strptime(str(dayobs_end), "%Y%m%d") + timedelta(days=1)
        almanac_info = []
        current = start
        while current < end:
            dayobs = int(current.strftime("%Y%m%d"))
            almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs)
            night_events = almanac.as_dict[0]
            twilight_evening_12deg = night_events["Evening Nautical Twilight"]
            twilight_morning_12deg = night_events["Morning Nautical Twilight"]
            almanac_info.append(
                {
                    "dayobs": dayobs,
                    "night_hours": almanac.night_hours,
                    "twilight_evening_18deg": night_events["Evening Astronomical Twilight"],
                    "twilight_morning_18deg": night_events["Morning Astronomical Twilight"],
                    "twilight_evening_12deg": twilight_evening_12deg,
                    "twilight_morning_12deg": twilight_morning_12deg,
                    "twilight_evening_6deg": night_events["Evening Civil Twilight"],
                    "twilight_morning_6deg": night_events["Morning Civil Twilight"],
                    "twilight_evening_0deg": night_events["Sun Set"],
                    "twilight_morning_0deg": night_events["Sun Rise"],
                    "moon_rise_time": night_events["Moon Rise"],
                    "moon_set_time": night_events["Moon Set"],
                    "moon_illumination": night_events["Moon Illumination"],
                    "elapsed_twilight_hours": _compute_elapsed_twilight_hours(
                        almanac.night_hours,
                        twilight_evening_12deg,
                        twilight_morning_12deg,
                    ),
                }
            )
            current += timedelta(days=1)
        return almanac_info
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error fetching almanac data for: {dayobs_start}, {dayobs_end}. Error: {e}")
        raise e
