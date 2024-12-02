import collections
import datetime as dt
import random

import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd

"""\
Isolate management of multiple timelogs.

We define a timelog as a list of records (dicts) that contain a
date-time to be used for ordering.  The timelog contains any number of
additional abrbitrary columns.

We want to merge multiple timelogs that have different indices.  The
result is sorted (ascending) by date-time. It is forced to fit into a list of
pre-defined date-time bins. The bins for one 24 hour period will span the time
from noon (in Dome timezoon) to noon the next day.
The datetime of every timelog record will satisfy:
   lowBin <= timelog.datetime < highBin
That is, it will fit into a (closed,open) interval.

The intent is to allow merging of multiple timelogs by "joining" on the index.

* Input Requirements
- Input data from a source is a list of homogenous dicts (records).
- There must be at least one datetime column in the records.
- The datetime column must be give as an ISO (T) string (assumed UTC).

* "Design"
Part A:  Merge many timelog sources into one dataframe containing the union
of columns from all sources and all rows.  This would very likley contain
a lot of redundancy.

Part B: Compact the giant dataframe through various methods:
  - remove redundancy
  - round values
  - remove selected columns
  - filter out rows based upon selected values in selected columns

Possibly useful:
  - pandas.merge_asof      # merge by near instead of exact matching keys
  - pandas.merge_ordered   # merge with optional group-wise merge
  - pandas.merge
  - pandas.date_range      # for time framing
  - pandas.timedelta_range
  - pandas.DatetimeIndex

"""


# Resolution of seconds
def randdatetime(start):
    dayseconds = random.randrange(60 * 60 * 24)
    time = start + dt.timedelta(seconds=dayseconds)
    return str(time)[:16]


# Could use https://factoryboy.readthedocs.io/ but ...
#   + not current with it
#   + its best for ORM (we have dicts, where are VERY close)
#   + and there IS factory.alchemy.SQLAlchemyModelFactory
#   + probably overkill if tests are simple
#   + would be another dependencey
# FactoryBoy appropriate for backend testing though!
def gen_test_records(
    start,  # datetime
    source="A",
    num_recs=3,
    num_cols=3,  # max=5
    weights=[10, 15, 45, 50],  # not 'date'
    columns=["date"] + list("abcd"),
):
    def scol(column_name):  # Source Column name
        return f"{source}_{column_name}"

    def rndval(column_name):
        if column_name == "date":
            time = randdatetime(start)
            return time
        else:
            return random.choices(list("wxyz"), weights=weights, k=1)[0]

    cnames = columns[:num_cols]
    recs = [{scol(cname): rndval(cname) for cname in cnames} for idx in range(num_recs)]
    return recs


def gen_source_dataframes(start=None):
    if start is None:
        sd = dt.datetime.today().replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        assert isinstance(start, str), f"Bad type {start=!r}"
        sd = dt.datetime.fromisoformat(f"{start}T12:00")
    df_dict = dict(
        A=pd.DataFrame(gen_test_records(sd, source="A", num_recs=3, num_cols=3)),
        B=pd.DataFrame(gen_test_records(sd, source="B", num_recs=19, num_cols=2)),
        C=pd.DataFrame(gen_test_records(sd, source="C", num_recs=5, num_cols=2)),
    )
    return df_dict


def distribution(records):
    counter = collections.Counter()
    [counter.update(r.values()) for r in records]
    return dict(counter)


def gen_timelog_frame(dayobs, noon="12:00", freq="20min"):
    """Generate a DataFrame as frame for hold multiple timelogs.
    noon: Local clock noon expressed in UTC.
    """
    dtnoon = dt.time.fromisoformat(noon)
    idayobs = ut.dayobs_int(dayobs)
    start_date = ut.get_datetime_from_dayobs_str(dayobs, local_noon=dtnoon)
    end_date = ut.get_datetime_from_dayobs_str(idayobs + 1, local_noon=dtnoon)
    dr = pd.date_range(start=start_date, end=end_date, freq=freq)
    df = pd.DataFrame(data=list(dr), index=dr, columns=["time"])
    return df, dr


def merge_to_timelog(left_df, right_df, right_date, suffixes=("_x", "_y")):
    right_df["Time"] = right_df[right_date].apply(dt.datetime.fromisoformat)
    right_df.sort_values(by="Time", inplace=True)

    df = pd.merge_ordered(left_df, right_df, on="Time", how="outer", suffixes=suffixes)
    return df


# Want to be able to specify time-bin size.
# Then get multiple records in a bin (sorted be actual time)
# Non-sense?  Just means
# So use groupby?
# split-apply-combine: https://pandas.pydata.org/docs/user_guide/groupby.html
def merge_all(date="2024-12-01", freq="20min"):
    source_dfs = gen_source_dataframes(date)
    tl_df, dr = gen_timelog_frame(date, freq=freq)

    df = pd.DataFrame([dict(time=ut.get_datetime_from_dayobs_str(date))])
    for source, source_df in source_dfs.items():
        df = merge_to_timelog(df, source_df, f"{source}_date")

    return df


# compact results of merge_all()
# Lossless
# 21 => 99 with sources A,B, and C
def compact(full_df):
    df = full_df.copy()
    exclude_cols = [
        "id",
        "observers_crew",
        "tags",
        "urls",
    ]

    drop_cols = [
        cname
        for cname in df.columns
        if (
            cname.startswith("date_")
            or cname in exclude_cols
            or cname.startswith("is_")
            or cname.startswith("id_")
            or cname.endswith("_id")
            or cname.startswith("tags_")
            or cname.startswith("urls_")
            or cname.startswith("level")
            or cname.startswith("user_agent_")
            or cname.endswith("_invalidated")
            or "components" in cname
            or "message" in cname
            or "systems" in cname
            or "_id_" in cname
        )
    ]
    print(f"DBG compact: dropping {drop_cols=}\n\n")
    df.drop(drop_cols, axis=1, inplace=True)

    df["Period"] = df["Time"].apply(lambda x: x.floor("4h").hour)
    df.set_index(["Period", "Time"], inplace=True)
    df.dropna(how="all", inplace=True)
    return (
        df.reset_index()
        .set_index(["Time"])
        .drop_duplicates()
        .reset_index()
        .set_index(["Period", "Time"])
    )
    # return df


# Result could be compacted further:
# + In one Period allow row merging between any that have duplicates when NaN
#   is considered to be "don't care".


def merge_sources(allsrc):
    sources = allsrc.get_sources_time_logs()
    print(
        f"Loaded sources with record counts: {[len(s) for s in sources]} "
        "(nig, exp, nar)"
    )
    nig_df, exp_df, nar_df = sources
    df = pd.DataFrame([dict(Time=ut.get_datetime_from_dayobs_str(allsrc.min_dayobs))])
    if not nig_df.empty:
        df = merge_to_timelog(df, nig_df, "date_added", suffixes=(None, "_NIG"))
    if not exp_df.empty:
        df = merge_to_timelog(df, exp_df, "date_added", suffixes=(None, "_EXP"))
    if not nar_df.empty:
        df = merge_to_timelog(df, nar_df, "date_added", suffixes=(None, "_NAR"))
    return df
