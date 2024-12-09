import collections
import datetime as dt
import random

import lsst.ts.logging_and_reporting.utils as ut
import lsst.ts.logging_and_reporting.views as views
import numpy as np
import pandas as pd

"""\
Single Unified Time Log (SUTL="subtle").

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


def prefix_columns(df, prefix):
    return df.rename(columns={cname: prefix + cname for cname in df.columns})


#
# Both DFs must have DatetimeIndex
def merge_to_timelog(prefix, tl_df, right_df, right_dfield="Timestamp"):
    """Merge a source df onto timelog df.
    prefix:: prepend to all columns of right_df (for provenance)
    tl_df:: Time Log dataframe
    """
    right_df.sort_index()
    rdf = prefix_columns(right_df, prefix)

    # The left_on and right_on columns are expected to contain datetime column
    # in *_Time
    df = pd.merge_ordered(
        tl_df,
        rdf,
        on="Time",
        how="outer",
    )

    return df  # left_df for next merge


# reduce_period(compact(merge_sources(allsrc)))
def merge_sources(allsrc, verbose=False):
    """Result contains a row for every source record. Only one source per row.
    This means that there will be NaN values for all columns that are
    not native to a row.
    """
    srecs = allsrc.get_sources_time_logs()  # Source Records

    # Frame for Night (Unified Time Log)
    dates = pd.date_range(allsrc.min_date, allsrc.max_date, freq="4h")
    utl_df = pd.DataFrame(dates, index=dates, columns=["Time"])
    df = utl_df
    for srcname, srcdf in srecs.items():
        if verbose:
            print(f"DBG merge_sources: {srcname=}")
        df = merge_to_timelog(f"{srcname}_", df, srcdf)
        if verbose:
            print(f"DBG merge_sources: {srcname=} {df.shape=} {df.columns.to_list()=}")

    df.set_index(["Time"], inplace=True)

    if verbose:
        print(f"DBG merge_sources: Output {df.shape=}")
    return df


def sutl(allsrc, delta="2h", allow_data_loss=False, verbose=False):
    """Extract a single unified time log (SUTL) from records of sources."""

    fdf = merge_sources(allsrc)
    cdf = compact(fdf, delta=delta, allow_data_loss=allow_data_loss)
    if verbose:
        print(f"DBG sutl: {fdf.shape=}")
        print(f"DBG sutl: {cdf.shape=}")
    rdf = reduce_period(cdf)
    if verbose:
        print(f"DBG sutl: {rdf.shape=}")
    html = views.render_reduced_df(rdf)
    return html


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


def exposure_quality(v):
    if v == "good":
        return "+"
    elif v == "questionable":
        return "?"
    elif v == "junk":
        return "X"
    return "NA"


def sutl_style(styler):
    styler.format(exposure_quality)
    styler.format(precision=1)
    # styler.format_index(lambda v: v.strftime("%A"))
    # styler.background_gradient(axis=None, vmin=1, vmax=5, cmap="YlGnBu")
    return styler


def remove_list_columns(df):
    """Removes columns from a DataFrame that contain lists."""
    columns_to_drop = []
    for col in df.columns:
        if any(isinstance(x, list) for x in df[col]):
            columns_to_drop.append(col)
    return df.drop(columns_to_drop, axis=1), columns_to_drop


def render_df(df):
    return views.render_reduced_df(df)


# compact results of merge_all()
# Started out Lossless.
#   With allow_data_loss=True, remove useless/problematic columns.
#   Also, see: remove_list_columns()
# Cell Values that are lists cause problems in pd.drop_duplicates()
def compact(full_df, delta="4h", allow_data_loss=False, verbose=False):
    df = full_df.copy()
    if verbose:
        print(f"DBG compact: Input {df.shape=}")

    exclude_cols = [  # TODO  REMOVE, calc columns instead of list them
        "day_obs",
        "day_obs_EXP",
        "id",
        "id_EXP",
        "id_NAR",
        "obs_id",
        "tags",
        "urls",
        "observers_crew",
        "instrument",
        "seq_num",
        "cscs",
        "site_id_EXP",
        "site_id_NAR",
        "user_id_NAR",
        "user_agent_NAR",
        "is_human_NAR",
        "is_valid_NAR",
        "parent_id_NAR",
        "category",
        "level",
        "level_NAR",
        "user_id_EXP",
        "user_agent_EXP",
        "is_human",
        "is_valid_EXP",
    ]

    drop_cols = [
        cname
        for cname in df.columns
        if (cname.startswith("date_") or cname in exclude_cols)
    ]
    if allow_data_loss:
        if verbose:
            print(f"DBG compact: {sorted(drop_cols)=}\n\n")
        df.drop(drop_cols, axis=1, inplace=True)  # DATA LOSS
        if verbose:
            print(f"DBG compact: {sorted(df.columns)=}\n\n")

        # Remove columns >= 95% NaN
        val_count = int(0.05 * len(df))
        df.dropna(thresh=val_count, axis="columns", inplace=True)  # DATA LOSS

    df.reset_index(inplace=True)
    df["Period"] = df["Time"].apply(lambda x: x.floor(delta).hour)
    df.set_index(["Period", "Time"], inplace=True)
    # Remove Rows and Columnas that are all NaN.
    df.dropna(how="all", axis="index", inplace=True)
    df.dropna(how="all", axis="columns", inplace=True)

    # df = df.fillna('')
    # #!df = ut.wrap_dataframe_columns(df)  # TODO re-enable

    # Trim whitespace from all columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df, columns = remove_list_columns(df)  # DATA LOSS
    if verbose:
        print(f"WARNING removed {len(columns)} containing list values. {columns=}")
        print(f"DBG compact: Output {df.shape=}")

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


# Reduce results of compact()
# + Column specific width (and formatting in general)
# + ALERT in column 1: function of regular expression in messages
#   (e.g.: fail, error)
# + Truncate very log messages. End truncated messages with "(MORE...)"
# + Replace multiple rows in a period with a single row. And ...
# + In Period: Replace multi-values in a column with a conctenation
#   of the unique values.
# TODO General aggregation using dtypes assigned in allsrc.
def reduce_period(df, verbose=False):
    """Group and aggregate by Period. Drops some columns. Reduces Rows."""

    def multi_string(group):
        return "\n\n".join([str(x) for x in set(group) if not pd.isna(x)])

    def multi_label(group):
        return ", ".join([str(x) for x in set(group) if not pd.isna(x)])

    if verbose:
        print(f"DBG reduce_period: Input {df.shape=}")

    nuke_columns = {
        "NIG_id",  # ignore
        "NIG_day_obs",  # ignore
        "NIG_user_id",  # ignore
        "NIG_date_added",
        "NIG_date_sent",  # ignore
        "NIG_parent_id",
        "NIG_Timestamp",  # ignore
        "NAR_id",  # ignore
        "NAR_date_begin",
        "NAR_user_id",  # ignore
        "NAR_date_added",
        "NAR_parent_id",
        "NAR_date_end",  # ignore
        "NAR_Timestamp",  # ignore
    }
    used_columns = set(df.columns.to_list()) - nuke_columns

    #  #! available = set(df.columns.to_list())
    #  #! available.discard('NIG_day_obs')
    #  #! available.discard('NIG_summary')
    #  #! id_fields = {c for c in df.columns.to_list()  if c.endswith('_id')}
    #  #! timestamp_fields = {c for c in df.columns.to_list()
    #  #!                     if c.endswith('_Timestamp')}
    #  #! date_fields = {c for c in df.columns.to_list() if '_date_' in c}
    #  #! message_fields = {c for c in df.columns.to_list() if 'message' in c}
    #  #! message_fields += 'NIG_telescope_status'
    #  #! message_fields += 'NIG_summary'
    #  #! available -= (id_fields | timestamp_fields | date_fields )
    #  #!
    #  #! facets = {c: set(df[c].unique()) for c in available
    #  #!           if 0 < len(df[c].unique()) < 10 }
    #  #!

    drop_columns = nuke_columns & used_columns
    dropped_df = df.drop(drop_columns, axis=1)
    df = dropped_df

    # We would rather not have these field names hardcoded!!!
    group_aggregator = dict()
    message_fields = {c for c in df.columns.to_list() if "message" in c}
    message_fields |= {"NIG_telescope_status"}
    message_fields |= {"NIG_summary"}
    group_aggregator.update({c: multi_string for c in message_fields})
    label_fields = [
        "NIG_site_id",
        "NIG_telescope",
        "NIG_confluence_url",
        "NIG_user_agent",
        "NIG_is_valid",
        "NAR_site_id",
        "NAR_level",
        "NAR_time_lost",
        "NAR_user_agent",
        "NAR_is_human",
        "NAR_is_valid",
        "NAR_category",
        "NAR_time_lost_type",
    ]
    group_aggregator.update({c: multi_label for c in label_fields})
    group_aggregator["NAR_time_lost"] = "sum"

    if verbose:
        print(f"DBG reduce_period: columns {df.columns.to_list()=}")
    agg_keys = set(group_aggregator.keys())
    use_agg = agg_keys & used_columns
    drop_agg = agg_keys - use_agg
    for col in drop_agg:
        del group_aggregator[col]

    if verbose:
        print(f"DBG {agg_keys=}")
        print(f"DBG {used_columns=}")
        print(f"DBG {use_agg=}")
        print(f"DBG {drop_agg=}")
        print(f"DBG final agg_keys={set(group_aggregator.keys())}")
    df = df.groupby(level="Period").agg(group_aggregator)
    if verbose:
        print(f"DBG reduce_period: Output {df.shape=}")
    return df


def field_distribution(df, available=None):
    if not available:
        available = set(df.columns)
    thresh = 0.10 * len(df)  # max density threshold
    facets = {
        c: set(df[c].unique()) - set([np.nan])
        for c in available
        if 0 < len(df[c].unique()) < thresh
    }
    return facets


def foo(df):

    # Create a dictionary to store the aggregated results
    result = {}
    dtypes = df.dtypes

    # Loop through unique dtypes and aggregate columns of that type
    for dtype in dtypes.unique():
        columns_of_dtype = dtypes[dtypes == dtype].index
        result[dtype] = df[columns_of_dtype].agg(
            ["mean", "sum"]
        )  # Replace with your desired aggregation functions

    # Create a final DataFrame
    result_df = pd.concat(result, axis=1)
    print(result_df)
