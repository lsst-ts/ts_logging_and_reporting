import collections
import datetime as dt
import random

import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
from jinja2 import Template

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


# Both DFs must be indexed by DatetimeIndex
def merge_to_timelog(left_df, right_df, right_dfield, prefix="R_"):
    right_date = right_dfield
    # #! right_df["Time"] = right_df.reset_index()[right_date]
    # #!       .apply(dt.datetime.fromisoformat)
    # #! right_df.sort_values(by="Time", inplace=True)
    right_df.sort_index()
    rdf = prefix_columns(right_df, prefix)

    print(
        f"""DBG merge_to_timelog-2: {prefix=}
    {right_date=}
    {left_df.index=}
    {left_df.columns=}
    {rdf.index=}
    {rdf.columns=}
    """
    )
    # The left_on and right_on columns are expected to contain datetime column
    # in *_Time
    df = pd.merge_ordered(
        left_df, rdf, left_on="UTL_Time", right_on=right_date, how="outer"
    )

    return df  # left_df for next merge


# reduce_period(compact(merge_sources(allsrc)))
def merge_sources(allsrc):
    sources = allsrc.get_sources_time_logs()
    alm_df, nig_df, exp_df, nar_df = sources
    # UTL:: Unified Time Log, prefix for time index across sources
    datefld = "UTL_Time"
    utl_records = [
        {
            datefld: allsrc.min_dayobs,  # ut.get_datetime_from_dayobs_str(allsrc.min_dayobs),
            "log": "min",
        },
        {
            datefld: allsrc.max_dayobs,
            "log": "max",
        },
    ]
    index = pd.DatetimeIndex([r[datefld] for r in utl_records])
    utl_df = pd.DataFrame(utl_records, index)
    print(
        f"""DBG merge_sources:
    {utl_records=}
    {utl_df.index=}
    {utl_df=}
    """
    )

    df = utl_df
    # #!df =     merge_to_timelog(utl_df, alm_df, prefix="ALM_")
    if not nig_df.empty:
        df = merge_to_timelog(df, nig_df, prefix="NIG_")
    if not exp_df.empty:
        df = merge_to_timelog(df, exp_df, prefix="EXP_")
    if not nar_df.empty:
        df = merge_to_timelog(df, nar_df, prefix="NAR_")

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


template_decanted_df = """
<!DOCTYPE html>
<html>
<head>
    <style>
    th,td {
        text-align: left;
        vertical-align: text-top;
    }
    </style>
</head>
<body>
    <table>
        <tr>
            <th>Period</th>
        {% for col in table_columns %}
            <th>{{ col }}</th>
        {% endfor %}
        </tr>
    </table>

    {% for index, row in df.iterrows() %}
    <table>
        <tr>
            <th>{{ index }}</th>
            {% for val in row %}
            <td>{{ val }}</td>
            {% endfor %}
        </tr>
    </table>
    <p><b>SUMMARY: </b>{{ row['message_text'] }}</p>
    <p><b>NARRATIVE: </b>{{ row['message_text_NAR'] }}</p>
    {% endfor %}

</body>
</html>
"""

# white-space: pre-wrap;
# vertical-align: text-top;
#    background-color: coral;


def decant_by_column_name(df, nontable_columns):
    """Partition a DF frame into a dense part and a sparse part.
    The dense part is stored in a DF.
    The sparse part is stored in a dict[col] => {elem1, ...}
    """
    table_columns = sorted(df.columns)
    for c in nontable_columns:
        table_columns.remove(c)
    dense_df = df[table_columns]
    sparse_dict = {
        k: set(v.values()) for k, v in df[nontable_columns].to_dict().items()
    }
    return dense_df, sparse_dict


def decant_by_density(df, thresh):
    """Partition a DF frame a dense part and a sparse part.
    See: decant_by_column_name

    The columns that that are removed from DF are determined
    by the density THRESH (1- NaN/num_rows).
    """

    def density():
        pass  # TODO

    # Remove columns >= 95% NaN
    val_count = int(0.05 * len(df))
    df.dropna(thresh=val_count, axis="columns", inplace=True)  # DATA LOSS
    nontable_columns = []
    return decant_by_column_name(df, nontable_columns)


# for reduced DF
def render_reduced_df(df, thresh=0.95):
    dense_df, sparse_dict = decant_by_density(df, thresh)
    # https://jinja.palletsprojects.com/en/stable/templates/
    context = dict(
        df=dense_df,
        sparse_dict=sparse_dict,
    )
    return Template(template_decanted_df).render(**context)


def remove_list_columns(df):
    """Removes columns from a DataFrame that contain lists."""
    columns_to_drop = []
    for col in df.columns:
        if any(isinstance(x, list) for x in df[col]):
            columns_to_drop.append(col)
    return df.drop(columns_to_drop, axis=1), columns_to_drop


# compact results of merge_all()
# Started out Lossless.
#   With allow_data_loss=True, remove useless/problematic columns.
#   Also, see: remove_list_columns()
# Cell Values that are lists cause problems in pd.drop_duplicates()
def compact(full_df, delta="4h", allow_data_loss=False, verbose=False):
    df = full_df.copy()
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

    df["Period"] = df["UTL_Time"].apply(lambda x: x.floor(delta).hour)
    df.set_index(["Period", "UTL_Time"], inplace=True)
    df.dropna(how="all", axis="index", inplace=True)
    df.dropna(how="all", axis="columns", inplace=True)
    # df = df.fillna('')
    #   df = ut.wrap_dataframe_columns(df)  # TODO re-enable
    # Trim whitespace from all columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df, columns = remove_list_columns(df)  # DATA LOSS
    if verbose:
        print(f"WARNING removed {len(columns)} containing list values. {columns=}")
    return (
        df.reset_index()
        .set_index(["UTL_Time"])
        .drop_duplicates()
        .reset_index()
        .set_index(["Period", "UTL_Time"])
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
def reduce_period(df):
    def multi_string(group):
        return "\n\n".join([str(x) for x in set(group) if not pd.isna(x)])

    group_aggregator = dict(
        message_text=multi_string,
        exposure_flag=multi_string,
        message_text_NAR=multi_string,
        time_lost="sum",  # multi_number,
        time_lost_type=multi_string,
    )
    nuke_aggregators = set(list(group_aggregator.keys())) - set(df.columns.to_list())
    for col in nuke_aggregators:
        del group_aggregator[col]

    df = df.groupby(level="Period").agg(group_aggregator)
    return df
