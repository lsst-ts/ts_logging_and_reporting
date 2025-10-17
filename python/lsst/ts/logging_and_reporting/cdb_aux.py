import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
import requests


# Use https://usdf-rsp-dev.slac.stanford.edu/consdb/docs
# to create a table focused on where to find fields in the schema.
# Helpful to tell what instruments have a field.
# May lead to "instrument personality"
# endpoint='https://usdf-rsp-dev.slac.stanford.edu/consdb/schema'
def field_schema_location_table(
    schema_endpoint,  # .../consdb/schema
    exclude_instruments=None,
):
    exclude_default = {
        # "latiss",
        # "lsstcam",
        # "lsstcamsim",
        # "lsstcomcam",
        # "lsstcomcamsim",
        "startrackerfast",
        "startrackernarrow",
        "startrackerwide",
    }
    if exclude_instruments is None:
        exclude = exclude_default
    else:
        exclude = set(exclude_instruments)

    # schemas[instrum][table] => [field_name_1, ...]
    schemas = dict()

    timeout = (5.05, 20.0)
    url = schema_endpoint
    response = requests.get(url, timeout=timeout, headers=ut.get_auth_header())
    response.raise_for_status()
    instruments = set(response.json())

    # Collect Fields associated with non-excluded Instruments (with
    # the Table that contains them).
    available = instruments - exclude
    for instrument in available:
        schemas[instrument] = dict()
        url = f"{schema_endpoint}/{instrument}"
        response = requests.get(url, timeout=timeout, headers=ut.get_auth_header())
        response.raise_for_status()
        for table in set(response.json()):
            url = f"{schema_endpoint}/{instrument}/{table}"
            response = requests.get(url, timeout=timeout, headers=ut.get_auth_header())
            response.raise_for_status()
            schemas[instrument][table] = set(response.json().keys())
    # Now we have
    # schemas[instrum][table] => [field_name_1, ...]

    df = pd.DataFrame(
        [
            {"instrument": instrum, "table": table, "field": field}
            for instrum in schemas
            for table in schemas[instrum]
            for field in schemas[instrum][table]
        ]
    )

    # Its nice to pivot the table for viewing with something like:
    #   pv = df.pivot_table(index='field',
    #                       columns='instrument',
    #                       values='table',
    #                       aggfunc=lambda x: ' '.join(x))
    #   HTML(pv.to_html())

    pv = df.pivot_table(index="field", columns="instrument", values="table", aggfunc=list)

    return pv


def common_fields(df):
    """List of fields common to all instruments."""
    return df[~(df.isna().any(axis=1))].index.to_list()


def uncommon_fields(df):
    """List of fields that are missing from at least one instrument."""
    return df[df.isna().any(axis=1)].index.to_list()


def divergent_personalities(df):
    """Percent of fields in each instrument that is not common to
    all instruments.
    """
    return (df.isna().sum() / df.shape[0]).to_dict()
