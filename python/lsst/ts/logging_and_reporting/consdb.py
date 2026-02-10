# Used efd.py as a starting point.  Layered in consolidated_database.py

import traceback
import warnings
from collections import defaultdict

import pandas as pd
import requests

import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.utils as ut
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter

# curl -X 'POST' \
#   'https://usdf-rsp.slac.stanford.edu/consdb/query' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "query": "SELECT * FROM cdb_lsstcomcam.exposure LIMIT 2"
# }'


class ConsdbAdapter(SourceAdapter):
    abbrev = "CDB"
    # See https://usdf-rsp-dev.slac.stanford.edu/consdb/docs
    service = "consdb"
    endpoints = [
        "",  # => dict[instruments, obs_types, dtypes] => [val1, ...]
        "schema",  # => list of instruments
        "schema/{instrument}",  # => list of tables
        "schema/{instrument}/{table}",  # => schema; dict(fname)=[type,dflt]
        "query",  # POST dict(query)=sql_string
    ]
    primary_endpoint = "NA"
    # join cdb_lsstcomcam.exposure/exposure_name
    #   to exposurelog/exposures.obs_id
    log_dt_field = "obs_start"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
        verbose=False,
        warning=True,
        auth_token=None,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
            warning=warning,
            auth_token=auth_token,
        )

        self.status = dict()
        self.exposures = dict()  # dd[instrument] = [rec, ...]
        self.instruments = list()
        self.tables = defaultdict(list)  # tables[instrument]=>[tab1, ...]

        # schemas[instrum][table][fname] => [type,dflt]
        self.schemas = defaultdict(dict)

        if self.verbose:
            print(f"Debug ConsdbAdapter: {self.instruments=}")

    # The consdb/query endpoint will return
    # "500 Internal Server Error"
    # if there is something wrong the SQL.
    # Not a "400 Bad Request" as it should.
    # It will then put the Postgres error in the response body as json
    # under the key "message".  Seems convoluted, so codify handling of it
    # here.
    def query(self, sql):
        url = f"{self.server}/{self.service}/query"
        if self.verbose:
            print(f"DEBUG query: {url=} {sql=}")
        jsondata = dict(query=sql)
        timeout = self.timeout
        records = []
        try:
            response = requests.post(
                url,
                json=jsondata,
                timeout=timeout,
                headers=ut.get_auth_header(self.token),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            # Invalid URL?, etc.
            code = err.response.status_code
            reason = err.response.reason
            apimsg = err.response.json()["message"]
            msg = f"""Error in {self.abbrev}.query(). Bad Response.
              {apimsg=}
              {sql=!r} {url=}
              {code=} {reason=} {timeout=}
              {str(err)}
            """
            warnings.warn(msg, category=ex.ConsdbQueryError, stacklevel=2)
            traceback.print_exc()
            raise ex.ConsdbQueryError(f"Upstream error: {msg}") from err
        except requests.exceptions.ConnectionError as err:
            # No VPN? Broken API?
            code = None
            msg = f"""Error in {self.abbrev}.query() connecting to Service.
              {url=}
              {jsondata=}
              {timeout=};
              {str(err)}.
            """
            warnings.warn(msg, category=ex.ConsdbQueryError, stacklevel=2)
            traceback.print_exc()
            raise ex.ConsdbQueryError(f"Connection error: {msg}") from err
        result = response.json()
        # The exposure and visit1_quicklook tables have some duplicate columns.
        # The below code makes sure that any null values returned by
        # visit1_quicklook do not overwrite valid values from exposure.
        records = []
        duplicate_columns = set()
        for row in result["data"]:
            record = {}
            for col, val in zip(result["columns"], row):
                if col in record:
                    # Track duplicates
                    duplicate_columns.add(col)
                    # Merge logic: keep first non-null value
                    if record[col] is None and val is not None:
                        record[col] = val
                else:
                    record[col] = val
            records.append(record)
        if duplicate_columns and self.warning:
            msg = (
                f"Duplicate ConsDB columns detected and merged safely: {', '.join(sorted(duplicate_columns))}"
            )
            warnings.warn(msg, category=ex.ConsdbQueryWarning, stacklevel=2)
        if len(records) == 0 and self.warning:
            msg = f"No results returned from {self.abbrev}.query().  "
            msg += f"{sql=!r} {url=}"
            warnings.warn(msg, category=ex.ConsdbQueryWarning, stacklevel=2)
        return records

    # Changes coming, see:
    # DM-48072 Add a visit1_exposure table to link visits and exposures
    # In the meantime KT says assume visit_id = exposure_id
    # In the presence of snaps, exposure_id and visit_id many-to-one.
    def get_exposures(self, instrument) -> pd.DataFrame:
        """SIDE-EFFECT: cache results in self.exposures[instrument]"""
        # DM-47573
        exposure_out = [
            "air_temp",
            # 'ccd_temp',   # Does not exist (yet?)
            "airmass",
            # Coordinates
            "altitude",  # also *_start, *_end
            "azimuth",  # also *_start, *_end
            "sky_rotation",
            "s_ra",
            "s_dec",
            # 'camera_rotation_angle',  # Does not exist (yet?)
            "band",
            "dimm_seeing",
            "exposure_id",
            "exposure_name",
            "target_name",
            "group_id",
            "exp_time",  # seconds, duration
            "obs_start",  # TAI
            "day_obs",  # int
        ]
        # EXTRAS: "day_obs", "seq_num", "exp_time", "shut_time", "dark_time",
        quicklook_out = [
            "sky_bg_median",
            "seeing_zenith_500nm_median",  # also *_min,*_max
            "psf_trace_radius_delta_median",  # not in LATISS
            "high_snr_source_count_median",
            "zero_point_median",
            "visit_id",
        ]

        if self.verbose:
            exposure_columns = ", ".join(["e." + c for c in exposure_out])
            quicklook_columns = ", ".join(["q." + c for c in quicklook_out])
            print(f"{exposure_columns=}")
            print(f"{quicklook_columns=}")

        # Would like to select just exposure_columns, quicklook_columns
        # except that for some instruments they aren't all there
        # (LATISS missing q.sky_bg_median).
        # Using a left join here to return all exposures, decorated with
        # quicklook data when available.
        ssql = f"""
            SELECT *
            FROM cdb_{instrument}.exposure e
            LEFT JOIN cdb_{instrument}.visit1_quicklook q
                ON e.exposure_id = q.visit_id
            WHERE {ut.dayobs_int(self.min_dayobs)} <= e.day_obs
                AND e.day_obs < {ut.dayobs_int(self.max_dayobs)}
        """
        sql = " ".join(ssql.split())  # remove redundant whitespace
        records = self.query(sql)
        if self.verbose and len(records) > 0:
            print(f"Debug cdb.get_exposures {instrument=} {sql=}")
            print(f"Debug cdb.get_exposures: {records[0]=}")

        self.exposures[instrument] = records

        if records:
            return pd.DataFrame(records)
        else:
            if self.warning:
                msg = f"No records found for ConsDB for {instrument=}."
                warnings.warn(msg, category=ex.NoRecordsWarning, stacklevel=2)
            return pd.DataFrame()  # empty

    def get_transformed_efd_data(self, instrument: str) -> pd.DataFrame:
        """Query transformed EFD table for columns associated with exposures.

        Columns to be retrieved are hard-coded per instrument in efd_fields.

        Parameters
        ----------
        instrument : `str`
            Instrument name, e.g. "LATISS", "LSSTCam"

        Returns
        -------
        df : `pandas.DataFrame`
            DataFrame with transformed EFD data associated with exposures.
            Empty DataFrame if error occurs querying the Transformed EFD.
        """

        # If further columns are needed, add those attributes to these
        # per-instrument channel lists
        efd_fields = {
            "LATISS": [],
            "LSSTCam": ["mt_salindex112_temperature_0_mean"],
        }
        fields = efd_fields.get(instrument, [])

        if not fields:
            return pd.DataFrame()

        table_name = f"efd_{instrument}"

        def make_sql(table_name):
            ssql = f"""
                SELECT
                    exposure_id,
                    {", ".join(fields)}
                FROM
                    {table_name}.exposure_efd e
                WHERE
                    {ut.dayobs_int(self.min_dayobs)} <= e.day_obs
                    AND e.day_obs < {ut.dayobs_int(self.max_dayobs)};
                """
            return " ".join(ssql.split())

        sql = make_sql(table_name)

        try:
            exposures = self.query(sql)
        except Exception as e:
            msg = f"Error querying transformed EFD data from ConsDB: {e}"
            warnings.warn(msg, category=ex.ConsdbQueryWarning, stacklevel=2)
            return pd.DataFrame()

        return pd.DataFrame(exposures)
