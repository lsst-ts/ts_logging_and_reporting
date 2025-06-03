# Used efd.py as a starting point.  Layered in consolidated_database.py

import warnings
from collections import defaultdict

import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
import requests
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter
import httpx
import traceback

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
        # self.load_schemas() # Disable until needed

        # Load the data (records) we need from relevant endpoints
        # in dependency order.
        #  self.hack_reconnect_after_idle()   # Need for some sources
        self.status["instruments"] = self.get_instruments()
        if self.verbose:
            print(f"Debug ConsdbAdapter: {self.instruments=}")
        #TODO: remove this ??? Will it break the MVP?
        # for instrument in self.instruments:
        #     self.get_exposures(instrument)

    # NOTE: the API returns lowcase instrument names but
    # https://sdm-schemas.lsst.io/ lists CamelCase table names
    # where part of the name comes from the instrument. It turns
    # out EITHER lowcase or camel case is ok for table.
    # Identifiers in Postgresql are case insenstive unless quoted.
    def get_instruments(self, include=None) -> dict:
        url = f"{self.server}/{self.service}/schema"
        ok, result, code = self.protected_get(url)
        if not ok:  # failure
            print(f"ERROR: Failed GET {ok=} {result=} {code=}")
            return None
        available_instruments = set(result)
        include_default = {
            "latiss",
            "lsstcam",  # CDB lists as instrument but has no table for it!
            # "lsstcamsim",
            "lsstcomcam",
            # "lsstcomcamsim",
            # "startrackerfast",
            # "startrackernarrow",
            # "startrackerwide"
        }

        if include is None:  # use the default list
            include = include_default
        exclude = available_instruments - include
        if exclude and self.warning:
            elist = ", ".join(sorted(exclude))
            msg = f"Excluding these instruments from results: {elist}"
            warnings.warn(msg, category=ex.ExcludeInstWarning, stacklevel=2)

        # Some sources are case sensitive and use CamelCase.
        # ConsDB will handle either, but REPORT lower case.
        # To be compatible with other sources, map to CamelCase.
        # These match https://sdm-schemas.lsst.io/
        camel = {
            "latiss": "LATISS",
            "lsstcam": "LSSTCam",
            "lsstcamsim": "LSSTCamSim",
            "lsstcomcam": "LSSTComCam",
            "lsstcomcamsim": "LSSTComCamSim",
            "startrackerfast": "StarTrackerFast",
            "startrackernarrow": "StarTrackerNarrow",
            "startrackerwide": "StarTrackerWide",
        }

        # Instruments that have a least one table associated with them.
        # #!available_instruments = {instrum
        # #!                        for instrum in self.schemas.keys()
        # #!                        if self.schemas.get(instrum,None)
        # #!                        }
        self.instruments = [camel[inst] for inst in include]

        status = dict(
            endpoint_url=url,
            number_of_records=len(result),
            error=None,
        )
        return status

    @property
    def all_available_fields(self):
        # schemas[instrum][table][fname]=[type,dflt]
        return [  # instrument/tablename/fieldname
            "/".join([instrum, tname, fname])
            for instrum, tables in self.schemas.items()
            for tname, fields in tables.items()
            for fname in fields.keys()
        ]

    def get_sample_of_each(self, day_obs):
        instrument = self.instruments[0]
        exposure_sql = (
            f"SELECT * FROM cdb_{instrument}.exposure WHERE day_obs = {day_obs}"
        )
        s1 = self.query(exposure_sql)
        return s1

    # The consdb/query endpoint will return
    # "500 Internal Server Error"
    # if there is something wrong the SQL.
    # Not a "400 Bad Request" as it should.
    # It will then put the Postgres error in the resonse body as json
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
                url, json=jsondata, timeout=timeout, headers=ut.get_auth_header(self.token)
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
        else:  # No exception. Could something else be wrong?
            result = response.json()
            records = [
                {c: v for c, v in zip(result["columns"], row)} for row in result["data"]
            ]
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
        ssql = f"""SELECT *
          FROM cdb_{instrument}.exposure e, cdb_{instrument}.visit1_quicklook q
          WHERE e.exposure_id = q.visit_id
              AND {ut.dayobs_int(self.min_dayobs)} <= e.day_obs
              AND e.day_obs < {ut.dayobs_int(self.max_dayobs)}
        """
        sql = " ".join(ssql.split())  # remove redundant whitespace
        records = self.query(sql)
        if self.verbose and len(records) > 0:
            print(f"Debug cdb.get_exposures {instrument=} {sql=}")
            print(f"Debug cdb.get_exposures: {records[0]=}")

        self.exposures[instrument] = records

        if records:
            df = pd.DataFrame(records)
            return ut.wrap_dataframe_columns(df)
        else:
            if self.warning:
                msg = f"No records found for ConsDB for {instrument=}."
                warnings.warn(msg, category=ex.NoRecordsWarning, stacklevel=2)
            return pd.DataFrame()  # empty

    # TODO Remove if this is still here after Feb 2025
    # This is here just to validate data.
    # Yes, indeed. The day_obs increments a day when obs_start crosses
    # the 12:00 Chile boundary (15:00 UTC in December)
    def test_get_exposure_times(self, instrument, min_dayobs, max_dayobs):
        sql = f"""
        SELECT obs_start, day_obs
          FROM cdb_{instrument}.exposure
        WHERE {min_dayobs} <= day_obs AND day_obs < {max_dayobs}
        """
        records = self.query(sql)
        return records

    # Possibly OBSOLETE: get schemas to facilitate generation of SQL
    # Instead see: https://sdm-schemas.lsst.io/cdb_lsstcomcam.html
    # Except: What the schemas we actually need in a given case depends
    # on that data we find for the night.
    def load_schemas(self):
        # schemas[instrum][table][fname] => [type,dflt]
        # get instruments
        if self.verbose:
            print("Loading schema: instruments")
        endpoint = f"{self.server}/{self.service}/schema"
        url = endpoint
        ok, result, code = self.protected_get(url)
        if not ok:  # failure
            status = dict(
                endpoint_url=url,
                number_of_records=None,
                error=result,
            )
            return status
        # success
        self.instruments = result
        if self.verbose:
            print(f"Loaded {self.instruments=}")

        # get tables[instrument] => [table1, ...]
        if self.verbose:
            print("Loading schema: tables[instrument]")
        for instrument in self.instruments:
            endpoint = f"{self.server}/{self.service}/schema"
            url = f"{endpoint}/{instrument}"
            ok, result, code = self.protected_get(url)
            if not ok:  # failure
                status = dict(
                    endpoint_url=url,
                    number_of_records=None,
                    error=result,
                )
                return status
            # success
            if self.verbose:
                print(f"Stuffing {self.tables=}")

            self.tables[instrument] = result
            if self.verbose:
                print(f"Loaded {self.tables[instrument]=}")

        # get schemas[instrum][table][fname] => [type,dflt]
        if self.verbose:
            print("Loading schema: fields [instrument][table]")
        for instrument in self.instruments:
            for table in self.tables[instrument]:
                endpoint = f"{self.server}/{self.service}/schema"
                url = f"{endpoint}/{instrument}/{table}"
                ok, result, code = self.protected_get(url)
                if not ok:  # failure
                    status = dict(
                        endpoint_url=url,
                        number_of_records=None,
                        error=result,
                    )
                    return status
                # success
                self.schemas[instrument][table] = result
                if self.verbose:
                    print(f"Loaded {self.schemas[instrument][table]=}")

        if self.verbose:
            print(f"Loaded Consolidated Databased schemas: {self.schemas=}")
        # END load_schemas()

    async def query_from_app(self, sql):
        url = f"{self.server}/{self.service}/query"
        jsondata = dict(query=sql)
        timeout = self.timeout
        try:
            headers = ut.get_auth_header(self.token)
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, json=jsondata, timeout=timeout, headers=headers
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as err:
            # Handles HTTP errors, e.g., 400, 404, 500, etc.
            traceback.print_exc()
            url = err.request.url
            try:
                apimsg = err.response.json().get("message")
            except ValueError:
                apimsg = err.response.text  # Fallback to plain text if not JSON
            raise ex.ConsdbQueryError(
                f"Upstream error from {self.abbrev} while requesting {url}: {apimsg}"
            ) from err
        except httpx.RequestError as err:
            # Handles connection errors, timeouts, etc.
            traceback.print_exc()
            raise ex.ConsdbQueryError(
                f"Connection error from {self.abbrev} while requesting {url}: {str(err)}"
            ) from err
        result = response.json()
        records = [
            {c: v for c, v in zip(result["columns"], row)} for row in result["data"]
        ]
        return records
