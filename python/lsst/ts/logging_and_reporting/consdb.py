# Used efd.py as a starting point.  Layered in consolidated_database.py

import os
from collections import defaultdict

import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter

# curl -X 'POST' \
#   'https://usdf-rsp.slac.stanford.edu/consdb/query' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "query": "SELECT * FROM cdb_lsstcomcam.exposure LIMIT 2"
# }'


class ConsdbAdapter(SourceAdapter):
    # See https://usdf-rsp.slac.stanford.edu/consdb/docs
    service = "consdb"
    endpoints = [
        "schema",  # => list of instruments
        "schema/{instrument}",  # => list of tables
        "schema/{instrument}/{table}",  # => schema; dict(fname)=[type,dflt]
        "query",  # POST dict(query)=sql_string
    ]
    primary_endpoint = "NA"
    # join cdb_lsstcomcam.exposure/exposure_name
    #   to exposurelog/exposures.obs_id
    log_dt_field = "day_obs"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
        verbose=False,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
        )
        try:
            import lsst.rsp

            self.token = lsst.rsp.get_access_token()
        except Exception as err:
            if self.verbose:
                print(f"Could not get_access_token: {err}")
            self.token = os.environ.get("ACCESS_TOKEN")

        self.instruments = list()
        self.tables = defaultdict(list)  # tables[instrument]=>[tab1, ...]
        # schemas[instrum][table][fname] => [type,dflt]
        self.schemas = defaultdict(dict)
        self.load_schemas()

    # get schemas to facilitate generation of SQL
    def load_schemas(self):
        # get instruments
        if self.verbose:
            print("Loading schema: instruments")
        endpoint = f"{self.server}/{self.service}/schema"
        url = endpoint
        ok, result, code = self.protected_get(url, token=self.token)
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
            ok, result, code = self.protected_get(url, token=self.token)
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
                ok, result, code = self.protected_get(url, token=self.token)
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

    def query(self, sql):
        url = f"{self.server}/{self.service}/query"
        if self.verbose:
            print(f"DEBUG query: {url=} {sql=}")
        qdict = dict(query=sql)
        ok, result, code = self.protected_post(url, qdict, token=self.token)
        if not ok:  # failure
            print(f"ERROR: Failed POST {ok=} {result=} {code=}")
            return None
        else:
            records = [
                {c: v for c, v in zip(result["columns"], row)} for row in result["data"]
            ]
            return records

    # Changes coming, see:
    # DM-48072 Add a visit1_exposure table to link visits and exposures
    # In the meantime KT says assume visit_id = exposure_id
    # In the presence of snaps, exposure_id and visit_id many-to-one.
    def get_exposures(self, instrument):
        # DM-47573
        detail = [
            "exposure_flag",  # LOVE
            "obs_id",  # exposure_flexdata (exposurelog.exposure.id)
            "seq_num",  # exposure_flexdata
            "observation_type",  # LOVE
            "observation_reason",  # LOVE
            "science_program",  # LOVE
        ]
        print(f"DBG not using {detail=}")

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
            "exp_time",  # seconds
            "obs_start",  # TAI
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

        exposure_columns = ", ".join(["e." + c for c in exposure_out])
        quicklook_columns = ", ".join(["q." + c for c in quicklook_out])
        sql = f"""
        SELECT {exposure_columns}, {quicklook_columns}
          FROM cdb_{instrument}.exposure e, cdb_{instrument}.visit1_quicklook q
        WHERE e.exposure_id = q.visit_id
              AND {ut.dayobs_int(self.min_dayobs)} <= e.day_obs
              AND e.day_obs < {ut.dayobs_int(self.max_dayobs)}
        """

        records = self.query(sql)
        if records:
            df = pd.DataFrame(records)
            return ut.wrap_dataframe_columns(df)
        else:
            print(f"ERROR: get_exposures: {sql=}")
            return pd.DataFrame()  # empty
