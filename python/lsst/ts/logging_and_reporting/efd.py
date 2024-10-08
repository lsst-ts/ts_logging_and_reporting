"""
NOTES:
- blocks_ipynb (part broken): some good stuff, but uses
     lsst.summit which we do not have in docker.
- FBS_Targets.ipynb (part broken): Much not runable in Docker-dev
     no import: colorcet, rubin_scheduler
     no os.environ: no_proxy
- groups.ipynb (part broken): get_df_from_next_visit_events(date)
- timing.ipynb (part broken): async def to get visit events
- weather.ipynb (all works): USE ALL in our NightLog?
"""

import asyncio
import datetime as dt
import os

import lsst.ts.logging_and_reporting.utils as ut
from astropy.time import Time, TimeDelta
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter
from lsst_efd_client import EfdClient


# Servers we might use
class Server:
    summit = "https://summit-lsp.lsst.codes"
    usdf = "https://usdf-rsp-dev.slac.stanford.edu"
    tucson = "https://tucson-teststand.lsst.codes"


class EfdAdapter(SourceAdapter):
    salindex = 2
    service = "efd"
    endpoints = [
        "targets",
    ]
    primary_endpoint = "targets"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
    ):
        super().__init__(max_dayobs=max_dayobs, min_dayobs=min_dayobs)

        self.client = None
        instance_url = os.getenv("EXTERNAL_INSTANCE_URL", self.server)
        self.server_url = server_url or instance_url
        match self.server_url:
            case Server.summit:
                self.client = EfdClient("summit_efd")
            case Server.usdf:
                self.client = EfdClient("usdf_efd")
                os.environ["RUBIN_SIM_DATA_DIR"] = (
                    "/sdf/data/rubin/shared/rubin_sim_data"
                )
            case Server.tucson:
                pass
            case _:
                msg = (
                    f"Unknown server from EXTERNAL_INSTANCE (env var).  "
                    f"Got {self.server_url=} "
                    f"Expected one of: "
                    f"{Server.summit}, {Server.usdf}, {Server.tucson}"
                )
                raise Exception(msg)

        self.targets = None
        self.status["targets"] = dict(
            endpoint_url="NA",
            number_of_records=0,
            error=None,
        )

    async def get_topics(self):
        self.topics = await self.client.get_topics()
        return self.topics

    # Ran <2024-09-26 Thu> (during possible USDF-dev issues)
    #   Run time: 111 minutes
    #   Total topics: 2778
    #   Total successful queries: 2564
    #   Total with data: 930
    #   Total failed EFD client calls: 92
    #      Possibly due to underlying SQL not matching schema.
    async def find_populated_topics(self, days=1, max_topics=None):
        topic_count = 0
        end = Time(dt.datetime.combine(self.max_date, dt.time()))
        start = end - TimeDelta(days, format="jd")
        errors = dict()  # errors[topic] = error_message
        populated = dict()  # populated[topic] = [field, ...]
        all_topics = await self.client.get_topics()
        print(
            f"Query all {len(all_topics)} topics "
            "(this will take a long time - maybe 2 hours): "
        )
        ut.tic()
        for topic in all_topics:
            if max_topics and (topic_count > max_topics):
                print("Aborting after 100 topics")
                break
            print(".", end="", flush=True)
            try:
                fields = [
                    f
                    for f in await self.client.get_fields(topic)
                    if not f.startswith("private_")
                ]
                if not fields:
                    continue
            except Exception as err:
                errors[topic] = str(err)
                continue

            try:
                series = await self.client.select_time_series(
                    topic,
                    fields,
                    start=start,
                    end=end,
                )
                topic_count += 1
                if not series.empty:
                    populated[topic] = fields
            except Exception as err:
                errors[topic] = str(err)
        print(f" DONE in {ut.toc()/60} minutes")
        return populated, errors, topic_count

    async def query_nights(self, topic, fields, index=301):
        start = Time(self.min_date)
        end = Time(self.max_date)

        # TODO resample
        series = await self.client.select_time_series(
            topic, fields, start=start, end=end, index=index
        )
        return series

    # slewTime (and probably others) are EXPECTED times, not ACTUAL.
    async def get_targets(self):
        if self.targets is not None:  # is cached
            return self.targets

        topic = "lsst.sal.Scheduler.logevent_target"
        fields_wanted = [
            "blockId",
            "sequenceDuration",
            "sequenceNVisits",
            "sequenceVisits",
            "slewTime",
        ]
        # end = Time(datetime.combine(self.max_date, time()))
        targets = await self.query_nights(
            topic,
            fields_wanted,
            index=self.salindex,
        )
        self.targets = targets
        self.status["targets"] = dict(
            endpoint_url="NA",
            number_of_records=len(targets),
            error=None,
        )
        return targets

    async def get_weather(self, days=1):

        result = dict(
            ess_wind=await self.query_nights(
                "lsst.sal.ESS.airFlow", ["speed", "direction"]  # m/s
            ),
            ess_temp=await self.query_nights(
                "lsst.sal.ESS.temperature", "temperatureItem0"  # C
            ),
            ess_dewpoint=await self.query_nights(
                "lsst.sal.ESS.dewPoint", "dewPointItem"  # C
            ),
            ess_humidity=await self.query_nights(
                "lsst.sal.ESS.relativeHumidity", "relativeHumidityItem"  # %
            ),
            ess_pressure=await self.query_nights(
                "lsst.sal.ESS.pressure", "pressureItem0"  # mbar
            ),
            dimm_fwhm=await self.query_nights(
                "lsst.sal.DIMM.logevent_dimmMeasurement", "fwhm"  # arcsec
            ),
        )
        return result


async def main():
    pass


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # For environments where the event loop is already running
            loop.create_task(main())
        else:
            # Run normally if no loop is running
            asyncio.run(main())
    except RuntimeError:
        # If there's no event loop, start one
        asyncio.run(main())
