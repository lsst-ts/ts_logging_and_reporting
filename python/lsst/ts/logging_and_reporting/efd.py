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

import nest_asyncio
import pandas as pd
from astropy.time import Time, TimeDelta
from lsst_efd_client import EfdClient

import lsst.ts.logging_and_reporting.utils as ut
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter

nest_asyncio.apply()

# from lsst.summit.utils.efdUtils import getEfdData, getTopics, makeEfdClient


class EfdAdapter(SourceAdapter):
    abbrev = "EFD"
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
        instance_url = ut.Server.get_url()
        self.server_url = server_url or instance_url
        match self.server_url:
            case ut.Server.summit:
                self.client = EfdClient("summit_efd")
                # #!self.client = makeEfdClient()
            case ut.Server.usdfdev:
                self.client = EfdClient("usdf_efd")
                # #!self.client = makeEfdClient()
                os.environ["RUBIN_SIM_DATA_DIR"] = "/sdf/data/rubin/shared/rubin_sim_data"
            case ut.Server.usdf:
                self.client = EfdClient("usdf_efd")
                # #!self.client = makeEfdClient()
                os.environ["RUBIN_SIM_DATA_DIR"] = "/sdf/data/rubin/shared/rubin_sim_data"
            case ut.Server.tucson:
                pass
            case ut.Server.base:
                pass
            case _:
                msg = (
                    f"Unknown value for environment variable EXTERNAL_INSTANCE_URL\n"
                    f"Got {self.server_url=} \n"
                    f"Expected one of: \n"
                    f"{ut.Server.get_all()}"
                )
                raise Exception(msg)

        self.targets = None
        self.mount_moves = None
        self.status["targets"] = dict(
            endpoint_url="NA",
            number_of_records=0,
            error=None,
        )

    async def get_topics(self):
        self.topics = await self.client.get_topics()
        return self.topics

    async def get_fields_from_topics(self, topic_list):
        topic_dict = dict()
        for topic in topic_list:
            topic_dict[topic] = await self.client.get_fields(topic)
        return topic_dict

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
        print(f"Query all {len(all_topics)} topics (this will take a long time - maybe 2 hours): ")
        ut.tic()
        for topic in all_topics:
            if max_topics and (topic_count > max_topics):
                print("Aborting after 100 topics")
                break
            print(".", end="", flush=True)
            try:
                fields = [f for f in await self.client.get_fields(topic) if not f.startswith("private_")]
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
        print(f" DONE in {ut.toc() / 60} minutes")
        return populated, errors, topic_count

    async def query_nights(self, topic, fields, min_date=None, max_date=None, index=None):
        start = Time(min_date or self.min_date)
        end = Time(max_date or self.max_date)

        # TODO resample
        series = await self.client.select_time_series(topic, fields, start=start, end=end, index=index)
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

    # Dates found:
    #   display(sorted(set([str(t.date()) for t in moves.index])))
    async def get_mount_moves(self):
        if self.mount_moves is not None:  # is cached
            return self.mount_moves

        topics = [
            "lsst.sal.ATDome.logevent_azimuthInPosition",
            "lsst.sal.ATMCS.logevent_azimuthInPosition",
            "lsst.sal.MTMount.logevent_azimuthInPosition",
            "lsst.sal.ATDome.logevent_elevationInPosition",
            "lsst.sal.ATMCS.logevent_elevationInPosition",
            "lsst.sal.MTMount.logevent_elevationInPosition",
        ]
        fields_wanted = [
            "inPosition",
        ]

        moves = dict()
        for topic in topics:
            colname = topic.replace("lsst.sal.", "").replace(".logevent_", "_")
            colname = colname.replace("InPosition", "")
            print(f"Debug get_mount_moves: {topic=} {colname=}")
            series = await self.query_nights(topic, fields_wanted)
            moves[topic] = series.rename(columns={"inPosition": colname})
        df = pd.concat(moves.values(), axis=1)
        return df

    async def query_weather_from_sal_components(self, days=1):
        result = dict(
            ess_wind=await self.query_nights(
                "lsst.sal.ESS.airFlow",
                ["speed", "direction"],  # m/s
                index=self.salindex,
            ),
            ess_temp=await self.query_nights(
                "lsst.sal.ESS.temperature",
                "temperatureItem0",  # C
                index=self.salindex,
            ),
            ess_dewpoint=await self.query_nights(
                "lsst.sal.ESS.dewPoint",
                "dewPointItem",  # C
                index=self.salindex,
            ),
            ess_humidity=await self.query_nights(
                "lsst.sal.ESS.relativeHumidity",
                "relativeHumidityItem",  # %
                index=self.salindex,
            ),
            ess_pressure=await self.query_nights(
                "lsst.sal.ESS.pressure",
                "pressureItem0",  # mbar
                index=self.salindex,
            ),
            dimm_fwhm=await self.query_nights(
                "lsst.sal.DIMM.logevent_dimmMeasurement",
                "fwhm",  # arcsec
                index=self.salindex,
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
