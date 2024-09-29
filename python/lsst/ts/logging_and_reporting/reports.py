# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Python Standard Library
from urllib.parse import urlencode
import itertools
from datetime import datetime, date, time, timedelta
from warnings import warn
from collections import defaultdict
from abc import ABC
# External Packages
import requests
from IPython.display import display, Markdown
import pandas as pd
# Local Packages
import lsst.ts.logging_and_reporting.almanac as alm

def md(markdown_str, color=None):
    # see https://www.w3schools.com/colors/colors_names.asp
    if color:
        display(Markdown(f"<font color='{color}'>{markdown_str}</font>"))
    else:
        display(Markdown(markdown_str))

def mdlist(markdown_list, color=None):
    if markdown_list is None:
        return

    for markdown_str in markdown_list:
        md(markdown_str, color=color)


def dict_to_md(in_dict):
    md_list = list()
    for key, content_list in in_dict.items():
        md_list.append(f'- {key}')
        for elem in content_list:
            md_list.append(f'    - {elem}')
    return md_list

def adapter_overview(adapter):
    status = adapter.get_status()
    count = status["number_of_records"]
    error  =  status["error"]
    more = '(There may be more.)' if count >= adapter.limit else ''
    result = error if error else f'Got {count} records. '
    mdlist([f'<a id="overview{adapter.service}"></a>\n## Overview for Service: `{adapter.service}` [{count}]',
            f'- Endpoint: {status["endpoint_url"]}',
            ])
    print(f'- {result} {more}')


# TODO move all instances of "row_header", "row_str_func" from source_adapters to here.
class Report(ABC):
    def __init__(self, *,
                 adapter=None, # instance of SourceAdapter
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today
                 ):
        self.source_adapter = adapter
        self.min_day_obs = min_day_obs
        self.max_day_obs = max_day_obs

    def day_obs_report(self, day_obs):
        """
        Create report for one source using data from one
        observing night (day_obs).
        """
        if adapter:
            self.time_log_as_markdown(
                log_title=f'{adapter.service.title()} Report for {day_obs}'
            )

    def time_log_as_markdown(self,
                             log_title=None,
                             zero_message=False,
                             ):
        '''Emit markdown for a date-time log.'''
        adapter = self.source_adapter
        records = adapter.records
        service = adapter.service
        url = adapter.get_status().get('endpoint_url')
        title = log_title if log_title else ''
        if records:
            md(f'### {title}')
            table = self.source_adapter.day_table('date_added')
            mdlist(table)
        else:
            if zero_message:
                md(f'No {service} records found.', color='lightblue')
                md(f'Used [API Data]({url})')

class AlmanacReport(Report):
    # moon rise,set,illumination %
    # (astronomical,nautical,civil) twilight (morning,evening)
    # sun rise,set

    def day_obs_report(self, day_obs):
        display(self.almanac_as_dataframe(day_obs))

    def almanac_as_dataframe(self, day_obs):
        # This display superfluous header: "0, 1"
        return pd.DataFrame(alm.Almanac(day_obs=day_obs).as_dict).T


class NightlyLogReport(Report):

    def day_obs_report(self, day_obs):
        if adapter:
            self.time_log_as_markdown(
                log_title=f'{adapter.service.title()} Report for {day_obs}'
            )


    def block_tickets_as_markdown(self,  tickets,
                                  title='## Nightly Jira BLOCKs'):
        # tickets[day_obs] = {ticket_url, ...}
        mdstr = ''
        if title:
            mdstr += title

        for day, url_list in tickets.items():
            mdstr += f'\n- {day}'
            for ticket_url in url_list:
                mdstr += f'\n    - [{ticket_url.replace(front,"")}]({ticket_url})'
        return mdstr


class ExposurelogReport(Report):

    # date, time, obs_id, message_text
    def time_log_as_markown(self, records,
                            title='# Exposure Log'):
        pass # TODO use "day_table"

    def daily_observation_gap(self, min_day_obs, max_day_obs):
        pass

class NarrativelogReport(Report):

    # date, time, message_text
    def time_log_as_markown(self, records,
                            title='# Exposure Log'):
        pass # TODO use "day_table"

class NightObsReport(Report):
    """Generate a report that combines all sources using data from one day_obs.
    This ignores reporting that is done once no matter how many
    nights (Overview, Links).
    Generate one page for each observing night. An Observing Night includes
    the evening of day_obs and morning of the day after day_obs.
    A page includes: Almanac, NightReport, Exposure, Narrative.
    """
