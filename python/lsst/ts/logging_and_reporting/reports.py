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
from lsst.ts.logging_and_reporting.utils import datetime_to_dayobs
import lsst.ts.logging_and_reporting.almanac as alm

def md(markdown_str, color=None):
    # see https://www.w3schools.com/colors/colors_names.asp
    if color:
        display(Markdown(f"### <font color='{color}'>{markdown_str}</font>"))
    else:
        display(Markdown(markdown_str))

def mdlist(markdown_list, color=None):
    for markdown_str in markdown_list:
        md(markdown_str, color=color)


def dict_to_md(in_dict):
    md_list = list()
    for key, content_list in in_dict.items():
        md_list.append(f'- {key}')
        for elem in content_list:
            md_list.append(f'    - {elem}')
    return md_list



# TODO move all instances of "row_header", "row_str_func" from source_adapters to here.
class Report(ABC):
    def __init__(self, *,
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today
                 ):
        pass  # TODO

class AlmanacReport(Report):
    # moon rise,set,illumination %
    # (astronomical,nautical,civil) twilight (morning,evening)
    # sun rise,set
    def almanac_as_dataframe(self):
        # This display superfluous header: "0, 1"
        return pd.DataFrame(alm.Almanac().as_dict).T


class NightlyLogReport(Report):

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
