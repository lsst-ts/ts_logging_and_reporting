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

from abc import ABC
from urllib.parse import urlparse

import pandas as pd
from IPython.display import HTML, Markdown, display


def md(markdown_str, color=None, background=None):
    # see https://www.w3schools.com/colors/colors_names.asp
    if color or background:
        fg = f"color:{color};" if color else ""
        bg = f"background-color:{background};"
        newtxt = f'<font style="{fg}{bg}">{markdown_str}</font>'
        display(Markdown(newtxt))
    else:
        display(Markdown(markdown_str))


htmlgood = '<font style="background-color:green; color:white; font-size:20px">&nbsp;G&nbsp;</font>'
htmlquestion = '<font style="background-color:yellow; color:black; font-size:20px">&nbsp;?&nbsp;</font>'
htmlbad = '<font style="background-color:red; color:black; font-size:20px">&nbsp;R&nbsp;</font>'


def html_beta(text):
    msg = f'<font style="background-color:green; color:white; font-size:20px">BETA</font> {text}'
    return msg


def html_draft(text):
    msg = f'<font style="background-color:yellow; color:black; font-size:20px">DRAFT</font> {text}'
    return msg


def mdlink(
    url,
    title=None,
    description="",
    caveat="Links to an external page that might not be maintained.",
):
    """Wrap link in html to open link in new tab, must be used in an md()"""
    if title is None:
        title = url
    html = f'<a href="{url}" target="_blank" rel="noreferrer noopener">{title}</a> {description} ({caveat}) '
    return html


def mdpathlink(url, remove="/browse/"):
    string = urlparse(url).path.replace(remove, "")
    return mdlink(string)


def mdfragmentlink(url, remove="!/"):
    string = urlparse(url).fragment.replace(remove, "")
    return mdlink(string)


def display_error(text, fgcolor="black", bgcolor="red", size="1em"):
    style = ""
    style += f"color: {fgcolor}; "
    style += f"background-color: {bgcolor}; "
    style += f"font-size: {size}; "

    elem = "pre"
    return display(HTML(f"<{elem} {style=}>{text}</{elem}>"))


def htmlcode(text, fgcolor="black", bgcolor="white", size="1em", left=0):
    style = ""
    style += f"color: {fgcolor}; "
    style += f"background-color: {bgcolor}; "
    style += f"font-size: {size}; "
    style += f"margin-left: {left}px; "

    # code, samp, pre
    elem = "pre"
    return f"<{elem} {style=}>{text}</{elem}>"


def mdlist(markdown_list, color=None):
    if markdown_list is None:
        return

    for markdown_str in markdown_list:
        md(markdown_str, color=color)


def dict_to_md(in_dict):
    md_list = list()
    for key, content_list in in_dict.items():
        md_list.append(f"- {key}")
        for elem in content_list:
            md_list.append(f"    - {elem}")
    return md_list


# TODO move all instances of "row_header", "row_str_func"
# from source_adapters to here.
class Report(ABC):
    def __init__(
        self,
        *,
        adapter=None,  # instance of SourceAdapter
    ):
        self.source_adapter = adapter

    def day_obs_report(self, day_obs):
        """
        Create report for one source using data from one
        observing night (day_obs).
        """
        adapter = self.source_adapter
        if adapter:
            self.time_log_as_markdown(log_title=f"{adapter.service.title()} Report for {day_obs}")

    def overview(self):
        """Emit overview of a source."""
        adapter = self.source_adapter
        status = adapter.get_status()
        count = status["number_of_records"]
        error = status["error"]
        result = error if error else f"Got {count} records. "

        print(md(f"### Overview for Service: `{adapter.service}` [{count}]"))
        print(md(f"- Endpoint: {status['endpoint_url']}"))
        print(f"- {result}")

    def time_log_as_markdown(self, zero_message=True):
        """Emit markdown for a date-time log."""
        adapter = self.source_adapter
        records = adapter.records
        if records:
            table = self.source_adapter.day_table("date_added")
            mdlist(table)
        else:
            if zero_message:
                service = adapter.service
                url = adapter.get_status().get("endpoint_url")
                msg = (
                    f"No {service} records found {adapter.min_dayobs} to {adapter.max_dayobs}. "
                    # f"status={adapter.status}"
                )
                md(msg, color="lightblue")
                md(f"Used [API Data]({url})")


class AlmanacReport(Report):
    # moon rise,set,illumination %
    # (astronomical,nautical,civil) twilight (morning,evening)
    # sun rise,set

    def day_obs_report(self):
        dayobs = self.source_adapter.min_dayobs
        md(f"**Almanac for the observing night starting {dayobs}**")
        df = self.almanac_as_dataframe()
        display(df.style.hide(axis="columns", subset=None))

    def almanac_as_dataframe(self):
        return pd.DataFrame.from_records(self.source_adapter.as_dict, index=["UTC", "Comment"]).T.sort_values(
            "UTC"
        )


class NightlyLogReport(Report):
    def day_obs_report(self, day_obs):
        adapter = self.source_adapter
        if adapter:
            self.time_log_as_markdown(log_title=f"{adapter.service.title()} Report for {day_obs}")

    def block_tickets_as_markdown(
        self,
        tickets,
        title="## Nightly Jira BLOCKs",
    ):
        # tickets[day_obs] = {ticket_url, ...}
        mdstr = ""
        if title:
            mdstr += title

        front = (
            "https://rubinobs.atlassian.net/projects/"
            "BLOCK?selectedItem=com.atlassian.plugins."
            "atlassian-connect-plugin:com.kanoah."
            "test-manager__main-project-page#!/"
        )
        for day, url_list in tickets.items():
            mdstr += f"\n- {day}"
            for ticket_url in url_list:
                tu_str = ticket_url.replace(front, "")
                str = f"\n    - [{tu_str}]({ticket_url})"
                mdstr += str
        return mdstr


class ExposurelogReport(Report):
    def daily_observation_gap(self, min_day_obs, max_day_obs):
        pass


class NarrativelogReport(Report):
    pass


class NightObsReport(Report):
    """Generate a report that combines all sources using data from one day_obs.
    This ignores reporting that is done once no matter how many
    nights (Overview, Links).
    Generate one page for each observing night. An Observing Night includes
    the evening of day_obs and morning of the day after day_obs.
    A page includes: Almanac, NightReport, Exposure, Narrative.
    """

    pass
