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
# #############################################################################
import logging
from urllib.parse import urlencode

import lsst.ts.logging_and_reporting.utils as utils
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter

logger = logging.getLogger(__name__)


class ExposurelogAdapter(SourceAdapter):
    service = "exposurelog"
    primary_endpoint = "messages?"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # Inclusive
        max_dayobs=None,  # Exclusive
        limit=None,
        verbose=False,
        warning=False,
        auth_token=None,
    ):
        default_record_limit = 2500  # Adapter specific default
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,  # handled here not in the function
            min_dayobs=min_dayobs,
            limit=limit or default_record_limit,
            verbose=verbose,
            warning=warning,
            auth_token=auth_token,
        )
        # The main point of us having a class here is to use SourceAdapter
        # to handle our authentication, and using protected post/get

    @property
    def sources(self):
        return {"Exposure Log API": "/".join([self.server, self.service])}

    def get_messages(
        self,
        instrument,
        is_human=None,
        order_by=None,
        offset=None,
        limit=None,
        **optional_params,
    ):
        parameters = dict(
            instrument=instrument,
            is_human=is_human if is_human is not None else "true",
            order_by=order_by or "-date_added",
            offset=offset,
            limit=limit or self.limit,
        )
        parameters.update(optional_params)

        if self.min_dayobs:
            parameters["min_day_obs"] = utils.dayobs_int(self.min_dayobs)
        # else, we get the most recent messages, up to limit
        if self.max_dayobs:
            parameters["max_day_obs"] = utils.dayobs_int(self.max_dayobs)
        # else, get most recent messages, up to limit, before max dayobs

        endpoint = "/".join([self.sources["Exposure Log API"], self.primary_endpoint])
        filtered_params = {key: val for key, val in parameters.items() if val is not None}
        endpoint += urlencode(filtered_params)

        # TODO: pool paginate
        # Protected get returns a tuple...
        status, messages, code = self.protected_get(endpoint)
        if code != 200:
            logger.warning(f"Error {code} getting exposurelog messages from {endpoint}")

        for entry in messages:
            # May just have a message.
            if entry.get("exposure_flag") == "none":
                entry["exposure_flag"] = "unknown"

        return messages
