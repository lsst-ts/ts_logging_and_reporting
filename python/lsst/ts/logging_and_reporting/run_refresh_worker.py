#
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

"""Entrypoint for the cache refresh worker process.

Runs one `RefreshWorker`, writing to the same Redis cache the API
service reads. Deploy exactly one instance: nothing in the
process coordinates with its peers, so a second instance just
duplicates upstream fetches.
"""

import logging
import os
import signal
from types import FrameType

from lsst.ts.logging_and_reporting import adapters
from lsst.ts.logging_and_reporting.redis_client import (
    DISABLE_ENV_VAR,
    redis_caching_disabled,
)

from .refresh_worker import RefreshWorker

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_refresh_worker() -> None:
    """Run the refresh worker until SIGTERM or SIGINT."""

    if redis_caching_disabled():
        # Nothing to warm: every entry the worker wrote would be
        # dropped, leaving only the upstream load.
        logger.warning(
            f"{DISABLE_ENV_VAR} is set; refresh worker has no cache to warm, exiting"
        )
        return

    worker = RefreshWorker(
        [
            adapters.get_consdb_exposures_adapter(),
            # The visit_overhead adapter reads data from the consdb_exposures
            # so deliberate cycle ordering ensures it reads fresh data.
            adapters.get_visit_overhead_adapter(),
            adapters.get_consdb_visits_adapter(),
            adapters.get_expected_exposures_adapter(),
            adapters.get_exposurelog_adapter(),
            adapters.get_jira_obs_adapter(),
            adapters.get_narrativelog_adapter(),
            adapters.get_nightreport_adapter(),
            adapters.get_rubin_nights_context_adapter(),
            adapters.get_rubin_nights_dome_adapter(),
            adapters.get_rubin_nights_obs_status_adapter(),
        ]
    )

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        # Handle graceful shutdown
        logger.info(
            f"Received signal {signal.Signals(signum).name}, stopping refresh worker"
        )
        worker.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    worker.run()


if __name__ == "__main__":
    run_refresh_worker()
