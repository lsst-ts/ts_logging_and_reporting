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


import matplotlib.pyplot as plt
from lsst.summit.utils import ConsDbClient


class ConsDbConnection:
    """Create and manage the connection to the Consolidated Database
    including knowledge of the schemas and instruments"""

    def __init__(self, url, day_obs):
        self.url = url
        self.day_obs = day_obs
        self.day_obs_int = int(day_obs.replace("-", ""))
        self.client = ConsDbClient(url)

    def query_visit(self, instrument: str, type: str = "visit1"):
        """Query visit1 and visit1_quicklook tables and join the data on
        visit_id, type can also be ccdvisit1"""
        visit1 = f"""SELECT * FROM cdb_{instrument}.{type}
             where day_obs = {self.day_obs_int}"""
        ccdvisit1_quicklook = f"SELECT * FROM cdb_{instrument}.{type}_quicklook"

        try:
            visits = self.client.query(visit1)
            quicklook = self.client.query(ccdvisit1_quicklook)
        except Exception as erry:
            print(f"{erry=}")

        # Join both on visit_id so we can access obs_start for a time axis
        return visits.join(quicklook, on="visit_id", lsuffix="", rsuffix="_q")

    def query_exposure(self, instrument: str, type: str = "exposure"):
        """Query exposure table and return data,
        Type may also be ccdexposure"""
        exposure_query = f"""SELECT * FROM cdb_{instrument}.{type}
             where day_obs = {self.day_obs_int}"""
        try:
            exposures = self.client.query(exposure_query)
        except Exception as erry:
            print(f"{erry=}")

        return exposures


def plot(y, x):
    """Plot the given x and y data."""
    fig = plt.figure(figsize=(6, 6))
    ax = fig.subplots()
    ax.scatter(x, y)
    plt.show()


def plot_ra_dec(y, x):
    """Plot the given x and y data."""
    fig = plt.figure(figsize=(6, 6))
    ax = fig.subplots()
    ax.scatter(x, y)
    plt.show()


URL = "http://consdb-pq.consdb:8080/consdb"
day_obs = "2024-06-26"
instruments = "latiss, lsstcomcamsim, lsstcomcam"

for instrument in instruments:
    db_client = ConsDbConnection(URL, day_obs)
    visits = db_client.query_visit(instrument=instrument)
    exposures = db_client.query_exposure(instrument=instrument)

    # This is our time axis for each visit
    obs_start = visits["obs_start"]

    psf_area = visits["psf_area"]
    plot(psf_area, obs_start)
    sky_bg = visits["sky_bg"]
    plot(sky_bg, obs_start)
    zero_point = visits["zero_point"]
    plot(zero_point, obs_start)

    ra = exposures["s_ra"]
    dec = exposures["s_dec"]
    plot_ra_dec(dec, ra)
