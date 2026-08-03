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

"""Service for the /static-visit-map endpoint.

A healpix visit-count PNG built directly from the raw ConsDB visit
columns cached by `ConsdbVisitsAdapter`; no augmentation is needed.
"""

import base64
import functools
import logging
from io import BytesIO

import healpy as hp
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import rubin_sim.maf as maf
from rubin_nights.reference_values import SCIENCE_PROGRAMS

from lsst.ts.logging_and_reporting.adapters.consdb_visits import (
    ConsdbVisitsAdapter,
    get_consdb_visits_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

# Pin matplotlib backend.
matplotlib.use("Agg")

logger = logging.getLogger(__name__)

NSIDE = 32
RENDER_DPI = 150

COLOR_FG = "#E5E5E5"
COLOR_BG = "black"

FONT_SIZE_BODY = 13
FONT_SIZE_LABELS = 12
FONT_SIZE_TICKS = 11
GRATICULE_SPACING_DEG = 30
GRATICULE_LINEWIDTH = 1.2


def _add_dec_labels(ax) -> None:
    """Add declination labels to the healpy sky axes.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the declination labels to.
    """
    for dec_deg in (-60, -30, 0, 30, 60):
        hp.projtext(
            30,
            dec_deg,
            f"{dec_deg}°",
            lonlat=True,
            color=COLOR_FG,
            fontsize=FONT_SIZE_LABELS,
            ha="left",
            va="center",
        )


def _add_ra_labels(ax) -> None:
    """Add right ascension labels to the healpy sky axes.
    Labels are added every 60 degrees, starting at 0 and
    ending at 300.
    However, the graticules are drawn every 30 degrees,
    so there will be graticules without labels in between.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the right ascension labels to.
    """
    for ra_deg in (0, 60, 120, 180, 240, 300):
        hp.projtext(
            ra_deg,
            -8,
            f"{ra_deg}°",
            lonlat=True,
            color=COLOR_FG,
            fontsize=FONT_SIZE_LABELS,
            ha="center",
            va="top",
        )


def _add_graticules(ax) -> None:
    """Draw graticule grid lines and RA/Dec labels on the
    main healpy sky axis.

    Parameters
    ----------
    ax : `matplotlib.axes.Axes`
        The axes to add the graticules and labels to.
    """
    plt.sca(ax)
    hp.graticule(
        dpar=GRATICULE_SPACING_DEG,
        dmer=GRATICULE_SPACING_DEG,
        color=COLOR_FG,
        lw=GRATICULE_LINEWIDTH,
    )
    _add_dec_labels(ax)
    _add_ra_labels(ax)


def _style_text(fig) -> None:
    """Apply foreground colour to all text in the figure.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure containing the text objects to style.
    """
    for text in fig.findobj(match=plt.Text):
        text.set_color(COLOR_FG)


def _style_axes(fig, main_ax) -> None:
    """Apply dark theme styling to axes chrome and sky-axis outlines.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure containing the axes to style.
    main_ax : `matplotlib.axes.Axes`
        The primary sky map axes (the one containing the image).
    """
    for ax in fig.axes:
        ax.set_facecolor(COLOR_BG)
        ax.tick_params(colors=COLOR_FG)

        for spine in ax.spines.values():
            spine.set_color(COLOR_FG)

        if ax is main_ax:
            for line in ax.lines:
                line.set_color(COLOR_FG)
                line.set_linewidth(GRATICULE_LINEWIDTH)


def _style_figure(fig, main_ax) -> None:
    """Apply full dark theme to the figure.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure to style.
    main_ax : `matplotlib.axes.Axes`
        The primary sky map axes (the one containing the image).
    """
    fig.patch.set_facecolor(COLOR_BG)
    _style_text(fig)
    _style_axes(fig, main_ax)


def _compute_nvisits_bundle(map_data) -> maf.MetricBundle:
    """Run the Nvisits count metric over the provided visit data.

    Parameters
    ----------
    map_data : `array-like`
        Structured visit records containing ``ra``, ``dec``, ``sky_rotation``,
        and ``mjd`` columns.

    Returns
    -------
    bundle : `maf.MetricBundle`
        Executed metric bundle with populated metric values.
    """
    m_nvis = maf.CountMetric(col="obs_start_mjd", metric_name="Nvisits")
    slicer = maf.HealpixSlicer(
        nside=NSIDE,
        lon_col="s_ra",
        lat_col="s_dec",
        rot_sky_pos_col_name="sky_rotation",
    )
    bundle = maf.MetricBundle(
        m_nvis,
        slicer,
        "",
        plot_funcs=[maf.HealpixSkyMap()],
        plot_dict={
            "title": "",
            "percentile_clip": 98,
            "n_ticks": 7,
            "extend": "max",
            "cmap": "viridis",
            "bgcolor": COLOR_BG,
            "badcolor": COLOR_BG,
            "fontsize": FONT_SIZE_BODY,
            "labelsize": FONT_SIZE_TICKS,
        },
    )
    group = maf.MetricBundleGroup({"nvisits": bundle}, None, save_early=False)
    group.run_current("", map_data)
    return bundle


def build_static_visit_map(visits) -> bytes:
    """Build the primary static visit map.

    Not safe to call concurrently. The render goes through pyplot's
    process-global active figure and axes, which healpy and maf both
    rely on, so simultaneous calls overwrite each other's state and
    return images built from another call's figure.

    Parameters
    ----------
    visits : `pandas.DataFrame`
        Visit records used to generate the map.

    Returns
    -------
    png_bytes : `bytes`
        Static visit map image as PNG bytes.
    """

    map_data = visits[visits["science_program"].isin(SCIENCE_PROGRAMS)] if not visits.empty else visits

    if map_data.empty:
        logger.warning("No science visits available for static map generation")
        return None

    bundle = _compute_nvisits_bundle(map_data.to_records())

    plot = bundle.plot()
    fig = plt.figure(plot["SkyMap"])
    try:
        main_ax = next((ax for ax in fig.axes if ax.images), None)

        _style_figure(fig, main_ax)

        if main_ax is not None:
            _add_graticules(main_ax)

        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=RENDER_DPI, bbox_inches="tight", facecolor=COLOR_BG)
    finally:
        # pyplot holds the figure until it is closed, so an error here
        # would otherwise leak it for the life of the process.
        plt.close(fig)
    return buf.getvalue()


def _encode_png_payload(image_bytes):
    """Serialize PNG bytes into a JSON-safe payload.

    Parameters
    ----------
    image_bytes : `bytes`
        Raw bytes of the PNG image.

    Returns
    -------
    `dict` or `None`
        A dictionary containing the MIME type and base64-encoded
        data of the image, or `None` if the input is `None`.
    """
    if image_bytes is None:
        return None

    return {
        "mime_type": "image/png",
        "data": base64.b64encode(image_bytes).decode("ascii"),
    }


class StaticVisitMapService(Service):
    """Serves /static-visit-map: the healpix visit-count PNG.

    The count map reads the raw ConsDB visit columns directly, so no
    augmentation is needed.
    """

    def __init__(self, consdb_adapter: ConsdbVisitsAdapter | None = None) -> None:
        self.consdb_adapter = consdb_adapter if consdb_adapter is not None else get_consdb_visits_adapter()

    def handle(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Build the static visit map for the range.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the frontend sends
            end + 1 day).
        instrument : `str`
            Instrument to query (e.g. ``LSSTCam``).
        """
        per_day = self.consdb_adapter.fetch(
            instrument, day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        return self.collate_response(per_day)

    def collate_response(self, data: dict[int, list[dict]]) -> dict:
        rows = [record for dayobs in sorted(data) for record in data[dayobs]]
        visits = pd.DataFrame(rows)
        png_bytes = build_static_visit_map(visits) if not visits.empty else None
        return {"static_map": _encode_png_payload(png_bytes)}


@functools.cache
def get_static_visit_map_service() -> StaticVisitMapService:
    return StaticVisitMapService()
