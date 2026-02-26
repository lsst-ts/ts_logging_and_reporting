"""Utility for building visualizations of Rubin Observatory visits.

Utility for creating interactive sky‑map visualisations of Rubin Observatory
observing visits using the **uranography** package.  The module
defines builder class `VisitMapBuilder` following the "fluent builder"
pattern: a caller instantiates a `VisitMapBuilder` object, chains
configuration methods, and ultimately produces a `bokeh.models.UIElement`
that can be displayed in a report, dashboard, or other interface.
"""

import warnings
from types import MethodType
from typing import Any, Callable, Dict, List, Optional, Self, Tuple

import bokeh
import bokeh.layouts
import bokeh.models
import bokeh.palettes
import bokeh.plotting
import bokeh.transform
import healpy as hp
import numpy as np
import pandas as pd
from astropy.coordinates import ICRS, get_body
from astropy.time import Time
from schedview.collect import load_bright_stars
from schedview.compute.camera import LsstCameraFootprintPerimeter
from schedview.compute.footprint import find_healpix_area_polygons
from schedview.plot import PLOT_BAND_COLORS
from uranography.api import ArmillarySphere, Planisphere, SphereMap, split_healpix_by_resolution

DEFAULT_VISIT_TOOLTIPS = (
    "@observationId: @start_timestamp{%F %T} UTC (mjd=@observationStartMJD{00000.0000}, "
    + "LST=@observationStartLST\u00b0): "
    + "@observation_reason (@science_program), "
    + "in @band at \u03b1,\u03b4=@fieldRA\u00b0,@fieldDec\u00b0; "
    + "q=@paraAngle\u00b0; a,A=@azimuth\u00b0,@altitude\u00b0"
)

SPHEREMAP_FIGURE_KWARGS = {"match_aspect": True}


class VisitMapBuilder:
    """Builder for interactive visit sky‑maps.

    The ``VisitMapBuilder`` class follows the fluent builder pattern:
    a ``VisitMapBuilder`` instance is created from a ``pandas.DataFrame`` of
    Rubit Observatory visits. Callers then call series of chained
    configuration methods to customise the visualisation.
    The ``build`` method can then be used to return a `bokeh.models.UIElement`
    instance that can be embedded in report, notebook, or dashboard.

    Parameters
    ----------
    visits : `pandas.DataFrame`
        Table of visits.  At minimum the DataFrame must contain the columns
        listed below.  Columns can be renamed by overriding the
        corresponding class attributes (e.g. ``VisitMapBuilder.ra_column``).

        Required columns
        ^^^^^^^^^^^^^^^^
        ``fieldRA`` : ``float``
            Right‑ascension of the field pointing (degrees).
        ``fieldDec`` : ``float``
            Declination of the field pointing (degrees).
        ``observationStartMJD`` : ``float``
            Start time of the exposure as a Modified Julian Date.
        ``band`` : ``str``
            Photometric band (e.g. ``u``, ``g``, ``r``, ``i``, ``z`` or ``y``).
        ``rotSkyPos`` : ``float``
            Camera rotation angle (degrees).

    mjd : float, optional
        Reference MJD for the underlying ``uranography`` maps.  If omitted the
        maximum ``observationStartMJD`` in *visits* is used; if *visits* is
        empty the current time is used.

    map_classes : list, optional
        List of ``uranography`` map classes to instantiate.  By default an
        :class:`~uranography.api.ArmillarySphere` and a
        :class:`~uranography.api.Planisphere` are created.

    camera_perimeter : callable, optional
        Function that returns camera‑footprint edge coordinates given arrays of
        ``fieldRA``, ``fieldDec`` and ``rotSkyPos``.  The default is
        :class:`~schedview.compute.camera.LsstCameraFootprintPerimeter`.

    visit_fill_colors : Dict[str, str], optional
        Colors for each of the bands. The default is None, which causes plots
        to default to using ``schedview.plot.PLOT_BAND_COLORS``.

    figure_kwargs : Dict[str, Asy], optional
        Keword arguments with which to instantiate `bokeh.plotting.figure`
        instances for each spheremap. The default is None, which causes plots
        to default to using ``SPHEREMAP_FIGURE_KWARGS``.

    Notes
    -----
    * The builder mutates its internal ``spheremaps`` list in‑place; each
      ``add_`` method returns ``self`` to enable fluent chaining.
    * Configuration methods such as `add_graticules` and `add_horizon`
      are thin wrappers around the corresponding ``uranography`` API calls.

    Examples
    --------

    >>> from uranography.api import ArmillarySphere, Planisphere
    >>> import bokeh.io
    >>> # Assume ``visits`` is a pandas.DataFrame with the required columns
    >>> builder = (
    ...     VisitMapBuilder(
    ...         visits,
    ...         mjd=visits['observationStartMJD'].max(),
    ...         map_classes=[ArmillarySphere, Planisphere]
    ...     )
    ...     .add_visit_patches()
    ...     .add_graticules()
    ...     .add_ecliptic()
    ...     .add_galactic_plane()
    ...     .add_datetime_slider()
    ...     .add_eq_sliders()
    ...     .hide_future_visits()
    ...     .highlight_recent_visits()
    ...     .add_body('sun', size=15, color='yellow', alpha=1.0)
    ...     .add_body('moon', size=15, color='orange', alpha=0.8)
    ...     .add_horizon()
    ...     .add_horizon(zd=70, color='red')
    ... )
    ...
    >>> # Build the Bokeh layout
    >>> viewable = builder.build()
    >>> # Write the layout to a standalone HTML file
    >>> bokeh.io.save(viewable, filename="visit_skymap.html")
    """

    mjd_column: str = "observationStartMJD"
    ra_column: str = "fieldRA"
    decl_column: str = "fieldDec"
    rot_column: str = "rotSkyPos"
    band_column: str = "band"
    recent_max: float = 1.0
    recent_fade_scale: float = 2.0 / (24 * 60)
    visit_columns: List[str] = [
        "observationId",
        "start_timestamp",
        "observationStartMJD",
        "observationStartLST",
        "band",
        "fieldRA",
        "fieldDec",
        "rotSkyPos",
        "paraAngle",
        "azimuth",
        "altitude",
        "observation_reason",
        "science_program",
    ]

    def __init__(
        self,
        visits: pd.DataFrame | None = None,
        mjd: Optional[float] = None,
        map_classes: List[SphereMap] = [ArmillarySphere, Planisphere],
        camera_perimeter: Optional[
            Callable[[pd.Series, pd.Series, pd.Series], Tuple[np.ndarray, np.ndarray]]
        ] = None,
        visit_fill_colors: Optional[Dict[str, str]] = None,
        figure_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.figure_kwargs: Dict[str, Any] = (
            SPHEREMAP_FIGURE_KWARGS if figure_kwargs is None else figure_kwargs
        )
        self.visit_fill_colors: Dict[str, str] = (
            PLOT_BAND_COLORS if visit_fill_colors is None else visit_fill_colors
        )
        self.visits = visits

        # If the mjd is not set by the caller, guess it from the visits
        # if there are any, and if there are not, assume "now".
        if mjd is None:
            if len(self.visits) > 0:
                self.mjd = visits[self.mjd_column].max() if mjd is None else mjd
            else:
                self.mjd = Time.now().mjd.item()
        else:
            self.mjd = mjd

        self.instantiate_spheremaps(map_classes)

        self.camera_perimeter = (
            camera_perimeter if camera_perimeter is not None else LsstCameraFootprintPerimeter()
        )

        self.visits_ds = None
        self.visit_patches_added = False
        self._add_mjd_slider()
        self.body_ds: Dict[str, bokeh.models.ColumnDataSource] = {}
        self.horizon_ds: Dict[float, bokeh.models.ColumnDataSource] = {}
        self.star_ds: Optional[bokeh.models.ColumnDataSource] = None
        self.healpix_high_ds: Optional[bokeh.models.ColumnDataSource] = None
        self.healpix_low_ds: Optional[bokeh.models.ColumnDataSource] = None

    def instantiate_spheremaps(self, map_classes: List[SphereMap]) -> None:
        """Instantiate spheremap objects for each class in map_classes.

        This method creates spheremap instances for each class provided in the
        `map_classes` list and stores them in `self.spheremaps`. The first
        spheremap instance is also stored in `self.ref_map` for reference.

        Parameters
        ----------
        map_classes : `list` [`uranography.api.SphereMap`]
            A list of spheremap class constructors (e.g., ``ArmillarySphere``,
            ``Planisphere``) to instantiate. Each class should accept a ``mjd``
            keyword argument.

        Notes
        -----
        * This method is separated from `__init__` to allow subclasses to
          override it and customize the spheremap instantiation process.
        * The `self.spheremaps` list is populated with instances of the
          provided classes, and `self.ref_map` is set to the first instance.
        """
        # Separated into its own method so that it can be easily modified in
        # subclasses.
        # For example, a subclass could generate instances of bokeh figure to
        # pass to each spheremap with customized parameters.
        self.spheremaps = [
            mc(mjd=self.mjd, plot=bokeh.plotting.figure(**self.figure_kwargs)) for mc in map_classes
        ]
        self.ref_map = self.spheremaps[0]

    def add_visit_patches(self, visits: pd.DataFrame | None = None) -> Self:
        """Add visit patches to the map.

        Parameters
        ----------
        visits : `pandas.DataFrame` or `None`
            Table of visits.  At minimum the DataFrame must contain the columns
            listed below.  Columns can be renamed by overriding the
            corresponding class attributes
            (e.g. ``VisitMapBuilder.ra_column``).

            Required columns
            ^^^^^^^^^^^^^^^^
            ``fieldRA`` : ``float``
                Right‑ascension of the field pointing (degrees).
            ``fieldDec`` : ``float``
                Declination of the field pointing (degrees).
            ``observationStartMJD`` : ``float``
                Start time of the exposure as a Modified Julian Date.
            ``band`` : ``str``
                Photometric band (e.g. ``u``, ``g``, ``r``, ``i``, ``z``
                or ``y``).
            ``rotSkyPos`` : ``float``
                Camera rotation angle (degrees).

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """

        if visits is not None:
            if self.visits is not None:
                raise ValueError("visits already supplied.")
            self.visits = visits

        if self.visits is None:
            warnings.warn("No visits to show.")
            return self

        present_visit_columns = [c for c in self.visit_columns if c in self.visits.columns]
        for band in "ugrizy":
            in_band_mask = self.visits[self.band_column].values == band
            band_visits = self.visits.reset_index().loc[in_band_mask, present_visit_columns].copy()

            if len(band_visits) < 1:
                continue

            ras, decls = self.camera_perimeter(
                band_visits[self.ra_column], band_visits[self.decl_column], band_visits[self.rot_column]
            )
            band_visits = band_visits.assign(ra=ras, decl=decls, mjd=band_visits[self.mjd_column].values)

            patches_kwargs = {"name": "visit_patches", "fill_color": self.visit_fill_colors[band]}

            self.visit_ds = self.ref_map.add_patches(
                band_visits,
                patches_kwargs=patches_kwargs,
            )

            for spheremap in self.spheremaps[1:]:
                spheremap.add_patches(data_source=self.visit_ds, patches_kwargs=patches_kwargs)

        self.visit_patches_added = True

        return self

    def _add_mjd_slider(
        self, visible: bool = True, start: Optional[float] = None, end: Optional[float] = None
    ) -> Self:
        """Add a slider to control the Date (MJD) of the visualization.

        This method adds an MJD slider to the visualization that allows users
        to control the time displayed in the sky map. The slider is linked
        across all spheremaps in the builder.

        Parameters
        ----------
        visible : `bool`, optional
            Whether the slider is visible in the visualization, by default True
        start : `float`, optional
            The start value for the slider range. If None, uses the minimum
            MJD value from the visits DataFrame, by default None
        end : `float`, optional
            The end value for the slider range. If None, uses the maximum
            MJD value from the visits DataFrame, by default None

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """
        if "mjd" not in self.ref_map.sliders:
            self.ref_map.add_mjd_slider()

        self.mjd_slider = self.ref_map.sliders["mjd"]

        self.mjd_slider.visible = visible
        if start is not None:
            self.mjd_slider.start = start
        elif self.visits is not None:
            self.mjd_slider.start = self.visits[self.mjd_column].min()
        else:
            self.mjd_slider.start = Time.now().mjd - 1

        if end is not None:
            self.mjd_slider.end = end
        elif self.visits is not None:
            self.mjd_slider.end = self.visits[self.mjd_column].max()
        else:
            self.mjd_slider.end = Time.now().mjd

        for spheremap in self.spheremaps[1:]:
            spheremap.sliders["mjd"] = self.mjd_slider

        # Support method chaining
        return self

    def hide_mjd_slider(self) -> Self:
        """Hide any mjd sliders.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """
        for spheremap in self.spheremaps:
            if "mjd" in spheremap.sliders:
                spheremap.sliders["mjd"].visible = False

        return self

    def add_datetime_slider(self, visible: bool = True, *args: Any, **kwargs: Any) -> Self:
        """Add a datetime slider linked to the (maybe invisible) MJD slider.

        Parameters
        ----------
        visible : `bool`, optional
            Whether the slider is visible in the visualization, by default True
        *args
            Additional positional arguments passed to the underlying
            uranography datetime slider implementation.
        **kwargs
            Additional keyword arguments passed to the underlying uranography
            datetime slider implementation.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns self to support method chaining.

        Notes
        -----

        The datetime slider controls the same MJD value as the underlying MJD
        slider, but displays it in a more traditional date/time format.
        """
        self.ref_map.add_datetime_slider()
        return self

    def _hide_visits_by_mjd(self, hide_js: str, show_alpha: float = 0.5, hide_alpha: float = 0.0) -> Self:
        """Hide visits based on MJD.

        Parameters
        ----------
        hide_js : `str`
            Javascript that shows or hides visits based on mjd.
        show_alpha: `float`
            The alpha of shown visits.
        hide_alpha: `float`
            The alpha of hidden visits.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable method chaining.
        """

        if "mjd" not in self.ref_map.sliders:
            # The slider must exist for this feature to work, but if we
            # have not yet explicitly added it, make it invisible.
            self.ref_map.add_mjd_slider(visible=False)

        past_future_transform = bokeh.models.CustomJSTransform(
            args=dict(
                mjd_slider=self.ref_map.sliders["mjd"],
                show_value=show_alpha,
                hide_value=hide_alpha,
            ),
            v_func=hide_js,
        )

        # Apply the transform to the visit patches
        try:
            for spheremap in self.spheremaps:
                visit_renderers = spheremap.plot.select(name="visit_patches")
                if visit_renderers:
                    for renderer in visit_renderers:
                        renderer.glyph.fill_alpha = bokeh.transform.transform("mjd", past_future_transform)
        except Exception as e:
            warnings.warn(f"Could not apply hide_future_visits transform: {e}")
            return self

        return self

    def hide_future_visits(self) -> Self:
        """Hide visits that occur after the MJD value set by the slider.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable method chaining.

        Notes
        -----
        * If the MJD slider has not yet been added to the reference map,
        this method adds an invisible MJD slider before creating the
        transform.
        """
        if not self.visit_patches_added:
            raise ValueError("add_visit_patches must be called before hide_future_visits")

        # Transforms for recent, past, future visits
        past_future_js = """
            const result = new Array(xs.length)
            for (let i = 0; i < xs.length; i++) {
            if (mjd_slider.value >= xs[i]) {
                result[i] = show_value
            } else {
                result[i] = hide_value
            }
            }
            return result
        """

        self._hide_visits_by_mjd(past_future_js)
        return self

    def hide_future_and_other_night_visits(self) -> Self:
        """Hide visits that occur after the MJD value set by the slider.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable method chaining.
        """
        # Transforms for recent, past, future visits
        # The "- 0.5" in the floor causes it to use the same night rollover
        # as dayobs.
        past_future_js = """
            const result = new Array(xs.length)
            for (let i = 0; i < xs.length; i++) {
            if ((mjd_slider.value >= xs[i])
                && (Math.floor(mjd_slider.value - 0.5) == Math.floor(xs[i] - 0.5))) {
                result[i] = show_value
            } else {
                result[i] = hide_value
            }
            }
            return result
        """

        self._hide_visits_by_mjd(past_future_js)
        return self

    def highlight_recent_visits(self) -> Self:
        """Highlight recent visits based on the MJD slider.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        recent_js = """
            const result = new Array(xs.length)
            for (let i = 0; i < xs.length; i++) {
            if (mjd_slider.value < xs[i]) {
                result[i] = 0
            } else {
                result[i] = Math.max(0, max_value * (1 - (mjd_slider.value - xs[i]) / scale))
            }
            }
            return result
        """

        if not self.visit_patches_added:
            raise ValueError("add_visit_patches must be called before highlight_recent_visits")

        if "mjd" not in self.ref_map.sliders:
            # The slider must exist for this feature to work, but if we
            # have not yet explicitly added it, make it invisible.
            self.ref_map.add_mjd_slider(visible=False)

        recent_transform = bokeh.models.CustomJSTransform(
            args=dict(
                mjd_slider=self.ref_map.sliders["mjd"],
                max_value=self.recent_max,
                scale=self.recent_fade_scale,
            ),
            v_func=recent_js,
        )

        # Apply the transform to the visit patches
        try:
            for spheremap in self.spheremaps:
                visit_renderers = spheremap.plot.select(name="visit_patches")
                if visit_renderers:
                    for renderer in visit_renderers:
                        renderer.glyph.line_alpha = bokeh.transform.transform("mjd", recent_transform)
                        renderer.glyph.line_color = "#ff00ff"
                        renderer.glyph.line_width = 2
        except Exception as e:
            warnings.warn(f"Could not apply highlight_recent_visits transform: {e}")
            return self

        return self

    def decorate(self) -> Self:
        """Add default decorations (graticules, ecliptic, and GP) to all maps.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        for spheremap in self.spheremaps:
            spheremap.decorate()

        return self

    def add_ecliptic(self, *args: Any, **kwargs: Any) -> Self:
        """Add the ecliptic to all maps.

        Parameters
        ----------
        *args
            Additional positional arguments forwarded to the underlying
            ``add_ecliptic`` method of each ``SphereMap``.
        **kwargs
            Additional keyword arguments forwarded to the underlying
            ``add_ecliptic`` method of each ``SphereMap``.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        for spheremap in self.spheremaps:
            spheremap.add_ecliptic(*args, **kwargs)

        return self

    def add_galactic_plane(self, *args: Any, **kwargs: Any) -> Self:
        """Add the galactic plane to all maps.

        Parameters
        ----------
        *args
            Additional positional arguments forwarded to the underlying
            ``add_galactic_plane`` method of each ``SphereMap``.
        **kwargs
            Additional keyword arguments forwarded to the underlying
            ``add_galactic_plane`` method of each ``SphereMap``.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        for spheremap in self.spheremaps:
            spheremap.add_galactic_plane(*args, **kwargs)

        return self

    def add_graticules(self, *args: Any, **kwargs: Any) -> Self:
        """Add graticules (RA/Dec grid lines) to all maps.

        Parameters
        ----------
        *args
            Additional positional arguments forwarded to the underlying
            ``add_graticules`` method of each ``SphereMap``.
        **kwargs
            Additional keyword arguments forwarded to the underlying
            ``add_graticules`` method of each ``SphereMap``.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        for spheremap in self.spheremaps:
            spheremap.add_graticules(*args, **kwargs)

        return self

    def add_body(self, body: str, size: float, color: str, alpha: float, time_step: float = 1.0) -> Self:
        """Add a celestial‑body marker to the reference map.

        Parameters
        ----------
        body : `str`
            Name of the solar system body (e.g. ``'sun'`` or ``'moon'``).
        size : `float`
            Marker size in screen pixels.
        color : `str`
            Color of the marker (any valid Bokeh color string).
        alpha : `float`
            Opacity of the marker (0.0‑1.0).
        time_step: `float`
            The time between "updates" to the position with time, in days.

        Returns
        -------
        self : `VisitMapBuilder`
            The builder instance to allow method chaining.
        """

        hide_js = """
            const result = new Array(xs.length)
            for (let i = 0; i < xs.length; i++) {
                if ((mjd_slider.value >= min_mjd) && (mjd_slider.value < max_mjd)) {
                    result[i] = show_value
                } else {
                    result[i] = hide_value
                }
            }
            return result
        """

        if time_step > self.mjd_slider.end - self.mjd_slider.start:
            mjds = [(self.mjd_slider.end + self.mjd_slider.start) / 2]
        else:
            start_mjd: float = self.mjd_slider.start - time_step
            end_mjd: float = self.mjd_slider.end + time_step
            first_mjd: float = start_mjd + time_step / 2
            last_mjd: float = end_mjd + time_step / 2
            mjds = np.arange(first_mjd, last_mjd, time_step)

        for mjd in mjds:
            ap_time = Time(mjd, format="mjd", scale="utc")
            body_name = body if len(mjds) == 1 else body + ap_time.strftime("%Y%m%d%H%M%S")
            min_mjd = mjd - time_step / 2
            max_mjd = mjd + time_step / 2

            body_coords = get_body(body, ap_time).transform_to(ICRS())

            hide_js_transform = bokeh.models.CustomJSTransform(
                args=dict(
                    mjd_slider=self.ref_map.sliders["mjd"],
                    min_mjd=min_mjd,
                    max_mjd=max_mjd,
                    show_value=alpha,
                    hide_value=0,
                ),
                v_func=hide_js,
            )

            hide_transform = bokeh.transform.transform("decl", hide_js_transform)

            circle_kwargs = {
                "color": color,
                "alpha": hide_transform,
                "name": body,
            }

            self.body_ds[body_name] = self.ref_map.add_marker(
                ra=body_coords.ra.deg,
                decl=body_coords.dec.deg,
                name=body_name,
                glyph_size=size,
                circle_kwargs=circle_kwargs,
            )

            for spheremap in self.spheremaps[1:]:
                spheremap.add_marker(
                    data_source=self.body_ds[body_name],
                    name=body_name,
                    glyph_size=size,
                    circle_kwargs=circle_kwargs,
                )

        return self

    def add_horizon(self, zd: float = 90, **line_kwargs: Any) -> Self:
        """Add a horizon line to all maps.

        Parameters
        ----------
        zd : `float`, optional
            The zenith distance of the horizon line in degrees. Default is 90
            degrees (the horizon).
        **line_kwargs
            Additional keyword arguments passed to the underlying
            ``add_horizon`` method of each ``SphereMap``.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns ``self`` to enable fluent method chaining.
        """
        data_source = self.ref_map.add_horizon(zd=zd, line_kwargs=line_kwargs)
        for spheremap in self.spheremaps[1:]:
            spheremap.add_horizon(data_source=data_source, line_kwargs=line_kwargs)

        self.horizon_ds[zd] = data_source

        return self

    def add_stars(self, star_data: Optional[pd.DataFrame] = None) -> Self:
        """Add star markers to all maps.

        Parameters
        ----------
        star_data : `pandas.DataFrame`, optional
            DataFrame containing star data with columns "name", "ra", "decl",
            and "Vmag".
            If None, bright stars are loaded using `load_bright_stars()`.
            Default is None.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns self to enable method chaining.
        """
        if star_data is None:
            star_data = load_bright_stars().loc[:, ["name", "ra", "decl", "Vmag"]]
        assert isinstance(star_data, pd.DataFrame)

        star_data["glyph_size"] = 15 - (15.0 / 3.5) * star_data["Vmag"]
        star_data.query("glyph_size>0", inplace=True)
        self.star_ds = self.ref_map.add_stars(
            star_data, mag_limit_slider=False, star_kwargs={"color": "yellow"}
        )

        for spheremap in self.spheremaps[1:]:
            spheremap.add_stars(
                star_data,
                data_source=self.star_ds,
                mag_limit_slider=True,
                star_kwargs={"color": "yellow"},
            )
        return self

    def add_hovertext(self, visit_tooltips: Optional[str] = None) -> Self:
        """Add hover tooltips to visit patches.

        Parameters
        ----------
        visit_tooltips : `str`, optional
            The tooltip format string. If None, the default tooltip format
            defined in DEFAULT_VISIT_TOOLTIPS is used. Default is None.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns self to enable method chaining.
        """
        if visit_tooltips is None:
            visit_tooltips = DEFAULT_VISIT_TOOLTIPS

        for spheremap in self.spheremaps:
            hover_tool = bokeh.models.HoverTool()
            hover_tool.renderers = list(spheremap.plot.select({"name": "visit_patches"}))
            hover_tool.tooltips = visit_tooltips
            hover_tool.formatters = {"@start_timestamp": "datetime"}
            spheremap.plot.add_tools(hover_tool)
        return self

    def add_footprint(self, footprint: np.ndarray, nside_low: int = 8) -> Self:
        """Add the LSST survey footprint to the sky maps.

        Parameters
        ----------
        footprint : `numpy.ndarray`
            A healpix map of the footprint.
        nside_low : `int`, optional
            The nside value for the low resolution component. Default is 8.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns self to enable method chaining.

        Notes
        -----
        Full healpix maps can make the full plots very large, resulting in
        long transfer times and large files.
        """
        cmap = bokeh.transform.linear_cmap("value", "Greys256", int(np.ceil(np.nanmax(footprint) * 2)), 0)

        nside_high = hp.npix2nside(footprint.shape[0])
        footprint_high, footprint_low = split_healpix_by_resolution(footprint, nside_low, nside_high)

        self.healpix_high_ds, cmap, glyph = self.ref_map.add_healpix(
            footprint_high, nside=nside_high, cmap=cmap, name="footprint_high"
        )
        self.healpix_low_ds, cmap, glyph = self.ref_map.add_healpix(
            footprint_low, nside=nside_low, cmap=cmap, name="footprint_low"
        )

        for spheremap in self.spheremaps[1:]:
            spheremap.add_healpix(self.healpix_high_ds, nside=nside_high, cmap=cmap, name="footprint_high")
            spheremap.add_healpix(self.healpix_low_ds, nside=nside_low, cmap=cmap, name="footprint_low")

        for spheremap in self.spheremaps:
            spheremap.connect_controls(self.healpix_high_ds)
            spheremap.connect_controls(self.healpix_low_ds)

        return self

    @staticmethod
    def _compute_footprint_outlines(footprint: np.ndarray) -> pd.DataFrame:
        footprint_regions = footprint.copy()
        footprint_regions[np.isin(footprint_regions, ["bulgy", "lowdust"])] = "WFD"
        footprint_regions[
            np.isin(footprint_regions, ["LMC_SMC", "dusty_plane", "euclid_overlap", "nes", "scp", "virgo"])
        ] = "other"

        # Get rid of tiny little loops
        footprint_outline = find_healpix_area_polygons(footprint_regions)
        tiny_loops = footprint_outline.groupby(["region", "loop"]).count().query("RA<10").index
        footprint_outline = footprint_outline.drop(tiny_loops)
        return footprint_outline

    def add_footprint_outlines(
        self,
        footprint: np.ndarray,
        colormap: Optional[Dict[str, str]] = None,
        filled: bool = False,
        **line_kwargs: Any,
    ) -> Self:
        """Add footprint outlines to the sky maps.

        Parameters
        ----------
        footprint : `numpy.ndarray`
            A HEALPix map of the survey footprint, where each pixel contains
            a region identifier.
        colormap : `dict` [`str`, `str`], optional
            Dictionary mapping region names to colors. If None, a default
            color palette is automatically generated based on the number of
            unique regions in the footprint. Default is None.
        filled : `bool`, optional
            If True, fill the regions with color instead of drawing outlines.
            If False, draw only the outline of each region. Default is False.
        **line_kwargs
            Additional keyword arguments passed to the underlying Bokeh
            plotting methods (``plot.patch`` for filled regions or
            ``plot.line`` for outlines). These can include line width, alpha,
            etc.

        Returns
        -------
        self : `VisitMapBuilder`
            Returns self to enable method chaining.

        Notes
        -----
        * When `filled=True`, the filled regions may not render correctly if
          the polygon crosses a projection discontinuity.
        * The footprint regions are defined by the values in the input
          `footprint` array. Special regions like "bulgy", "lowdust", etc. are
          grouped into "WFD" (Wide, Deep, and Fast), and other regions are
          grouped into "other".
        """
        footprint_polygons = self._compute_footprint_outlines(footprint)

        if filled:
            warnings.warn(
                'The "filled" option does work correctly when the polygon crosses a projection discontinuity.'
            )

        if "region" not in footprint_polygons.index.names:
            footprint_polygons = (
                footprint_polygons.assign(region="only").set_index("region", append=True).copy()
            )

        if "loop" not in footprint_polygons.index.names:
            footprint_polygons = footprint_polygons.assign(loop=0).set_index("loop", append=True).copy()

        footprint_polygons = footprint_polygons.reorder_levels(["region", "loop"]).copy()

        outside = ""
        if colormap is None:
            regions = [
                r for r in footprint_polygons.index.get_level_values("region").unique() if r != outside
            ]
            if len(regions) == 1:
                palette = ["black"]
            elif len(regions) == 2:
                palette = ["black", "darkgray"]
            else:
                # Try palettes from the "colorblind" section in the bokeh docs
                try:
                    palette = bokeh.palettes.Colorblind[len(regions)]
                except KeyError:
                    palette = bokeh.palettes.TolRainbow[len(regions)]

            colormap = {r: c for r, c in zip(regions, palette)}

        footprint_regions = {}
        for region_name, loop_id in set(footprint_polygons.index.values.tolist()):
            if region_name not in footprint_regions:
                footprint_regions[region_name] = {}
            footprint_regions[region_name][loop_id] = (
                footprint_polygons.loc[(region_name, loop_id), ["RA", "decl"]]
                .apply(tuple, axis="columns")
                .tolist()
            )

        for region_name in footprint_regions:
            if region_name == outside:
                continue

            for this_loop in footprint_regions[region_name].values():
                line_kwargs["name"] = "footprint_outline"
                loop_ds = bokeh.models.ColumnDataSource({"coords": this_loop})
                for spheremap in self.spheremaps:
                    if filled:
                        spheremap.plot.patch(
                            spheremap.x_transform("coords"),
                            spheremap.y_transform("coords"),
                            color=colormap[region_name],
                            source=loop_ds,
                            **line_kwargs,
                        )
                    else:
                        spheremap.plot.line(
                            spheremap.x_transform("coords"),
                            spheremap.y_transform("coords"),
                            color=colormap[region_name],
                            source=loop_ds,
                            **line_kwargs,
                        )
                    spheremap.connect_controls(loop_ds)

        return self

    def add_eq_sliders(self) -> Self:
        """Add sliders for setting the center using equatorial coordinates.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """

        for spheremap in self.spheremaps:
            # Only call on maps for which it applies
            maybe_add_eq_sliders_method = getattr(spheremap, "add_eq_sliders", None)
            if isinstance(maybe_add_eq_sliders_method, MethodType):
                spheremap.add_eq_sliders()

        return self

    def hide_horizon_sliders(self) -> Self:
        """Set horizon sliders to not be visible.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """
        for spheremap in self.spheremaps:
            for coord in ("alt", "az"):
                if coord in spheremap.sliders:
                    spheremap.sliders[coord].visible = False

        return self

    def make_up_north(self) -> Self:
        """Set "up" in the maps to be north.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """

        for spheremap in self.spheremaps:
            if "up" in spheremap.sliders:
                spheremap.sliders["up"].value = "north is up"

        return self

    def show_up_selector(self) -> Self:
        """Make the selector for the orientation visible.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """

        for spheremap in self.spheremaps:
            if "up" in spheremap.sliders:
                spheremap.sliders["up"].visible = True

        return self

    def hide_up_selector(self) -> Self:
        """Make the selector for the orientation invisible.

        Returns
        -------
        self: `VisitMapBuilder`
            Returns self to support method chaining.
        """

        for spheremap in self.spheremaps:
            if "up" in spheremap.sliders:
                spheremap.sliders["up"].visible = False

        return self

    def build(
        self, layout: Callable[[List[bokeh.models.UIElement]], bokeh.models.UIElement] = bokeh.layouts.row
    ) -> bokeh.models.UIElement:
        if self.visits_ds is not None:
            for spheremap in self.spheremaps:
                spheremap.connect_controls(self.visit_ds)

        map_figures = list(s.figure for s in self.spheremaps)
        combined_figure = layout(map_figures)
        return combined_figure
