# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

import os
import io
import base64
import logging
from typing import Union, Optional, Callable

LOGGER = logging.getLogger("knime.api.views")

_SVG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <head>
        <script type="text/javascript">{js}</script>
    </head>
    <body>
        <div id="view-container">{svg}</div>
    </body>
</html>
"""

_PNG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <head><script type="text/javascript">{js}</script></head>
    <body>
    <img id="view-container" src="data:image/png;base64,{png_b64}" style="width: 100%; height: 100%;" />
    </body>
</html>
"""

_JPEG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <head><script type="text/javascript">{js}</script></head>
    <body>
        <img id="view-container" src="data:image/jpeg;base64,{jpeg_b64}" style="width: 100%; height: 100%;" />
    </body>
</html>
"""


def _read_js_file(name):
    try:
        with open(
            os.path.normpath(os.path.join(__file__, "..", name)),
            "r",
        ) as f:
            return f.read()
    except Exception:
        import warnings

        warnings.warn(
            f"Could not read {name} JavaScript. "
            + "Selections will not be propagated to and from Python views. "
            + "This warning can be ignored when running Python code outside of KNIME AP."
        )
        return ""


# Open the knime-ui-extension-service.min.js file to include it in the HTML if needed
KNIME_UI_EXT_SERVICE_JS = _read_js_file("knime-ui-extension-service.min.js")
"""
The JS source of the knime-ui-extension-service. This can be used to create a
SelectionService and propagate selections between a static HTML view and KNIME AP.
``KNIME_UI_EXT_SERVICE_JS_DEV`` contains a non-minified version of the same code.

:meta hide-value:
"""

KNIME_UI_EXT_SERVICE_JS_DEV = _read_js_file("knime-ui-extension-service.dev.js")
"""
The JS source of the knime-ui-extension-service. This can be used to create a
SelectionService and propagate selections between a static HTML view and KNIME AP.
Use the minified version ``KNIME_UI_EXT_SERVICE_JS`` to reduce the size of the final html
file.

:meta hide-value:
"""

_IMAGE_REPORTING_JS = KNIME_UI_EXT_SERVICE_JS + _read_js_file("image-reporting.js")


class NodeView:
    """A wrapper that embeds a visualisation in KNIME Analytics Platform.

    Notes
    -----
    Do **not** instantiate *NodeView* directly—use one of the helper functions
    (:func:`view`, :func:`view_html`, :func:`view_svg`, :func:`view_png`,
    :func:`view_jpeg`, …).

    Parameters
    ----------
    html : str
        Self-contained HTML snippet that renders the interactive view inside
        KNIME AP.
    svg_or_png : str | bytes | None, optional
        Static SVG **or** PNG/JPEG bytes handed to the Reporting engine when a
        report is generated.  If *None*, the value is produced lazily via
        *render_fn*.
    render_fn : Callable[[], str | bytes] | None, optional
        Callback that returns *svg_or_png* on-demand.
    can_be_used_in_report : bool, default ``False``
        Set **True** to signal that this view can be embedded in a report
        produced by the **KNIME Reporting Extension**.

        * **Image helpers** (`view_svg`, `view_png`, `view_jpeg`,
          :pydata:`view_matplotlib`, …) turn the flag on automatically because
          they always provide the image to the Reporting engine.

        * **HTML helpers** leave the flag *False* by default. Only enable it
          when the view's JavaScript sends a static representation to the
          ReportingService using ``reportingService.setReportingContent(...)``.
          If you are not in control of the JavaScript or cannot ensure this,
          keep the flag *False* so the view is omitted from the report instead
          of breaking it.
    """

    def __init__(
        self,
        html: str,
        svg_or_png: Optional[Union[str, bytes]] = None,
        render_fn: Optional[Callable[[], Union[str, bytes]]] = None,
        can_be_used_in_report: bool = False,
    ) -> None:
        self.html = html
        self._svg_or_png = svg_or_png
        self._render_fn = render_fn
        self.can_be_used_in_report = can_be_used_in_report

        # TODO(AP-22036, AP-22035) we could use the render_fn to generate the
        # image and always be able to use the view in reports. However, this would
        # require us to always call the render_fn and put the result into the HTML
        # even if the view is not used in a report.

    def render(self) -> Union[str, bytes]:
        # We alread have a rendered representation
        if self._svg_or_png is not None:
            return self._svg_or_png

        # We need to call the render function to get a representation
        if self._render_fn is not None:
            self._svg_or_png = self._render_fn()
            return self._svg_or_png

        # We cannot get a rendered representation
        raise NotImplementedError("cannot generate an SVG or PNG image from the view")


# Functions for creating NodeViews


def view(obj) -> NodeView:
    """Return a best-effort :class:`NodeView` for *obj*.

    The function attempts, in order, to

    1.  match *obj* to one of the dedicated helper functions listed
        below, or
    2.  fall back to the object's IPython ``_repr_*_`` methods
        (``_repr_html_``, ``_repr_svg_``, ``_repr_png_``,
        ``_repr_jpeg_``).

    Images - SVG, PNG, JPEG, matplotlib, seaborn - are exported with
    a static snapshot and therefore show up in Reports of the *KNIME
    Reporting Extension* outputs automatically. All other supported
    objects (HTML strings, Plotly figures, etc.) are not supported in
    reports by default. To enable them in reports, use the
    :func:`view_html` function and set the ``can_be_used_in_report``
    flag.

    ----------------------------------------------------------------
    Special view implementations
    ----------------------------------------------------------------
    The input must match one of the following patterns:

    - **HTML**  ``str`` starting with ``"<!DOCTYPE html>"``.
      Must be self-contained; external links open in a browser.
    - **SVG**   ``str`` containing valid ``<svg … xmlns="…">`` markup.
    - **PNG**   ``bytes`` beginning with the PNG magic number.
    - **JPEG**  ``bytes`` beginning ``0xFFD8FF`` and ending ``0xFFD9``.
    - **Matplotlib**  ``matplotlib.figure.Figure`` instance.
    - **Plotly**  ``plotly.graph_objects.Figure`` instance.

    Parameters
    ----------
    obj : Any
        The object to visualise.

    Raises
    ------
    ValueError
        If no suitable helper or ``_repr_*_`` method is found.
    """

    if type(obj) is str:
        # HTML
        if obj.lstrip()[:15].lower() == "<!doctype html>":
            return view_html(obj)

        # SVG
        # https://developer.mozilla.org/en-US/docs/Web/SVG/Tutorial/Getting_Started
        if "<svg" in obj[:256] and 'xmlns="http://www.w3.org/2000/svg"' in obj[:256]:
            return view_svg(obj)

    if type(obj) == bytes:
        # PNG
        if obj.startswith(b"\211PNG\r\n\032\n"):
            return view_png(obj)

        # JPEG
        if obj.startswith(b"\xff\xd8\xff") and obj.endswith(b"\xff\xd9"):
            return view_jpeg(obj)

    # Special implementations
    # NOTE: This is not extensible from the outside.
    # Utility functions from third parties that create views from other
    # objects can live somewhere else and can be called explicitly.

    # Matplotlib
    try:
        return view_matplotlib(obj)
    except (ImportError, TypeError):
        pass

    # Plotly
    try:
        return view_plotly(obj)
    except (ImportError, TypeError):
        pass

    # TODO AP-18344: Bokeh

    # TODO AP-18346: Altair

    # TODO AP-18347: Pygal

    # Use IPython _repr_*_
    try:
        return view_ipy_repr(obj)
    except ValueError:
        raise ValueError(f"no view could be created for {obj}")


def view_ipy_repr(obj) -> NodeView:
    """
    Create a NodeView by using the IPython `_repr_*_` function of the object.

    Tries to use:

    1. `_repr_html_`
    2. `_repr_svg_`
    3. `_repr_png_`
    4. `_repr_jpeg_`

    in this order.

    Parameters
    ----------
    obj : object
        The object which should be displayed.

    Raises
    ------
    ValueError
        If no view could be created for the given object.
    """

    def rm_md(data):
        """
        Split data from metadata if necessary.
        """
        if isinstance(data, tuple) and len(data) == 2:
            return data[0]
        else:
            return data

    def hasrepr(type):
        """
        Check if the object has a representation of the given type.
        """
        return hasattr(obj, f"_repr_{type}_")

    def find_render_fn():
        """
        Find the best render function for the object.
        """
        if hasrepr("svg"):
            return obj._repr_svg_, view_svg
        if hasrepr("png"):
            return obj._repr_png_, view_png
        if hasrepr("jpeg"):
            return obj._repr_jpeg_, view_jpeg
        return None, None

    # Object has an HTML representation
    if hasrepr("html"):
        html = rm_md(obj._repr_html_())
        render_fn, _ = find_render_fn()
        return view_html(html, render_fn=render_fn)

    # Try other representations
    render_fn, view_fn = find_render_fn()
    if render_fn is not None:
        return view_fn(rm_md(render_fn()))

    raise ValueError("no _repr_*_ function is implemented by obj")


def view_html(
    html: str,
    svg_or_png: Optional[Union[str, bytes]] = None,
    render_fn: Optional[Callable[[], Union[str, bytes]]] = None,
    can_be_used_in_report=False,
) -> NodeView:
    """
    Create a NodeView that displays the given HTML document.

    The document must be self-contained and must not reference external resources. Links
    to external resources will be opened in an external browser.

    Parameters
    ----------
    html : str
        A string containing the HTML document.
    svg_or_png : str or bytes
        A rendered representation of the HTML page. Either a string containing an SVG or
        a bytes object containing a PNG image.
    render_fn : callable
        A callable that returns an SVG or PNG representation of the page.
    can_be_used_in_report : bool, default ``False``
        Indicates whether this view will appear in a report generated by
        the **KNIME Reporting Extension**. Only set the flag to ``True`` when you
        provide the view to the *knime-ui-extension-service* ReportingService.
    """
    return NodeView(
        html,
        svg_or_png=svg_or_png,
        render_fn=render_fn,
        can_be_used_in_report=can_be_used_in_report,
    )


def view_svg(svg: str) -> NodeView:
    """
    Create a :class:`NodeView` that shows an **SVG** and is *report-ready*
    out of the box.

    Parameters
    ----------
    svg : str
        SVG markup (must include the ``<svg ...>`` root element with the
        XML namespace declaration).
    """
    return NodeView(
        _SVG_HTML_BODY.format(svg=svg, js=_IMAGE_REPORTING_JS),
        svg_or_png=svg,
        can_be_used_in_report=True,
    )


def view_png(png: bytes) -> NodeView:
    """
    Create a :class:`NodeView` that shows a **PNG** image and is
    *report-ready* out of the box.

    Parameters
    ----------
    png : bytes
        Raw PNG data.
    """
    b64 = base64.b64encode(png).decode("ascii")
    return NodeView(
        _PNG_HTML_BODY.format(png_b64=b64, js=_IMAGE_REPORTING_JS),
        svg_or_png=png,
        can_be_used_in_report=True,
    )


def view_jpeg(jpeg: bytes) -> NodeView:
    """
    Create a :class:`NodeView` that shows a **JPEG** image and is
    *report-ready* out of the box.

    Parameters
    ----------
    jpeg : bytes
        Raw JPEG data.
    """
    b64 = base64.b64encode(jpeg).decode("ascii")
    return NodeView(
        _JPEG_HTML_BODY.format(jpeg_b64=b64, js=_IMAGE_REPORTING_JS),
        svg_or_png=jpeg,
        can_be_used_in_report=True,
    )


##########################################################################
# MATPLOTLIB
##########################################################################

try:
    import matplotlib

    # Set the matplotlib backend to svg which is a non-GUI backend
    # Therefore, plt.show() will do noting
    matplotlib.use("svg")

    # Set the default figure size to 1014x730 px at 100dpi (size of the view window)
    matplotlib.rcParams["figure.figsize"] = (10.14, 7.3)
except ImportError:
    # matplotlib is not available
    pass


def view_matplotlib(fig=None, format="png") -> NodeView:
    """
    Create a :class:`NodeView` that displays a **matplotlib** figure and is
    *report-ready* out of the box.

    The figure is exported as a PNG or SVG (controlled by *format*).
    If *fig* is *None*, the current active figure is used.  The figure is
    then closed so it should not be modified afterwards.

    Because a static image is always supplied, the helper sets
    ``can_be_used_in_report=True`` automatically, allowing the view to appear
    in reports generated by the **KNIME Reporting Extension** without
    additional JavaScript.

    Parameters
    ----------
    fig : matplotlib.figure.Figure, optional
        Figure to render.  Defaults to the current figure.
    format : {"png", "svg"}, default "png"
        Output format embedded in the HTML snippet.

    Raises
    ------
    ImportError
        If matplotlib is not available.
    TypeError
        If the figure is not a matplotlib figure.
    """
    import matplotlib.figure

    if format not in ["png", "svg"]:
        raise ValueError(
            f'unsupported format for matplotlib view: {format}. Use "png" or "svg".'
        )

    if fig is None:
        # Use the current active figure
        import matplotlib.pyplot

        fig = matplotlib.pyplot.gcf()
    else:
        # Check the type of the figure that we got
        if not isinstance(fig, matplotlib.figure.Figure):
            raise TypeError("the given object is not a matplotlib figure")

    buffer = io.BytesIO() if format == "png" else io.StringIO()
    fig.savefig(buffer, format=format)

    # Matplotlib remembers every figure in an interal state
    # We remove the figure from the interal state such that the gc can clear the memory
    matplotlib.pyplot.close(fig)

    if format == "png":
        return view_png(buffer.getvalue())
    else:
        return view_svg(buffer.getvalue())


def view_seaborn() -> NodeView:
    """
    Create a :class:`NodeView` that shows the current active **seaborn**
    figure and is *report-ready* out of the box.

    The function simply forwards to :func:`view_matplotlib` because seaborn
    charts are regular matplotlib figures under the hood.

    As a static image is always embedded, the helper sets
    ``can_be_used_in_report=True`` automatically, so the view can be
    included in reports generated by the **KNIME Reporting Extension**
    without any extra JavaScript.

    Raises
    ------
    ImportError
        If matplotlib is not available.
    """
    return view_matplotlib()


##########################################################################
# PLOTLY
##########################################################################

PLOTLY_POST_JS = KNIME_UI_EXT_SERVICE_JS + _read_js_file("plotly-post-script.js")


def view_plotly(fig) -> NodeView:
    """
    Create a view showing the given plotly figure.

    The figure is displayed by exporting it as an HTML document.

    To be able to synchronize the selection between the view and other KNIME views the
    customdata of the figure traces must be set to the RowID.

    Parameters
    ----------
    fig : plotly.graph_objects.Figure
        A plotly figure object which should be displayed.

    Raises
    ------
    ImportError
        If plotly is not available.
    TypeError
        If the figure is not a plotly figure.

    Examples
    ---------

    >>> fig = px.scatter(df, x="my_x_col", y="my_y_col", color="my_label_col",
    ...                 custom_data=[df.index])
    ... node_view = view_plotly(fig)
    """
    import plotly
    import plotly.graph_objects

    if not isinstance(fig, plotly.graph_objects.Figure):
        raise TypeError("the given figure is not a plotly figure")

    def has_customdata(trace):
        return "customdata" in trace and trace["customdata"] is not None

    if any([has_customdata(trace) for trace in fig.data]):
        # There is customdata set -> We can set our JS for selection
        if KNIME_UI_EXT_SERVICE_JS == "":
            LOGGER.warning(
                f"Could not read knime-ui-extension-service.min.js JavaScript. "
                + "Selections will not be propagated to and from Python views. "
                + "This warning can be ignored when running Python code outside of KNIME AP."
            )

        try:
            current_version = tuple(int(s) for s in plotly.__version__.split("."))
            if current_version < (5, 10, 0):
                LOGGER.info(
                    "Plotly < 5.10.0 does not support deleting the previous selection "
                    "box or lasso when a selection event from another node comes in. "
                    "Please update plotly for full selection synchronization support."
                )
        except:
            # This is optional
            pass

        html = fig.to_html(post_script=PLOTLY_POST_JS)
    else:
        # There is no customdata -> Inform the user and do not use our JS for selection
        LOGGER.info(
            "No custom data set in the figure data. "
            "Selection will not be synced with other views. "
            "Set custom_data to the RowID to enable selection syncing."
        )
        html = fig.to_html()

    def render_image():
        try:
            return fig.to_image("png")
        except ValueError as e:
            # NB: The error message if no engine is available is not nice -> Give our own message
            if "pip install -U kaleido" in e.args[0]:
                raise ValueError(
                    "plotly figure could not be rendered to an image. "
                    "Install 'kaleido' in your Python environment."
                )

            # Re-raise the exception so the user knows what's wrong
            raise e

    return view_html(html, render_fn=render_image)
