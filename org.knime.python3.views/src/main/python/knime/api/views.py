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
from typing import Any, Union, Optional, Callable

import knime._backend._gateway as kg

LOGGER = logging.getLogger("knime.api.views")

_SVG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <body>
    {svg}
    </body>
</html>
"""

_PNG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <body>
    <img src="data:image/png;base64,{png_b64}" style="width: 100%; height: 100%;" />
    </body>
</html>
"""

_JPEG_HTML_BODY = """
<!DOCTYPE html>
<html>
    <body>
    <img src="data:image/jpeg;base64,{jpeg_b64}" style="width: 100%; height: 100%;" />
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


class NodeView:
    """A view of a KNIME node that can be displayed for the user.

    Do not create a NodeView directly but use the utility functions
    view, view_html, view_svg, view_png, and view_jpeg.
    """

    def __init__(
        self,
        html: str,
        svg_or_png: Optional[Union[str, bytes]] = None,
        render_fn: Optional[Callable[[], Union[str, bytes]]] = None,
    ) -> None:
        self.html = html
        self._svg_or_png = svg_or_png
        self._render_fn = render_fn

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
    """Create an NodeView for the given object.

    This method tries to find out the best option to display the given object.
    First, the method checks if a special view implementation (listed below)
    exists for the given object. Next, IPython _repr_html_, _repr_svg_,
    _repr_png_, or _repr_jpeg_ are used.

    Special view implementations:

    - HTML: The obj must be of type str and start with "<!DOCTYPE html>". The document
      must be self-contained and must not reference external resources. Links to
      external resources will be opened in an external browser.
    - SVG: The obj must be of type str and contain a valid SVG
    - PNG: The obj must be of type bytes and contain a PNG image file
    - JPEG: The obj must be of type bytes and contain a JPEG image file
    - Matplotlib: The obj must be a matplotlib.figure.Figure
    - Plotly: The obj must be a plotly.graph_objects.Figure

    Args:
        obj: The object which should be displayed

    Raises:
        ValueError: If no view could be created for the given object
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
    """Create a NodeView by using the IPython _repr_*_ function of the object.

    Tries to use
    * _repr_html_
    * _repr_svg_
    * _repr_png_
    * _repr_jpeg_
    in this order.

    Args:
        obj: The object which should be displayed

    Raises:
        ValueError: If no view could be created for the given object
    """

    def rm_md(data):
        """Split data from metadata if necessary"""
        if isinstance(data, tuple) and len(data) == 2:
            return data[0]
        else:
            return data

    def hasrepr(type):
        """Check if the object has a representation of the given type"""
        return hasattr(obj, f"_repr_{type}_")

    def find_render_fn():
        """Find the best render function for the object"""
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
) -> NodeView:
    """Create a NodeView that displays the given HTML document.

    The document must be self-contained and must not reference external resources. Links
    to external resources will be opened in an external browser.

    Args:
        html: A string containing the HTML document.
        svg_or_png: A rendered representation of the HTML page. Either a string
            containing an SVG or a bytes object containing an PNG image
        render_fn: A callable that returns an SVG or PNG representation of the page
    """
    return NodeView(html, svg_or_png=svg_or_png, render_fn=render_fn)


def view_svg(svg: str) -> NodeView:
    """Create a NodeView that displays the given SVG.

    Args:
        svg: A string containing the SVG.
    """
    return NodeView(_SVG_HTML_BODY.format(svg=svg), svg_or_png=svg)


def view_png(png: bytes) -> NodeView:
    """Create a NodeView that displays the given PNG image.

    Args:
        png: The bytes of the PNG image
    """
    b64 = base64.b64encode(png).decode("ascii")
    return NodeView(_PNG_HTML_BODY.format(png_b64=b64), svg_or_png=png)


def view_jpeg(jpeg: bytes) -> NodeView:
    """Create a NodeView that displays the given JPEG image.

    Args:
        jpeg: The bytes of the JPEG image
    """
    b64 = base64.b64encode(jpeg).decode("ascii")
    return NodeView(_JPEG_HTML_BODY.format(jpeg_b64=b64), svg_or_png=jpeg)


# NodeViewSink


@kg.data_sink("org.knime.python3.views")
class NodeViewSink:
    """A sink consuming views and making them available to the Java process.

    The implementation simply writes the HTML to a file path given by the Java
    process.
    """

    def __init__(self, java_data_sink) -> None:
        self._java_data_sink = java_data_sink

    def display(self, obj: Union[NodeView, Any]):
        if isinstance(obj, NodeView):
            node_view = obj
        else:
            node_view = view(obj)

        with open(self._java_data_sink.getOutputFilePath(), "w", encoding="utf-8") as f:
            f.write(node_view.html)


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
    """Create a view showing the given matplotlib figure.

    The figure is displayed by exporting it as an SVG. If no figure is given
    the current active figure is displayed. Note that the figure is closed and
    should not be used after calling this method.

    Args:
        fig: A matplotlib.figure.Figure which should be displayed.
        format: Format of the view inside the HTML document. Either "png" or "svg".

    Raises:
        ImportError: If matplotlib is not available.
        TypeError: If the figure is not a matplotlib figure.
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
    """Create a view showing the current active seaborn figure.

    This fuction just calls view_matplotlib() because seaborn plots are just
    matplotlib figures under the hood.

    Raises:
        ImportError: If matplotlib is not available.
    """
    return view_matplotlib()


##########################################################################
# PLOTLY
##########################################################################

PLOTLY_POST_JS = KNIME_UI_EXT_SERVICE_JS + _read_js_file("plotly-post-script.js")


def view_plotly(fig) -> NodeView:
    """Create a view showing the given plotly figure.

    The figure is displayed by exporting it as an HTML document.

    To be able to synchronize the selection between the view and other KNIME views the
    customdata of the figure traces must be set to the RowID.

    **Example**::

        fig = px.scatter(df, x="my_x_col", y="my_y_col", color="my_label_col",
                         custom_data=[df.index])
        node_view = view_plotly(fig)

    Args:
        fig: A plotly.graph_objects.Figure object which should be displayed.

    Raises:
        ImportError: If plotly is not available.
        TypeError: If the figure is not a plotly figure.
    """
    import plotly
    import plotly.graph_objects

    if not isinstance(fig, plotly.graph_objects.Figure):
        raise TypeError("the given figure is not a plotly figure")

    if any(["customdata" in trace for trace in fig.data]):
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
