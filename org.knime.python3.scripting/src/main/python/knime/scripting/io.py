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

"""
Input and output variables used to communicate with KNIME from within
KNIME's Python Scripting nodes
"""
import sys

if "knime_io" in sys.modules:
    try:
        import sphinx

        sphinx_build = hasattr(sphinx, "application")
    except ImportError:
        sphinx_build = False
    if not sphinx_build:
        raise ImportError(
            "Cannot use knime_io and knime.scripting.io in the same Python script"
        )

from typing import Any, Dict, List, Union, Optional

# Do not remove, meant to be reexported.
from knime.api.table import Table, BatchOutputTable
from knime.api.views import (
    NodeView,
    view,
    view_html,
    view_svg,
    view_png,
    view_jpeg,
    view_ipy_repr,
    view_matplotlib,
    view_seaborn,
    view_plotly,
    KNIME_UI_EXT_SERVICE_JS,
    KNIME_UI_EXT_SERVICE_JS_DEV,
)
import knime.scripting._io_containers as _ioc

# -----------------------------------------------------------------------------------------
def _prepare_input_tables():
    if len(_ioc._input_tables) == 0:
        return

    import knime._arrow._table as kat

    for idx, data_source in enumerate(_ioc._input_tables):
        _ioc._input_tables[idx] = kat.ArrowSourceTable(data_source)


# We only know during import of this module or of knime_io which of the two APIs are
# used in the Python script, so we can only create the respective table implementations here
_prepare_input_tables()
# -----------------------------------------------------------------------------------------

flow_variables: Dict[str, Any] = _ioc._flow_variables
"""
A dictionary of flow variables provided by the KNIME workflow.
New flow variables can be added to the output of the node by adding them to the dictionary.
Supported flow variable types are numbers, strings, booleans and lists thereof.
"""

input_objects: List = _ioc._FixedSizeListView(_ioc._input_objects, "input_object")
"""
A list of input objects of this script node using zero-based indices. This list has a fixed size, which is determined 
by the number of input object ports configured for this node. Input objects are Python objects that are passed in from 
another Python script node's``output_object`` port. This can, for instance, be used to pass trained models between 
Python nodes. If no input is given, the list exists but is empty.
"""

input_tables: List[Table] = _ioc._FixedSizeListView(_ioc._input_tables, "input_table")
"""
The input tables of this script node. This list has a fixed size, which is determined by the number of input table 
ports configured for this node.  Tables are available in the same order as the port connectors are displayed 
alongside the node (from top to bottom), using zero-based indexing. If no input is given, the
list exists but is empty.
"""

output_tables: List[Union[Table, BatchOutputTable]] = _ioc._FixedSizeListView(
    _ioc._output_tables, "output_table"
)
"""
The output tables of this script node. This list has a fixed size, which is determined by the number of output table 
ports configured for this node.  You should assign a ``Table`` or ``BatchOutputTable`` to each output port of this node. 

**Example**::

    import knime.scripting.io as knio
    knio.output_tables[0] = knio.Table.from_pandas(my_pandas_df)

"""

output_images: List = _ioc._FixedSizeListView(_ioc._output_images, "output_image")
"""
The output images of this script node. This list has a fixed size, which is determined by the number of output images
configured for this node. The value passed to the output port should be a bytes-like object encoding an SVG or PNG image.

**Example**::

    import knime.scripting.io as knio

    data = knio.input_tables[0].to_pandas()
    buffer = io.BytesIO()

    pyplot.figure()
    pyplot.plot('x', 'y', data=data)
    pyplot.savefig(buffer, format='svg')

    knio.output_images[0] = buffer.getvalue()

"""

output_objects: List = _ioc._FixedSizeListView(_ioc._output_objects, "output_object")
"""
The output objects of this script node. This list has a fixed size, which is determined by the number of output object 
ports configured for this node. Each output object can be an arbitrary Python object as long as it can be *pickled*. 
Use this to, for example, pass a trained model to another Python script node.

**Example**::

    model = torchvision.models.resnet18()
    ...
    # train/finetune model
    ...
    knime.scripting.io.output_objects[0] = model

"""

output_view: Optional[NodeView] = None
"""
The output view of the script node. This variable must be populated with a ``NodeView`` when using the Python
View node. Views can be created by calling the ``view(obj)`` method with a viewable object. See the
documentation of ``view(obj)`` to understand how views are created from different kinds of objects.

**Example**::

    import knime.scripting.io as knio
    import plotly.express as px

    fig = px.scatter(x=data_x, y=data_y)
    knio.output_view = knio.view(fig)

"""


def get_workflow_temp_dir() -> str:
    """
    Returns the local absolute path where temporary files for this workflow
    should be stored. Files created in this folder are not automatically deleted
    by KNIME.

    By default, this folder is located in the operating system's
    temporary folder. In that case, the contents will be cleaned by the OS.
    """
    return _ioc._get_workflow_temp_dir()


def get_workflow_data_area_dir() -> str:
    """
    Returns the local absolute path to the current workflow's data area folder.
    This folder is meant to be part of the workflow, so its contents are included
    whenever the workflow is shared.
    """
    return _ioc._get_workflow_data_area_dir()


__all__ = [
    "flow_variables",
    "input_objects",
    "input_tables",
    "output_tables",
    "output_objects",
    "output_images",
    "output_view",
    "Table",
    "BatchOutputTable",
    "get_workflow_temp_dir",
    "get_workflow_data_area_dir",
]
