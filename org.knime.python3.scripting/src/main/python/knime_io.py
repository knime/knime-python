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

# @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
from typing import Any, Dict, List

# Do not remove, meant to be reexported.
from knime_table import write_table, batch_write_table, WriteTable, ReadTable, Batch
from knime_table import _FixedSizeListView

flow_variables: Dict[str, Any] = {}

_input_objects = []
_input_tables = []

input_objects: List = _FixedSizeListView(_input_objects, "input_object")
"""
A list of input objects of this script node using zero-based indices.
Input objects are Python objects that are passed in from another Python script node's
``output_object`` port. This can, for instance, be used to pass trained models between Python nodes.
"""

input_tables: List[ReadTable] = _FixedSizeListView(_input_tables, "input_table")
"""
The input tables of this script node. Tables are available in the same order as the port 
connectors are displayed alongside the node, using zero-based indexing.
"""

_output_tables = []
_output_images = []
_output_objects = []

output_tables: List[WriteTable] = _FixedSizeListView(_output_tables, "output_table")
"""
The output tables of this script node. You should assign a WriteTable
or BatchWriteTable to each output port of this node. See the factory methods
``knime_io.write_table()`` and ``knime_io.batch_write_table()`` below.

**Example**::

    knime_io.output_tables[0] = knime_io.write_table(my_pandas_df)

"""

output_images: List = _FixedSizeListView(_output_images, "output_image")
"""The output images of this script node. The value passed to the output port
should be an array of bytes encoding an SVG or PNG image.

**Example**::

    data = knime_io.input_tables[0].to_pandas()
    buffer = io.BytesIO()

    pyplot.figure()
    pyplot.plot('x', 'y', data=data)
    pyplot.savefig(buffer, format='svg')

    knime_io.output_images[0] = buffer.getvalue()

"""

output_objects: List = _FixedSizeListView(_output_objects, "output_object")
"""
The output objects of this script node. Each output object can be an arbitrary 
Python object as long as it can be *pickled*. Use this to pass e.g. a trained
model to another Python script node.

**Example**::

    data = pandas.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    knime_io.output_objects[0] = data

"""


def _pad_up_to_length(lst: list, length: int) -> None:
    lst += [None] * (length - len(lst))


__all__ = [
    "flow_variables",
    "input_objects",
    "input_tables",
    "output_tables",
    "output_objects",
    "output_images",
    "write_table",
    "batch_write_table",
    "WriteTable",
    "ReadTable",
    "Batch",
]
