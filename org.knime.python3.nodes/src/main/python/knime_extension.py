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
Provides a unified user interface for the development of KNIME extensions in Python.
It simplifies import of KNIME-related Python files such that only knime_extension needs to imported. 

@author Steffen Fissler, KNIME GmbH, Konstanz, Germany
"""
import knime_node_table as _kt
import knime_node as _kn
import knime_schema as _ks
import knime_views as _kv

# re-exporting symbols so that "import knime_extension" allows the user to conduct every KAP-facing call via this interface

## knime_node
# (the first bunch comes from knime_parameter)
IntParameter = _kn.IntParameter
DoubleParameter = _kn.DoubleParameter
BoolParameter = _kn.BoolParameter
StringParameter = _kn.StringParameter
parameter_group = _kn.parameter_group
ColumnParameter = _kn.ColumnParameter
MultiColumnParameter = _kn.MultiColumnParameter

PortType = _kn.PortType
Port = _kn.Port
ViewDeclaration = _kn.ViewDeclaration
ConfigurationContext = _kn.ConfigurationContext
ExecutionContext = _kn.ExecutionContext
PythonNode = _kn.PythonNode
category = _kn.category
NodeType = _kn.NodeType
node = _kn.node
InvalidParametersError = _kn.InvalidParametersError
input_binary = _kn.input_binary
input_table = _kn.input_table
output_binary = _kn.output_binary
output_table = _kn.output_table
output_view = _kn.output_view


## knime_node_table
Table = _kt.Table
BatchOutputTable = _kt.BatchOutputTable

## knime_schema
KnimeType = _ks.KnimeType
PrimitiveTypeId = _ks.PrimitiveTypeId
DictEncodingKeyType = _ks.DictEncodingKeyType
PrimitiveType = _ks.PrimitiveType
ListType = _ks.ListType
StructType = _ks.StructType
LogicalType = _ks.LogicalType
int32 = _ks.int32
int64 = _ks.int64
double = _ks.double
bool_ = _ks.bool_
string = _ks.string
blob = _ks.blob
list_ = _ks.list_
struct = _ks.struct
logical = _ks.logical
PortObjectSpec = _ks.PortObjectSpec
BinaryPortObjectSpec = _ks.BinaryPortObjectSpec
Column = _ks.Column
Schema = _ks.Schema

## knime_views
view = _kv.view
view_ipy_repr = _kv.view_ipy_repr
view_html = _kv.view_html
view_svg = _kv.view_svg
view_png = _kv.view_png
view_jpeg = _kv.view_jpeg
NodeViewSink = _kv.NodeViewSink
view_matplotlib = _kv.view_matplotlib
view_seaborn = _kv.view_seaborn
