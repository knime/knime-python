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
Helper module that provides method for extracting parameter and UI definitions from
an annotated PythonNode.

@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

from knime_node import Parameter, UI, Rule, PythonNode
from packaging.version import Version
from numbers import Number


def extract_parameters(node: PythonNode):
    """
    Extracts the currently set parameters from the provided node.
    """
    param_descriptors = _collect_parameter_descriptors(type(node))
    return {k: getattr(node, k) for k in param_descriptors}


def inject_parameters(node: PythonNode, parameters: dict, version: Version) -> None:
    """
    Injects the provided parameters into the node while ensuring backwards compatibility.
    """
    descriptors = _collect_parameter_descriptors(type(node))
    for k, p in descriptors.items():
        # only set the parameter if it already existed in the version the settings were stored with
        if p.exists_in_version(version):
            setattr(node, k, parameters[k])


def validate_parameters(node: PythonNode, parameters: dict, version: Version) -> None:
    """
    Validates the provided parameters against the parameter definitions of the node while ensuring backwards compatibility.
    """
    descriptors = _collect_parameter_descriptors(type(node))
    for k, p in descriptors.items():
        if p.exists_in_version(version):
            if not k in parameters:
                return f"Missing the parameter {k}."
            try:
                p.validate(node, parameters[k])
            except ValueError as error:
                return str(error)


def extract_schema(node: PythonNode) -> dict:
    """
    Extracts the (JSON Forms like) schema of the parameters of the provided node.
    """
    descriptors = _collect_parameter_descriptors(type(node))
    properties = {
        k: _create_property(p, getattr(node, k)) for k, p in descriptors.items()
    }
    return {"type": "object", "properties": properties}


def _collect_parameter_descriptors(node_class):
    descriptors = _collect_descriptors(node_class)
    return {k: d.get_parameter() for k, d in descriptors.items()}


def _collect_descriptors(node_class):
    return {
        k: p
        for k, p in node_class.__dict__.items()
        if isinstance(p, (Parameter, UI, Rule))
    }


def _create_property(parameter, value) -> dict:
    return {"type": _get_type(parameter, value)}


def _get_type(parameter, value) -> str:
    if parameter._type is not None:
        return parameter._type
    if isinstance(value, str):
        return "string"
    elif isinstance(value, Number):
        return "number"
    else:
        raise ValueError(f"Unsupported value {value} encountered.")


def extract_ui_schema(node: PythonNode) -> dict:
    """
    Extracts the (JSON Forms like) UI schema from the provided node.
    """
    descriptors = _collect_parameter_descriptors(type(node))
    elements = [_create_control(n, d) for n, d in descriptors.items()]
    return {"type": "VerticalLayout", "elements": elements}


def _create_control(name, descriptor):
    # TODO extract options and suggestions if UI descriptor
    return {"type": "Control", "scope": "#/properties/" + name}  # TODO support nesting
