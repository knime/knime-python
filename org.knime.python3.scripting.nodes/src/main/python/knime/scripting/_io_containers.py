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
Internal storage for inputs and outputs of KNIME Python Scripting nodes

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

_flow_variables = {}

_input_objects = []
_input_tables = []

_output_tables = []
_output_images = []
_output_objects = []

# Access to functionality from the Java side that can be exposed via knio
_java_callback = None


def _get_workflow_temp_dir() -> str:
    _check_java_callback()
    return _java_callback.get_workflow_temp_dir()


def _get_workflow_data_area_dir() -> str:
    _check_java_callback()
    import os.path

    return os.path.join(_java_callback.get_workflow_dir(), "data")


def _check_java_callback():
    if _java_callback is None:
        raise RuntimeError("No Java callback configured, this is a coding/setup error")


def _pad_up_to_length(lst: list, length: int) -> None:
    lst += [None] * (length - len(lst))


class _FixedSizeListView:
    """
    A list view that allows element access but no deletion or append.

    Attention: if the underlying list grows, more values will also be available in this view!
    """

    def __init__(self, data, name):
        if not isinstance(data, list):
            raise TypeError("Can only convert a list into a FixedSizeList")
        self._data = data
        self._name = name

    @property
    def _name_for_len(self):
        if len(self._data) == 1:
            return self._name
        else:
            return self._name + "s"

    @property
    def _str_is_or_are(self):
        if len(self._data) == 1:
            return "is"
        else:
            return "are"

    def __getitem__(self, idx):
        if not 0 <= idx < len(self._data):
            raise KeyError(
                f"Invalid port index {idx}, only {len(self._data)} {self._name_for_len} {self._str_is_or_are} available"
            )
        return self._data[idx]

    def __setitem__(self, idx, value):
        if not 0 <= idx < len(self._data):
            raise KeyError(
                f"Invalid port index {idx}, only {len(self._data)} {self._name_for_len} {self._str_is_or_are} available"
            )
        self._data[idx] = value

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return f"{len(self._data)} {self._name_for_len}: [{', '.join([str(v) for v in self._data])}]"
