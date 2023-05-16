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
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

from abc import ABC, abstractmethod
import sys
from py4j.java_collections import JavaArray
from py4j.java_gateway import JavaClass
from typing import Any, Dict, Optional

import knime.scripting._deprecated._table as kt
import knime.scripting._deprecated._arrow_table as kat

import knime.api.table as ktn
import knime._arrow._table as katn
import knime.api.views as kv

import knime.scripting._io_containers as _ioc

import knime._backend._gateway as kg

# from autocompletion_utils import disable_autocompletion


def _format_table_type_msg(x, idx):
    return (
        type(x)
        if x is not None
        else f"Output table '{idx}' must be of type knime.api.Table or knime.api.BatchOutputTable, but got None. knio.output_tables[{idx}] has not been populated."
    )


class ScriptingBackend(ABC):
    @abstractmethod
    def check_output_table(self, table_index, table):
        pass

    @abstractmethod
    def get_output_table_sink(self, table_index: int) -> JavaClass:
        pass

    @abstractmethod
    def set_up_arrow(self, sink_factory):
        pass

    @abstractmethod
    def tear_down_arrow(self, flush: bool):
        pass

    @abstractmethod
    def get_output_view(self):
        pass


class ScriptingBackendV0(ScriptingBackend):
    """
    Deprecated scripting backend provided by knime_io
    """

    def check_output_table(self, idx, table):
        if table is None or not isinstance(table, kat._ArrowWriteTableImpl):
            type_str = type(table) if table is not None else "None"

            raise KnimeUserError(
                f"Expected a WriteTable in output_tables[{idx}], got {type_str}. "
                "Please use knime_io.write_table(data) or knime_io.batch_write_table() to create a WriteTable."
            )

    def get_output_table_sink(self, table_index: int) -> JavaClass:
        # Get the java sink for this write table
        write_table = _ioc._output_tables[table_index]
        if (not hasattr(write_table, "_sink")) or (
            not hasattr(write_table._sink, "_java_data_sink")
        ):
            raise TypeError(
                f"Output table '{table_index}' is no valid knime_io.WriteTable."
            )
        return write_table._sink._java_data_sink

    def set_up_arrow(self, sink_factory):
        kt._backend = kat.ArrowBackend(sink_factory)

    def tear_down_arrow(self, flush: bool):
        # batch tables without batches have an invalid arrow file because no schema
        # has been written yet
        for write_table in _ioc._output_tables:
            if (
                isinstance(write_table, kat.ArrowBatchWriteTable)
                and write_table.num_batches == 0
            ):
                write_table._write_empty_batch()
        kt._backend.close()
        kt._backend = None

    def get_output_view(self):
        raise KnimeUserError(
            "The deprecated knime_io backend does not support views. "
            + "Please import knime.scripting.io"
        )


class ScriptingBackendV1(ScriptingBackend):
    """
    Current scripting backend provided by knime.scripting.io
    """

    def check_output_table(self, idx, table):
        if table is None or (
            not isinstance(table, katn.ArrowTable)
            and not isinstance(table, ktn._TabularView)
            and not isinstance(table, katn.ArrowBatchOutputTable)
        ):
            raise KnimeUserError(_format_table_type_msg(table, idx))

    def get_output_table_sink(self, table_index: int):
        return _ioc._output_tables[table_index]

    def set_up_arrow(self, sink_factory):
        ktn._backend = katn._ArrowBackend(sink_factory)

    def _write_all_tables(self):
        for idx, table in enumerate(_ioc._output_tables):
            if isinstance(table, ktn._TabularView):
                table = table.get()

            if isinstance(table, katn.ArrowTable):
                sink = ktn._backend.create_sink()
                table._write_to_sink(sink)
                _ioc._output_tables[idx] = sink._java_data_sink
            elif isinstance(table, katn.ArrowBatchOutputTable):
                # write empty batch to have a valid Arrow schema
                if table.num_batches == 0:
                    table._write_empty_batch()
                _ioc._output_tables[idx] = table._sink._java_data_sink
            else:
                raise KnimeUserError(_format_table_type_msg(table, idx))

    def tear_down_arrow(self, flush: bool):
        # we write all tables here and just read the sink in the get_output_table_sink method
        if flush:
            self._write_all_tables()

        ktn._backend.close()
        ktn._backend = None

    def get_output_view(self):
        # knime.scripting.io is imported anyways because this is the active backend
        import knime.scripting.io

        return knime.scripting.io.output_view


class ScriptingBackendCollection:
    """
    A collection of all available scripting backends. Performs simultaneous initialization
    of all backends but retrieves results from only the active backend. The "active" backend
    is determined by checking which backend was imported by the user.

    As all currently available backends use the same variables for storage, some methods are not
    delegated to a specific backend but are performed here on the general storage containers.
    """

    def __init__(self, backends: Dict[str, ScriptingBackend]):
        self._backends = backends
        self._expect_view = False

    @property
    def flow_variables(self) -> Dict[str, Any]:
        return _ioc._flow_variables

    def set_flow_variables(self, flow_variables: Dict[str, Any]) -> None:
        self.flow_variables.clear()
        for key, value in flow_variables.items():
            if isinstance(value, JavaArray):
                value = [x for x in value]
            self.flow_variables[key] = value

    def get_output_table_sink(self, table_index: int) -> JavaClass:
        return self.get_active_backend_or_raise().get_output_table_sink(table_index)

    @property
    def active_backend(self) -> ScriptingBackend:
        """
        Only one of the backends can be imported at a time, so we can figure out which backend is active by
        checking which backend the user imported
        """
        for module_name, backend in self._backends.items():
            if module_name in sys.modules:
                return backend

        return None

    def get_active_backend_or_raise(self) -> ScriptingBackend:
        if self.active_backend is None:
            raise KnimeUserError(
                "Either the script has not been executed, or no KNIME scripting interface has been imported. "
                + "Please import knime.scripting.io"
            )
        return self.active_backend

    def get_flow_variables(self) -> JavaClass:
        self._check_flow_variables()

        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in self.flow_variables.keys():
            flow_variable = self.flow_variables[key]
            java_flow_variables[key] = flow_variable
        return java_flow_variables

    def check_outputs(self):
        for i, o in enumerate(_ioc._output_tables):
            self.get_active_backend_or_raise().check_output_table(i, o)

        for i, o in enumerate(_ioc._output_objects):
            if o is None:
                raise KnimeUserError(
                    f"Expected an object in output_objects[{i}], got None. Did you assign the output object?"
                )

        for i, o in enumerate(_ioc._output_images):
            if o is None:
                if i == 0 and self._expect_view:
                    # If we have an output view we will just try to render the view to the
                    # first output image
                    _ioc._output_images[0] = self._render_view()
                else:
                    raise KnimeUserError(
                        f"Expected an image in output_images[{i}], got None. Did you assign the output image?"
                    )
            else:
                try:
                    import io

                    io.BytesIO(o)
                except TypeError:
                    raise KnimeUserError(
                        f"The image in output_images[{i}] (of type {type(o)}) can't be written into a file."
                    )

        self._check_flow_variables()

        if self._expect_view:
            v = self.get_active_backend_or_raise().get_output_view()
            if v is None:
                raise KnimeUserError(
                    "Expected an output view in output_view, got None. Did you assign an output view?"
                )
            elif not isinstance(v, kv.NodeView):
                raise KnimeUserError(
                    f"Expected an output view in output_view, got {type(v)}"
                )

    def _render_view(self):
        v = self.get_active_backend_or_raise().get_output_view()
        # NB: if the view is None this will be handled later
        if v is not None:
            try:
                rendered = v.render()
                if isinstance(rendered, str):
                    rendered = rendered.encode()
                return rendered
            except NotImplementedError:
                raise KnimeUserError(
                    "Cannot generate an SVG or PNG image from the view. "
                    "Please assign a value to output_images[0]."
                )

    def _check_flow_variables(self):
        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in self.flow_variables.keys():
            flow_variable = self.flow_variables[key]
            try:
                java_flow_variables[key] = flow_variable
            except AttributeError as ex:
                # py4j raises attribute errors of the form "'<type>' object has no attribute '_get_object_id'" if it
                # fails to translate Python objects to Java objects.
                raise KnimeUserError(
                    f"Flow variable '{key}' of type '{type(flow_variable)}' cannot be translated to a valid KNIME flow "
                    f"variable. Please remove the flow variable or change its type to something that can be translated."
                )

    def set_up_arrow(self, sink_factory):
        for b in self._backends.values():
            b.set_up_arrow(sink_factory)

    def tear_down_arrow(self, flush: bool):
        for b in self._backends.values():
            is_active_backend = (
                False if self.active_backend is None else b == self.active_backend
            )
            b.tear_down_arrow(flush and is_active_backend)


class KnimeUserError(Exception):
    """An error that indicates that there is an error in the user script."""

    pass
