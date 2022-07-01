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
@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""

import json
import py4j.clientserver
import sys
from py4j.java_collections import ListConverter
from typing import Any, Dict, List

import knime_arrow_table as kat
import knime_gateway as kg
import knime_io as kio
import knime_table as kt
from knime_main_loop import MainLoop

# TODO(AP-19333) organize imports
# TODO(AP-19333) logging (see knime_node_backend)
# TODO(AP-19337) support multiple inputs/outputs
# TODO(AP-19337) support different port types
# TODO(AP-19333) immediately check the output when it is assined
#      Also do row checking etc. -> If the interactive run works the node execution should also work


class ScriptingEntryPoint(kg.EntryPoint):
    def __init__(self):
        super().__init__()
        self._main_loop = MainLoop()
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

        self._workspace: Dict[str, Any] = {}

    def setupIO(self, data_sources, num_outputs, java_callback):
        # TODO(AP-19339) adapt to new API with Table
        def create_python_sink():
            java_sink = java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        sources = [kg.data_source_mapper(d) for d in data_sources]

        # Set the input_tables in knime_io
        kio._pad_up_to_length(kio._input_tables, len(sources))
        for idx, s in enumerate(sources):
            # TODO(AP-19333) we need to close the input tables?
            kio._input_tables[idx] = kat.ArrowReadTable(s)

        # Prepare the output_tables in knime_io
        kio._pad_up_to_length(kio._output_tables, num_outputs)

        # Set the table backend such that new tables can be
        # created in the script
        kt._backend = kat.ArrowBackend(create_python_sink)

    def execute(self, script):
        # Run the script
        exec(script, self._workspace)

        sys.stdout.flush()
        sys.stderr.flush()

        return self._getVariablesInWorkspace()

    def getOutputs(self, allow_incomplete):
        # TODO(AP-19333) check the outputs? See knime_kernel _check_outputs
        # TODO(AP-19333) allow incomplete outputs for interactive runs

        # Close the backend to finish up the outputs
        kt._backend.close()
        kt._backend = None

        # Collect the outputs
        # TODO(AP-19333) check if the table is a valid table?
        output_sinks = [table._sink._java_data_sink for table in kio.output_tables]

        return ListConverter().convert(output_sinks, kg.client_server._gateway_client)

    def _getVariablesInWorkspace(self) -> List[Dict[str, str]]:
        # TODO(AP-19345) provide integers + doubles not as string
        # TODO(AP-19345) provide small images of the plots in the workspace

        # TODO(AP-19333) make configurable
        max_string_length = 100

        def object_to_string(obj):
            try:
                string = str(obj)
                return (
                    (string[: (max_string_length - 4)] + "\n...")
                    if len(string) > max_string_length
                    else string
                )
            except Exception:
                # TODO(AP-19333) handle better?
                return ""

        workspace = {
            "names": [],
            "types": [],
            "values": [],
        }

        for key, value in self._workspace.items():
            if key.startswith("__") and key.endswith("__"):
                continue  # Hide magic objects.

            var_type = type(value).__name__
            if var_type not in ["module", "type", "function", "builtin_function_or_method"]:
                # Not a special type: Get the readable string for the user
                var_value = object_to_string(value)
            elif var_type == "builtin_function_or_method":
                # builtin
                var_type = "builtin"  # shorten the type string
                var_value = ""
            else:
                # module, type or function
                var_value = ""

            workspace["names"].append(key)
            workspace["types"].append(var_type)
            workspace["values"].append(var_value)

        return json.dumps(workspace)

    class Java:
        implements = ["org.knime.python3.js.scripting.PythonJsScriptingEntryPoint"]


if __name__ == "__main__":
    try:
        scripting_ep = ScriptingEntryPoint()
        kg.connect_to_knime(scripting_ep)
        py4j.clientserver.server_connection_stopped.connect(
            lambda *args, **kwargs: scripting_ep._main_loop.exit()
        )
        scripting_ep._main_loop.enter()
    finally:
        if kg.client_server is not None:
            kg.client_server.shutdown()
