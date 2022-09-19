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
import sys
from contextlib import redirect_stdout, redirect_stderr
import pickle
from typing import Any, Dict, List, TextIO, Callable

import py4j.clientserver

import knime_arrow_table as kat
import knime_arrow as ka
import knime_gateway as kg
import knime_io as kio
import knime_table as kt
from knime_main_loop import MainLoop

# TODO(AP-19333) organize imports
# TODO(AP-19333) logging (see knime_node_backend)
# TODO(AP-19333) immediately check the output when it is assined
#      Also do row checking etc. -> If the interactive run works the node execution should also work


@kg.data_source("org.knime.python3.pickledobject")
def read_pickled_obj(java_data_source):
    """
    Read the pickled object from the file that is provided by the java_data_source object
    """
    with open(java_data_source.getAbsolutePath(), "rb") as file:
        return pickle.load(file)


class ScriptingEntryPoint(kg.EntryPoint):
    def __init__(self):
        super().__init__()
        self._main_loop = MainLoop()
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

        self._workspace: Dict[str, Any] = {}

    def setupIO(
        self,
        data_sources,
        num_out_tables,
        num_out_images,
        num_out_objects,
        java_callback,
    ):
        self._java_callback = java_callback

        # TODO(AP-19551) make flow variables available in knio.flow_variables

        # TODO(AP-19339) adapt to new API with Table
        def create_python_sink():
            java_sink = java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        sources = [kg.data_source_mapper(d) for d in data_sources]

        # Set the input_tables in knime_io
        # Note: We only support arrow tables
        table_sources = [s for s in sources if isinstance(s, ka.ArrowDataSource)]
        kio._pad_up_to_length(kio._input_tables, len(table_sources))
        for idx, s in enumerate(table_sources):
            # TODO(AP-19333) we need to close the input tables?
            kio._input_tables[idx] = kat.ArrowReadTable(s)

        # Set the input_objects in knime_io (every other source)
        objects = [s for s in sources if not isinstance(s, ka.ArrowDataSource)]
        kio._pad_up_to_length(kio._input_objects, len(objects))
        for idx, obj in enumerate(objects):
            kio._input_objects[idx] = obj

        # Prepare the output_* lists in knime_io
        kio._pad_up_to_length(kio._output_tables, num_out_tables)
        kio._pad_up_to_length(kio._output_images, num_out_images)
        kio._pad_up_to_length(kio._output_objects, num_out_objects)

        # Set the table backend such that new tables can be
        # created in the script
        kt._backend = kat.ArrowBackend(create_python_sink)

    def execute(self, script):
        with redirect_stdout(
            _ForwardingTextIO(sys.stdout, self._java_callback.add_stdout)
        ), redirect_stderr(
            _ForwardingTextIO(sys.stderr, self._java_callback.add_stderr)
        ):
            # Run the script
            exec(script, self._workspace)

        return self._getVariablesInWorkspace()

    def closeOutputs(self, check_outputs):
        # TODO(AP-19333) check the outputs? See knime_kernel _check_outputs

        # Close the backend to finish up the outputs
        kt._backend.close()
        kt._backend = None

    def getOutputTable(self, idx: int):
        return kio.output_tables[idx]._sink._java_data_sink

    def writeOutputImage(self, idx: int, path: str):
        with open(path, "wb") as file:
            file.write(kio.output_images[idx])

    def writeOutputObject(self, idx: int, path: str) -> None:
        obj = kio.output_objects[idx]
        with open(path, "wb") as file:
            pickle.dump(obj=obj, file=file)

    def getOutputObjectType(self, idx: int) -> str:
        return type(kio.output_objects[idx]).__name__

    def getOutputObjectStringRepr(self, idx: int) -> str:
        object_as_string = str(kio.output_objects[idx])
        return (
            (object_as_string[:996] + "\n...")
            if len(object_as_string) > 1000
            else object_as_string
        )

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
            if var_type not in [
                "module",
                "type",
                "function",
                "builtin_function_or_method",
            ]:
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


class _ForwardingTextIO:
    """
    Forwards outputs intended for a wrapped output file while mimicking the file. All attribute look-ups except for the
    ones that are relevant for copying are simply delegated to the original file. Look-ups of magic attributes cannot
    be delegated. The only remotely relevant magic attributes should be the ones for entering/exiting the runtime
    context (and even those will most likely not matter in practice).
    """

    def __init__(self, original: TextIO, consumer: Callable[[str], None]):
        self._original = original
        self._original_context = None
        self._consumer = consumer

    def write(self, s):
        self._get_original().write(s)
        self._consumer(s)

    def writelines(self, lines):
        self._get_original().writelines(lines)
        for l in lines:
            self._consumer(l)

    def __enter__(self):
        self._original_context = self._original.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        suppress = self._original_context.__exit__(exc_type, exc_val, exc_tb)
        self._original_context = None
        return suppress

    def __getattr__(self, item):
        return getattr(self._get_original(), item)

    def _get_original(self):
        return (
            self._original if self._original_context is None else self._original_context
        )


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
