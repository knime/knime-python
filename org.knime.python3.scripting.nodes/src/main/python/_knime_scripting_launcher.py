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

from contextlib import redirect_stdout, redirect_stderr
from typing import Any, Dict, List, TextIO, Callable, Optional

import py4j.clientserver
from py4j.java_gateway import JavaClass

from knime.scripting._backend import (
    ScriptingBackendCollection,
    ScriptingBackendV0,
    ScriptingBackendV1,
)
import knime.scripting._io_containers as _ioc

import knime._arrow._backend as ka
import knime._backend._gateway as kg
from knime._backend._mainloop import MainLoop
from knime.scripting._backend import KnimeUserError

import os
import traceback
import json
import sys
import logging
import warnings
import pickle


@kg.data_source("org.knime.python3.pickledobject")
def read_pickled_obj(java_data_source):
    """
    Read the pickled object from the file that is provided by the java_data_source object
    """
    with open(java_data_source.getAbsolutePath(), "rb") as file:
        return pickle.load(file)


class ScriptingEntryPoint(kg.EntryPoint):
    def __init__(self):
        self._workspace: Dict[str, Any] = {}  # TODO: should we make this thread safe?
        self._main_loop = MainLoop()
        self._external_custom_path_initialized = False
        self._working_dir_initialized = False
        self._java_callback = None
        self._backends = ScriptingBackendCollection(
            {
                "knime_io": ScriptingBackendV0(),
                "knime.scripting.io": ScriptingBackendV1(),
            }
        )
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    def setCurrentWorkingDirectory(self, working_directory: str) -> None:
        os.chdir(working_directory)
        sys.path.insert(0, working_directory)

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        if self._java_callback is not None:
            raise RuntimeError(
                "Java callback has already been initialized. Calling this method again is an implementation error."
            )
        self._java_callback = java_callback

    @property
    def java_callback(self):
        """
        Provides access to functionality on the Java side. Used by e.g. knime.scripting.jupyter to resolve KNIME URLs.
        :return: The callback on the Java side of Java type
        org.knime.python3.scripting.Python3KernelBackendProxy.Callback.
        """
        return self._java_callback

    def setFlowVariables(self, flow_variables: Dict[str, Any]) -> None:
        self._backends.set_flow_variables(flow_variables)

    def getFlowVariables(self) -> JavaClass:
        return self._backends.get_flow_variables()

    def setupIO(
        self,
        data_sources,
        flow_var_sources,
        num_out_tables,
        num_out_images,
        num_out_objects,
        java_callback,
    ):
        self._java_callback = java_callback

        def create_python_sink():
            java_sink = java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        self._backends.set_up_arrow(create_python_sink, self._java_callback)
        self.setFlowVariables(flow_var_sources)

        sources = [kg.data_source_mapper(d) for d in data_sources]

        # Set the input_tables in knime_io
        # Note: We only support arrow tables
        table_sources = [s for s in sources if isinstance(s, ka.ArrowDataSource)]
        _ioc._pad_up_to_length(_ioc._input_tables, len(table_sources))

        for idx, s in enumerate(table_sources):
            # TODO(AP-19333) we need to close the input tables?
            # ArrowSourceTable
            _ioc._input_tables[idx] = s

        # Set the input_objects in knime_io (every other source)
        objects = [s for s in sources if not isinstance(s, ka.ArrowDataSource)]
        _ioc._pad_up_to_length(_ioc._input_objects, len(objects))
        for idx, obj in enumerate(objects):
            _ioc._input_objects[idx] = obj

        # Prepare the output_* lists in knime_io
        _ioc._pad_up_to_length(_ioc._output_tables, num_out_tables)
        _ioc._pad_up_to_length(_ioc._output_images, num_out_images)
        _ioc._pad_up_to_length(_ioc._output_objects, num_out_objects)

    def execute(self, script, check_outputs):
        with redirect_stdout(
            _ForwardingTextIO(sys.stdout, self._java_callback.add_stdout)
        ), redirect_stderr(
            _ForwardingTextIO(sys.stderr, self._java_callback.add_stderr)
        ):
            try:
                assert self._java_callback
                _ioc._java_callback = self._java_callback
                # TODO: If we want tear down arrow in check_outputs
                #       then we also need to redo setupio
                exec(script, self._workspace)

                if check_outputs:
                    self._backends.check_outputs()

                return json.dumps(
                    {
                        "status": "SUCCESS",
                        "data": self._getVariablesInWorkspace(),
                        "description": "Successfully executed script",
                    }
                )

            except KnimeUserError as e:
                return json.dumps(
                    {
                        "status": "KNIME_ERROR",
                        "description": f"KnimeUserError: {str(e)}",
                        "traceback": [],
                        "data": self._getVariablesInWorkspace(),
                    }
                )

            except Exception as e:
                stacksummary = traceback.extract_tb(e.__traceback__)
                stacksummary.pop(0)
                intro_line = "Traceback (most recent call last):"

                return json.dumps(
                    {
                        "status": "EXECUTION_ERROR",
                        "description": f"{type(e).__name__}: {str(e)}",
                        "traceback": [intro_line]
                        + [i[:-1] for i in traceback.format_list(stacksummary)],
                        "data": self._getVariablesInWorkspace(),
                    }
                )
            finally:
                _ioc._java_callback = None

    def getOutputTable(
        self,
        table_index: int,
    ) -> Optional[JavaClass]:
        return self._backends.get_output_table_sink(table_index)

    def closeOutputs(self, check_outputs):
        # called form nodemodal
        # if called from execute it should also setup arrow afterwards
        try:
            self._backends.tear_down_arrow(flush=check_outputs)
        except KnimeUserError as e:
            sys.tracebacklimit = -1
            raise e

        except Exception as e:
            raise e
        finally:
            sys.tracebacklimit = None

    def writeOutputImage(self, idx: int, path: str):
        with open(path, "wb") as file:
            file.write(_ioc._output_images[idx])

    def writeOutputObject(self, idx: int, path: str) -> None:
        obj = _ioc._output_objects[idx]
        with open(path, "wb") as file:
            pickle.dump(obj=obj, file=file)

    def getOutputObjectType(self, idx: int) -> str:
        return type(_ioc._output_objects[idx]).__name__

    def getOutputObjectStringRepr(self, idx: int) -> str:
        object_as_string = str(_ioc._output_objects[idx])
        return (
            (object_as_string[:996] + "\n...")
            if len(object_as_string) > 1000
            else object_as_string
        )

    def _getVariablesInWorkspace(self) -> List[Dict[str, str]]:
        # TODO(AP-19345) provide integers + doubles not as string
        # TODO(AP-19345) provide small images of the plots in the workspace
        max_string_length = 100

        def object_to_string(obj):
            try:
                string = str(obj)
                return (
                    (string[: (max_string_length - 4)] + "\n...")
                    if len(string) > max_string_length
                    else string
                )
            except Exception as e:
                raise KnimeUserError(
                    f"It was not possible to represent {type(obj).__name__} as a string for the workspace."
                ) from e

        workspace = []

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

            workspace.append({"name": key, "type": var_type, "value": var_value})

        return workspace

    class Java:
        implements = ["org.knime.python3.scripting.nodes2.PythonScriptingEntryPoint"]


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
        # Hook into warning delivery.
        default_showwarning = warnings.showwarning

        def showwarning_hook(message, category, filename, lineno, file=None, line=None):
            """
            Copied from warnings.showwarning.
            We use this hook to prefix warning messages with "[WARN]". This makes them easier to identify on Java
            side and helps printing them at the correct log level.
            Providing a custom hook is supported as per the API documentations:
            https://docs.python.org/3/library/warnings.html#warnings.showwarning
            """
            try:
                if file is None:
                    file = sys.stderr
                    if file is None:
                        # sys.stderr is None when run with pythonw.exe - warnings get lost.
                        return
                try:
                    # Do not change the prefix. Expected on Java side.
                    file.write(
                        "[WARN]"
                        + warnings.formatwarning(
                            message, category, filename, lineno, line
                        )
                    )
                except OSError:
                    pass  # The file (probably stderr) is invalid - this warning gets lost.
            except Exception:
                # Fall back to the default implementation.
                return default_showwarning(
                    message, category, filename, lineno, file, line
                )

        warnings.showwarning = showwarning_hook
    except Exception:
        pass

    try:
        warnings.filterwarnings("default", category=DeprecationWarning)

        logging.basicConfig()
        logging.getLogger("py4j").setLevel(logging.FATAL)  # suppress py4j logs
        logging.getLogger().setLevel(logging.INFO)

        scripting_ep = ScriptingEntryPoint()
        kg.connect_to_knime(scripting_ep)
        py4j.clientserver.server_connection_stopped.connect(
            lambda *args, **kwargs: scripting_ep._main_loop.exit()
        )
        scripting_ep._main_loop.enter()
        # idles until dialog closed or after execute finish
    finally:
        if kg.client_server is not None:
            kg.client_server.shutdown()
