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

import os
import pickle
import py4j.clientserver
import sys
import warnings
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from py4j.java_collections import ListConverter
from py4j.java_gateway import JavaClass
from queue import Full, Queue
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional, TextIO

import knime_arrow_table as kat
import knime_gateway as kg
import knime_io as kio
import knime_table as kt


class PythonKernel(kg.EntryPoint):
    def __init__(self):
        self._workspace: Dict[str, Any] = {}  # TODO: should we make this thread safe?
        self._main_loop_queue: Queue[Optional[_ExecuteTask]] = Queue()
        self._main_loop_lock = Lock()
        self._main_loop_stopped = False
        self._external_custom_path_initialized = False
        self._working_dir_initialized = False
        self._java_callback = None

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        if self._java_callback is not None:
            raise RuntimeError(
                "Java callback has already been initialized. Calling this method again is an implementation error."
            )
        self._java_callback = java_callback

    @property
    def java_callback(self):
        """
        Provides access to functionality on the Java side. Used by e.g. knime_jupyter to resolve KNIME URLs.
        :return: The callback on the Java side of Java type org.knime.python3for2.Python3KernelBackendProxy.Callback.
        """
        return self._java_callback

    def initializeExternalCustomPath(self, external_custom_path: str) -> None:
        if self._external_custom_path_initialized:
            raise RuntimeError(
                "External custom path has already been initialized. "
                "Calling this method again is an implementation error."
            )
        sys.path.append(external_custom_path)
        self._external_custom_path_initialized = True

    def initializeCurrentWorkingDirectory(self, working_directory_path: str) -> None:
        if self._working_dir_initialized:
            raise RuntimeError(
                "Working directory has already been initialized. Calling this method again is an implementation error."
            )
        os.chdir(working_directory_path)
        sys.path.insert(0, working_directory_path)
        self._working_dir_initialized = True

    def enter_main_loop(self) -> None:
        queue = self._main_loop_queue
        while True:
            task = queue.get()
            if task is not None:
                task.run()
                queue.task_done()
            else:
                # Poison pill, exit loop.
                queue.task_done()
                return

    def exit_main_loop(self) -> None:
        with self._main_loop_lock:
            self._main_loop_stopped = True
            queue = self._main_loop_queue
            # Aggressively try to clear queue and insert poison pill.
            while True:
                with queue.all_tasks_done:
                    queue.queue.clear()
                    queue.unfinished_tasks = 0
                try:
                    queue.put_nowait(None)  # Poison pill
                except Full:
                    continue
                else:
                    break

    def setFlowVariables(self, flow_variables: Dict[str, Any]) -> None:
        kio.flow_variables.clear()
        kio.flow_variables.update(flow_variables)

    def getFlowVariables(self) -> JavaClass:
        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in kio.flow_variables.keys():
            flow_variable = kio.flow_variables[key]
            try:
                java_flow_variables[key] = flow_variable
            except AttributeError as ex:
                # py4j raises attribute errors of the form "'<type>' object has no attribute '_get_object_id'" if it
                # fails to translate Python objects to Java objects.
                raise TypeError(
                    f"Flow variable '{key}' of type '{type(flow_variable)}' cannot be translated to a valid KNIME flow "
                    f"variable. Please remove the flow variable or change its type to something that can be translated."
                ) from ex
        return java_flow_variables

    # TODO: at some point in the future, we should change this method such that it accepts all input tables at once
    #  instead of setting the tables one by one. The same applies to the way back to Java as well as to the
    #  corresponding methods dealing with pickled objects and images.
    def setInputTable(
        self, table_index: int, java_table_data_source: Optional[JavaClass]
    ) -> None:
        if java_table_data_source is not None:
            # NB: We don't close the source. It must be available for the lifetime of the process.
            table_data_source = kg.data_source_mapper(java_table_data_source)
            read_table = kat.ArrowReadTable(table_data_source)
        else:
            read_table = None
        kio._pad_up_to_length(kio._input_tables, table_index + 1)
        kio._input_tables[table_index] = read_table

    def setNumExpectedOutputTables(self, num_output_tables: int) -> None:
        kio._pad_up_to_length(kio._output_tables, num_output_tables)

    def getOutputTable(
        self,
        table_index: int,
    ) -> Optional[JavaClass]:
        # Get the java sink for this write table
        write_table = kio.output_tables[table_index]
        if (not hasattr(write_table, "_sink")) or (
            not hasattr(write_table._sink, "_java_data_sink")
        ):
            raise RuntimeError(
                "Write table was not created with the Arrow backend. "
                + "This is an implementation error."
            )
        return write_table._sink._java_data_sink

    def setInputObject(self, object_index: int, path: Optional[str]) -> None:
        if path is not None:
            with open(path, "rb") as file:
                obj = pickle.load(file)
        else:
            obj = None
        kio._pad_up_to_length(kio._input_objects, object_index + 1)
        kio._input_objects[object_index] = obj

    def setNumExpectedOutputObjects(self, num_output_objects: int) -> None:
        kio._pad_up_to_length(kio._output_objects, num_output_objects)

    def getOutputObject(self, object_index: int, path: str) -> None:
        obj = kio.output_objects[object_index]
        with open(path, "wb") as file:
            pickle.dump(obj=obj, file=file)

    def getOutputObjectType(self, object_index: int) -> str:
        return type(kio.output_objects[object_index]).__name__

    def getOutputObjectStringRepresentation(self, object_index: int) -> str:
        object_as_string = str(kio.output_objects[object_index])
        return (
            (object_as_string[:996] + "\n...")
            if len(object_as_string) > 1000
            else object_as_string
        )

    def setNumExpectedOutputImages(self, num_output_images: int) -> None:
        kio._pad_up_to_length(kio._output_images, num_output_images)

    def getOutputImage(
        self,
        image_index: int,
        path: str,
    ) -> None:
        image = kio.output_images[image_index]
        with open(path, "wb") as file:
            file.write(image)

    def getVariablesInWorkspace(self) -> List[Dict[str, str]]:
        def object_to_string(obj):
            try:
                string = str(obj)
                return (string[:996] + "\n...") if len(string) > 1000 else string
            except Exception:
                return ""

        modules = []
        classes = []
        functions = []
        variables = []
        for key, value in self._workspace.items():
            var_type = type(value).__name__
            var_value = ""
            if var_type == "module":
                category = modules
            elif var_type == "type":
                category = classes
            elif var_type == "function":
                category = functions
            else:
                category = variables
                var_value = object_to_string(value)
            if not (key.startswith("__") and key.endswith("__")):  # Hide magic objects.
                category.append({"name": key, "type": var_type, "value": var_value})

        def sort(unsorted):
            return sorted(unsorted, key=lambda e: e["name"])

        all_variables = []
        all_variables.extend(sort(modules))
        all_variables.extend(sort(classes))
        all_variables.extend(sort(functions))
        all_variables.extend(sort(variables))
        # TODO: py4j's auto-conversion does not work for some reason. It should be enabled...
        return ListConverter().convert(all_variables, kg.client_server._gateway_client)

    def autoComplete(
        self, source_code: str, line: int, column: int
    ) -> List[Dict[str, str]]:
        try:
            import jedi
        except ImportError:
            jedi = None
        suggestions = []
        if jedi is not None:
            # Needed to make jedi thread-safe. Calls to this method are initiated asynchronously on the Java side.
            jedi.settings.fast_parser = False
            try:
                #  Jedi's line numbering starts at 1.
                line += 1
                try:
                    # Use jedi's 0.16.0+ API.
                    completions = jedi.Script(source_code, path="").complete(
                        line,
                        column,
                    )
                except AttributeError:
                    # Fall back to jedi's older API. ("complete" raises the AttributeError caught here.)
                    completions = jedi.Script(
                        source_code, line, column, ""
                    ).completions()
                for completion in completions:
                    suggestions.append(
                        {
                            "name": completion.name,
                            "type": completion.type,
                            "doc": completion.docstring(),
                        }
                    )
            except Exception:  # Autocomplete is purely optional. So a broad exception clause should be fine.
                warnings.warn("An error occurred while autocompleting.")
        return ListConverter().convert(suggestions, kg.client_server._gateway_client)

    def executeOnMainThread(self, source_code: str) -> List[str]:
        with self._main_loop_lock:
            if self._main_loop_stopped:
                raise RuntimeError(
                    "Cannot schedule executions on the main thread after the main loop stopped."
                )
            execute_task = _ExecuteTask(self._execute, source_code)
            self._main_loop_queue.put(execute_task)
            return execute_task.result()

    def executeOnCurrentThread(self, source_code: str) -> List[str]:
        return self._execute(source_code)

    def _execute(self, source_code: str) -> List[str]:
        def create_python_sink():
            java_sink = self._java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        with redirect_stdout(_CopyingTextIO(sys.stdout)) as stdout, redirect_stderr(
            _CopyingTextIO(sys.stderr)
        ) as stderr:
            kt._backend = kat.ArrowBackend(create_python_sink)
            exec(source_code, self._workspace)
            kt._backend.close()
            kt._backend = None
        return ListConverter().convert(
            [stdout.get_copy(), stderr.get_copy()], kg.client_server._gateway_client
        )

    class Java:
        implements = ["org.knime.python3for2.Python3KernelBackendProxy"]


class _ExecuteTask:
    def __init__(self, execute_func: Callable[[str], List[str]], *args):
        self._execute_func = execute_func
        self._args = args
        self._execute_finished = Event()
        self._result = None
        self._exception = None

    def run(self):
        try:
            self._result = self._execute_func(*self._args)
        except Exception as ex:
            self._exception = ex
        finally:
            self._execute_finished.set()

    def result(self) -> List[str]:
        self._execute_finished.wait()
        if self._exception is not None:
            try:
                raise self._exception
            finally:
                # Copied from concurrent.futures._base.Future:
                # Break a reference cycle with the exception in self._exception
                self = None
        return self._result


class _CopyingTextIO:
    """
    Copies outputs intended for a wrapped output file while mimicking the file. All attribute look-ups except for the
    ones that are relevant for copying are simply delegated to the original file. Look-ups of magic attributes cannot
    be delegated. The only remotely relevant magic attributes should be the ones for entering/exiting the runtime
    context (and even those will most likely not matter in practice).
    """

    def __init__(self, original: TextIO):
        self._original = original
        self._original_context = None
        self._copy = StringIO()

    def write(self, s):
        self._get_original().write(s)
        self._copy.write(s)

    def writelines(self, lines):
        self._get_original().writelines(lines)
        self._copy.writelines(lines)

    def flush(self):
        self._get_original().flush()
        self._copy.flush()

    def get_copy(self) -> str:
        return self._copy.getvalue()

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
        kernel = PythonKernel()
        kg.connect_to_knime(kernel)
        py4j.clientserver.server_connection_stopped.connect(
            lambda *args, **kwargs: kernel.exit_main_loop()
        )
        kernel.enter_main_loop()
    finally:
        if kg.client_server is not None:
            kg.client_server.shutdown()
