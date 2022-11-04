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
import os
import pickle
import py4j.clientserver
import sys
import warnings
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from py4j.java_collections import JavaArray, ListConverter
from py4j.java_gateway import JavaClass
from typing import Any, Callable, Dict, List, Optional, TextIO

import knime_table as kt
import knime_arrow_table as kat

import knime.api.table as ktn
import knime.api.views as kv
import knime._arrow._table as katn

import knime.scripting._io_containers as _ioc

import knime_gateway as kg
from knime_main_loop import MainLoop
from autocompletion_utils import disable_autocompletion


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

    def check_output_table(self, table_index, table):
        if table is None or not isinstance(table, kat._ArrowWriteTableImpl):
            type_str = type(table) if table is not None else "None"
            raise TypeError(
                f"Expected a WriteTable in output_tables[{table_index}], got {type_str}. "
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
        kt._backend.close()
        kt._backend = None

    def get_output_view(self):
        raise NotImplementedError(
            "The deprecated knime_io backend does not support views. "
            + "Please import knime.scripting.io"
        )


class ScriptingBackendV1(ScriptingBackend):
    """
    Current scripting backend provided by knime.scripting.io
    """

    def check_output_table(self, table_index, table):
        if table is None or (
            not isinstance(table, katn.ArrowTable)
            and not isinstance(table, ktn._TabularView)
            and not isinstance(table, katn.ArrowBatchOutputTable)
        ):
            type_str = type(table) if table is not None else "None"
            raise TypeError(
                f"Output table '{table_index}' must be of type knime.api.Table or knime.api.BatchOutputTable, but got {type_str}"
            )

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
                _ioc._output_tables[idx] = table._sink._java_data_sink
            else:
                raise TypeError(
                    f"Output table '{idx}' must be of type knime.api.Table or knime.api.BatchOutputTable, but got {type(table)}"
                )

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

    def set_input_table(
        self, table_index: int, java_table_data_source: Optional[JavaClass]
    ):
        """
        Here we prepare the input table by creating an ArrowDataSource. As soon as the user imports
        either 'knime_io' or 'knime.scripting.io', the data sources will be converted to the appropriate
        table representation.
        """
        if java_table_data_source is not None:
            # NB: We don't close the source. It must be available for the lifetime of the process.
            table_data_source = kg.data_source_mapper(java_table_data_source)
        else:
            table_data_source = None
        _ioc._pad_up_to_length(_ioc._input_tables, table_index + 1)
        _ioc._input_tables[table_index] = table_data_source

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
            raise RuntimeError(
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

    def set_num_expected_output_tables(self, num_output_tables: int) -> None:
        _ioc._pad_up_to_length(_ioc._output_tables, num_output_tables)

    def release_input_tables(self):
        for table in _ioc._input_tables:
            table._source.close()

    def set_input_object(self, object_index: int, path: Optional[str]) -> None:
        if path is not None:
            with open(path, "rb") as file:
                obj = pickle.load(file)
        else:
            obj = None
        _ioc._pad_up_to_length(_ioc._input_objects, object_index + 1)
        _ioc._input_objects[object_index] = obj

    def set_num_expected_output_objects(self, num_output_objects: int) -> None:
        _ioc._pad_up_to_length(_ioc._output_objects, num_output_objects)

    def get_output_object(self, object_index: int, path: str) -> None:
        obj = _ioc._output_objects[object_index]
        with open(path, "wb") as file:
            pickle.dump(obj=obj, file=file)

    def get_output_object_type(self, object_index: int) -> str:
        return type(_ioc._output_objects[object_index]).__name__

    def get_output_object_string_representation(self, object_index: int) -> str:
        object_as_string = str(_ioc._output_objects[object_index])
        return (
            (object_as_string[:996] + "\n...")
            if len(object_as_string) > 1000
            else object_as_string
        )

    def set_num_expected_output_images(self, num_output_images: int) -> None:
        _ioc._pad_up_to_length(_ioc._output_images, num_output_images)

    def get_output_image(
        self,
        image_index: int,
        path: str,
    ) -> None:
        image = _ioc._output_images[image_index]
        with open(path, "wb") as file:
            file.write(image)

    def set_expected_output_view(self, expect_view: bool):
        self._expect_view = expect_view

    def get_output_view(self, view_sink):
        view_sink.display(self.get_active_backend_or_raise().get_output_view())

    def check_outputs(self):
        for i, o in enumerate(_ioc._output_tables):
            self.get_active_backend_or_raise().check_output_table(i, o)

        for i, o in enumerate(_ioc._output_objects):
            if o is None:
                raise ValueError(
                    f"Expected an object in output_objects[{i}], got {type(o)}"
                )

        for i, o in enumerate(_ioc._output_images):
            if o is None:
                raise ValueError(
                    f"Expected an image in output_images[{i}], got {type(o)}"
                )

        self._check_flow_variables()

        if self._expect_view:
            v = self.get_active_backend_or_raise().get_output_view()
            if v is None or not isinstance(v, kv.NodeView):
                raise ValueError(
                    f"Expected an output view in output_view, got {type(v)}"
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
                raise TypeError(
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


class PythonKernel(kg.EntryPoint):
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
        :return: The callback on the Java side of Java type
        org.knime.python3.scripting.Python3KernelBackendProxy.Callback.
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

    def setFlowVariables(self, flow_variables: Dict[str, Any]) -> None:
        self._backends.set_flow_variables(flow_variables)

    def getFlowVariables(self) -> JavaClass:
        return self._backends.get_flow_variables()

    # TODO: at some point in the future, we should change this method such that it accepts all input tables at once
    #  instead of setting the tables one by one. The same applies to the way back to Java as well as to the
    #  corresponding methods dealing with pickled objects and images.
    def setInputTable(
        self, table_index: int, java_table_data_source: Optional[JavaClass]
    ):
        self._backends.set_input_table(table_index, java_table_data_source)

    def releaseInputTables(self):
        self._backends.release_input_tables()

    def setNumExpectedOutputTables(self, num_output_tables: int) -> None:
        self._backends.set_num_expected_output_tables(num_output_tables)

    def getOutputTable(
        self,
        table_index: int,
    ) -> Optional[JavaClass]:
        return self._backends.get_output_table_sink(table_index)

    def setInputObject(self, object_index: int, path: Optional[str]) -> None:
        self._backends.set_input_object(object_index, path)

    def setNumExpectedOutputObjects(self, num_output_objects: int) -> None:
        self._backends.set_num_expected_output_objects(num_output_objects)

    def getOutputObject(self, object_index: int, path: str) -> None:
        return self._backends.get_output_object(object_index, path)

    def getOutputObjectType(self, object_index: int) -> str:
        return self._backends.get_output_object_type(object_index)

    def getOutputObjectStringRepresentation(self, object_index: int) -> str:
        return self._backends.get_output_object_string_representation(object_index)

    def setNumExpectedOutputImages(self, num_output_images: int) -> None:
        self._backends.set_num_expected_output_images(num_output_images)

    def getOutputImage(
        self,
        image_index: int,
        path: str,
    ) -> None:
        self._backends.get_output_image(image_index, path)

    def setExpectedOutputView(self, expect_view):
        self._backends.set_expected_output_view(expect_view)

    def getOutputView(self, java_view_sink):
        self._backends.get_output_view(kg.data_sink_mapper(java_view_sink))

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
                current_line = source_code.split("\n")[line][:column]
                completions = []
                line += 1

                if not disable_autocompletion(current_line):
                    try:
                        # Use jedi's 0.16.0+ API.
                        # HACK: According to the doc path should be '' but this does not work
                        # See https://github.com/davidhalter/jedi/issues/1888
                        completions = jedi.Script(source_code, path=None).complete(
                            line,
                            column,
                        )
                    except AttributeError:
                        # Fall back to jedi's older API. ("complete" raises the AttributeError caught here.)
                        completions = jedi.Script(
                            source_code, line, column, None
                        ).completions()

                for completion in completions:
                    if completion.name.startswith("_"):
                        # skip all private members
                        break

                    # HACK: Workaround for https://github.com/davidhalter/jedi/issues/1890
                    try:
                        docstring = completion.docstring()
                    except AttributeError:
                        docstring = ""

                    suggestions.append(
                        {
                            "name": completion.name,
                            "type": completion.type,
                            "doc": docstring,
                        }
                    )
            except Exception:  # Autocomplete is purely optional. So a broad exception clause should be fine.
                warnings.warn("An error occurred while autocompleting.")
        return ListConverter().convert(suggestions, kg.client_server._gateway_client)

    def executeOnMainThread(self, source_code: str, check_outputs: bool) -> List[str]:
        return self._main_loop.execute(self._execute, source_code, check_outputs)

    def executeOnCurrentThread(self, source_code: str) -> List[str]:
        return self._execute(source_code)

    def _execute(self, source_code: str, check_outputs: bool = False) -> List[str]:
        def create_python_sink():
            java_sink = self._java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        with redirect_stdout(_CopyingTextIO(sys.stdout)) as stdout, redirect_stderr(
            _CopyingTextIO(sys.stderr)
        ) as stderr:
            self._backends.set_up_arrow(create_python_sink)
            exec(source_code, self._workspace)
            if check_outputs:
                self._backends.check_outputs()

            # We only need to flush the tables to disk if we also expect the outputs to be
            # filled, meaning we run the whole script.
            self._backends.tear_down_arrow(flush=check_outputs)
        return ListConverter().convert(
            [stdout.get_copy(), stderr.get_copy()], kg.client_server._gateway_client
        )

    class Java:
        implements = ["org.knime.python3.scripting.Python3KernelBackendProxy"]


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
        warnings.filterwarnings("default", category=DeprecationWarning)
        kernel = PythonKernel()
        kg.connect_to_knime(kernel)
        py4j.clientserver.server_connection_stopped.connect(
            lambda *args, **kwargs: kernel._main_loop.exit()
        )
        kernel._main_loop.enter()
    finally:
        if kg.client_server is not None:
            kg.client_server.shutdown()
