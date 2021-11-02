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

import pyarrow as pa
import sys
import pickle
from typing import Dict, List, Optional, Tuple, Union
import warnings
from py4j.java_collections import ListConverter
from py4j.java_gateway import JavaClass


import knime_arrow_table as kat
import knime_gateway as kg


class PythonKernel(kg.EntryPoint):
    def __init__(self):
        self._workspace = {}  # TODO: should we make this thread safe?

    def putFlowVariablesIntoWorkspace(self, name: str, java_flow_variables) -> None:
        self._workspace[name] = dict(java_flow_variables)

    def getFlowVariablesFromWorkspace(self, name: str) -> JavaClass:
        flow_variables = self._workspace[name]
        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in flow_variables.keys():
            flow_variable = flow_variables[key]
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

    def putTableIntoWorkspace(
        self,
        variable_name: str,
        java_table_data_source,
        num_rows: int,
        sentinel: Optional[Union[str, int]] = None,
    ) -> None:
        with kg.data_source_mapper(java_table_data_source) as table_data_source:
            read_table = kat.ArrowReadTable(table_data_source)
            data_frame = read_table.to_pandas(rows=num_rows, sentinel=sentinel)
            # The first column of a KNIME table is interpreted as its index (row keys).
            data_frame.set_index(data_frame.columns[0], inplace=True)
            self._workspace[variable_name] = data_frame


    def writeImageToPath(
        self,
        image_name: str,
        path: str,
    ) -> None:
        image = self._workspace[image_name]
        with open(path, "wb") as file:
            file.write(image)

    def pickleObjectToFile(self, object_name: str, path: str):
        obj = self._workspace[object_name]
        with open(path, "wb") as file:
            pickle.dump(obj=obj, file=file)

    def getObjectType(self, object_name: str) -> str:
        return type(self._workspace[object_name]).__name__

    def getObjectStringRepresentation(self, object_name: str) -> str:
        object_as_string = str(self._workspace[object_name])
        return (
            (object_as_string[:996] + "\n...")
            if len(object_as_string) > 1000
            else object_as_string
        )

    def loadPickledObjectIntoWorkspace(self, object_name: str, path: str):
        with open(path, "rb") as file:
            self._workspace[object_name] = pickle.load(file)

    def listVariablesInWorkspace(self) -> List[Dict[str, str]]:
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
                        line, column,
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
        # TODO: py4j's auto-conversion does not work for some reason. It should be enabled...
        return ListConverter().convert(suggestions, kg.client_server._gateway_client)

    class Java:
        implements = ["org.knime.python3for2.Python3KernelBackendProxy"]


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

    kernel = PythonKernel()
    kg.connect_to_knime(kernel)
