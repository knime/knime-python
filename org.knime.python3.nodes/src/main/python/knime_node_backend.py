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
Backend for KNIME nodes written in Python. Handles the communication with Java.

@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

import knime_node as kn
import knime_node_parameter as knp
import knime_gateway as kg
import knime_schema as ks
import knime_arrow_table as kat
import knime_table as kt
import importlib
import json
import pickle
import logging
from py4j.java_gateway import JavaClass
from py4j.java_collections import ListConverter

# TODO currently not part of our dependencies but maybe worth adding instead of reimplementing here.
# TODO an alternative could be the use of tuples since we are only interested in comparability
from packaging.version import parse

# TODO: register extension types


class _PythonNodeProxy:
    def __init__(self, node: kn.PythonNode) -> None:
        self._node = node

    def getDialogRepresentation(self, parameters: str, parameters_version: str, specs):
        self.setParameters(parameters, parameters_version)
        # TODO construct json forms here instead of in the node so that it isn't exposed as our API
        json_forms_dict = {
            "data": knp.extract_parameters(self._node),
            "schema": knp.extract_schema(self._node),
            "ui_schema": knp.extract_ui_schema(self._node),
        }
        return json.dumps(json_forms_dict)

    def getInitialParameters(self) -> str:
        parameters_dict = knp.extract_parameters(self._node)
        return json.dumps(parameters_dict)

    def setParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        knp.inject_parameters(self._node, parameters_dict, version)

    def validateParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        return knp.validate_parameters(self._node, parameters_dict, version)

    # TODO turn this stub into a proper implementation
    def getDescription(self) -> str:
        description_dict = self._node.get_description()
        return json.dumps(description_dict)

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        self._java_callback = java_callback

    def execute(
        self, in_sources, in_object_paths, out_object_paths, exec_context
    ) -> list:
        # convert sources to tables
        tables = [kat.ArrowReadTable(kg.data_source_mapper(s)) for s in in_sources]

        # unpickle object from file paths
        objects = []
        for path in in_object_paths:
            if path is not None:
                with open(path, "rb") as file:
                    objects.append(pickle.load(file))

        # prepare output table creation
        def create_python_sink():
            java_sink = self._java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        kt._backend = kat.ArrowBackend(create_python_sink)

        # execute
        out_tables, out_objects = self._node.execute(tables, objects, exec_context)
        kt._backend.close()
        kt._backend = None

        # get Java sink from WriteTables:
        out_sinks = [t._sink._java_data_sink for t in out_tables]

        # pickle objects to the specified output paths
        for obj, path in zip(out_objects, out_object_paths):
            with open(path, "wb") as file:
                pickle.dump(obj=obj, file=file)

        return ListConverter().convert(out_sinks, kg.client_server._gateway_client)

    def configure(self, in_schemas: list[ks.Schema]) -> list[ks.Schema]:
        knime_in_schemas = [
            ks.Schema.from_knime_dict(json.loads(i)) for i in in_schemas
        ]
        knime_out_schemas = self._node.configure(knime_in_schemas)
        out_str_schemas = [json.dumps(s.to_knime_dict()) for s in knime_out_schemas]
        return ListConverter().convert(
            out_str_schemas, kg.client_server._gateway_client
        )

    class Java:
        implements = ["org.knime.python3.nodes.proxy.NodeProxy"]


class _KnimeNodeBackend(kg.EntryPoint):
    def __init__(self) -> None:
        super().__init__()
        self._callback = None

    def createNodeProxy(
        self, module_name: str, python_node_class: str
    ) -> _PythonNodeProxy:
        module = importlib.import_module(module_name)
        node_class = getattr(module, python_node_class)
        node = node_class()  # TODO assumes that there is an empty constructor
        return _PythonNodeProxy(node)

    def initializeJavaCallback(self, callback):
        self._callback = callback

    class Java:
        implements = ["org.knime.python3.nodes.KnimeNodeBackend"]


class KnimeLogHandler(logging.StreamHandler):
    def __init__(self, backend):
        super().__init__(self)
        self.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        self._backend = backend

    def emit(self, record: logging.LogRecord):
        if self._backend._callback is not None:
            msg = self.format(record)
            self._backend._callback.log(msg)


backend = _KnimeNodeBackend()
kg.connect_to_knime(backend)
logging.getLogger().addHandler(KnimeLogHandler(backend))
# I'd like to set the log level to INFO here, but py4j logs some stuff to info while
# calling into Java and then our log redirection into Java is biting itself :D so no
# global log setting to INFO possible without other hacks (I didn't google yet).
