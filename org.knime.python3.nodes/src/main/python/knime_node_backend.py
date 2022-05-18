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

from typing import List, Tuple, Union

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


class _PythonPortObject:
    def __init__(self, java_class_name):
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> str:
        return self._java_class_name

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObject"
        ]


class _PythonTablePortObject:
    def __init__(self, java_class_name, sink):
        self._java_class_name = java_class_name
        self._sink = sink

    def getJavaClassName(self) -> str:
        return self._java_class_name

    def getPythonArrowDataSink(self):
        return self._sink

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonTablePortObject"
        ]


class _PythonBinaryPortObject:
    def __init__(self, java_class_name, filestore_file, data, id):
        self._java_class_name = java_class_name
        self._data = data
        self._id = id
        self._key = filestore_file.get_key()

        with open(filestore_file.get_file_path(), "wb") as f:
            f.write(data)

    def getJavaClassName(self) -> str:
        return self._java_class_name

    def getPortId(self):
        return self._id

    def getFileStoreKey(self):
        return self._key

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonBinaryPortObject"
        ]


class _PythonPortObjectSpec:
    def __init__(self, java_class_name, json_string_data):
        self._java_class_name = java_class_name
        self._json_string_data = json_string_data

    def getJavaClassName(self) -> str:
        return self._java_class_name

    def toJsonString(self) -> str:
        return self._json_string_data

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObjectSpec"
        ]


def _spec_to_python(spec: _PythonPortObjectSpec, port: kn.Port):
    # TODO: use extension point,
    #       see https://knime-com.atlassian.net/browse/AP-18368
    class_name = spec.getJavaClassName()
    data = json.loads(spec.toJsonString())
    if class_name == "org.knime.core.data.DataTableSpec":
        assert port.type == kn.PortType.TABLE
        return ks.Schema.from_knime_dict(data)
    elif class_name == "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec":
        assert port.type == kn.PortType.BYTES
        bpos = ks.BinaryPortObjectSpec.from_knime_dict(data)
        assert (
            bpos.id == port.id
        ), f"Expected binary input port ID {port.id} but got {bpos.id}"
        return bpos

    raise TypeError("Unsupported PortObjectSpec found in Python, got " + class_name)


def _spec_from_python(spec, port: kn.Port):
    # TODO: use extension point,
    #       see https://knime-com.atlassian.net/browse/AP-18368
    if port.type == kn.PortType.TABLE:
        assert isinstance(spec, ks.Schema)
        data = spec.to_knime_dict()
        class_name = "org.knime.core.data.DataTableSpec"
    elif port.type == kn.PortType.BYTES:
        assert isinstance(spec, ks.BinaryPortObjectSpec)
        assert (
            port.id == spec.id
        ), f"Expected binary output port ID {port.id} but got {spec.id}"

        data = spec.to_knime_dict()
        class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec"
    else:
        raise ValueError("Configure got unsupported PortObject")

    return _PythonPortObjectSpec(class_name, json.dumps(data))


def _port_object_to_python(port_object: _PythonPortObject, port: kn.Port):
    # TODO: use extension point,
    #       see https://knime-com.atlassian.net/browse/AP-18368
    class_name = port_object.getJavaClassName()

    if class_name == "org.knime.core.node.BufferedDataTable":
        assert port.type == kn.PortType.TABLE
        java_source = port_object.getDataSource()
        return kat.ArrowReadTable(kg.data_source_mapper(java_source))
    elif (
        class_name
        == "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
    ):
        assert port.type == kn.PortType.BYTES
        file = port_object.getFilePath()
        with open(file, "rb") as f:
            return f.read()

    raise TypeError("Unsupported PortObjectSpec found in Python, got " + class_name)


def _port_object_from_python(obj, file_creator, port: kn.Port) -> _PythonPortObject:
    # TODO: use extension point,
    #       see https://knime-com.atlassian.net/browse/AP-18368
    if port.type == kn.PortType.TABLE:
        assert isinstance(obj, kt.WriteTable)
        class_name = "org.knime.core.node.BufferedDataTable"
        return _PythonTablePortObject(class_name, obj._sink._java_data_sink)
    elif port.type == kn.PortType.BYTES:
        assert isinstance(obj, bytes)
        class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
        return _PythonBinaryPortObject(class_name, file_creator(), obj, port.id)
    else:
        raise ValueError("Configure got unsupported PortObject")


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

    def getParameters(self) -> str:
        parameters_dict = knp.extract_parameters(self._node)
        return json.dumps(parameters_dict)

    def getSchema(self) -> str:
        schema = knp.extract_schema(self._node)
        return json.dumps(schema)

    def setParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        knp.inject_parameters(self._node, parameters_dict, version)

    def validateParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        return knp.validate_parameters(self._node, parameters_dict, version)

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        self._java_callback = java_callback

    def execute(
        self, input_objects: List[_PythonPortObject], exec_context
    ) -> List[_PythonPortObject]:
        inputs = [
            _port_object_to_python(po, self._node.input_ports[idx])
            for idx, po in enumerate(input_objects)
        ]

        # prepare output table creation
        def create_python_sink():
            java_sink = self._java_callback.create_sink()
            return kg.data_sink_mapper(java_sink)

        kt._backend = kat.ArrowBackend(create_python_sink)

        # execute
        outputs = self._node.execute(inputs, exec_context)

        kt._backend.close()
        kt._backend = None

        java_outputs = [
            _port_object_from_python(
                obj,
                lambda: self._java_callback.create_filestore_file(),
                self._node.output_ports[idx],
            )
            for idx, obj in enumerate(outputs)
        ]

        return ListConverter().convert(java_outputs, kg.client_server._gateway_client)

    def configure(
        self, input_specs: List[_PythonPortObjectSpec]
    ) -> List[_PythonPortObjectSpec]:

        inputs = [
            _spec_to_python(spec, self._node.input_ports[i])
            for i, spec in enumerate(input_specs)
        ]
        outputs = self._node.configure(inputs)

        output_specs = [
            _spec_from_python(spec, self._node.output_ports[i])
            for i, spec in enumerate(outputs)
        ]
        return ListConverter().convert(output_specs, kg.client_server._gateway_client)

    class Java:
        implements = ["org.knime.python3.nodes.proxy.NodeProxy"]


class _KnimeNodeBackend(kg.EntryPoint):
    def __init__(self) -> None:
        super().__init__()
        self._callback = None

    def createNodeExtensionProxy(
        self, factory_module_name: str, factory_method_name: str, node_id: str
    ) -> _PythonNodeProxy:
        factory_module = importlib.import_module(factory_module_name)
        factory_method = getattr(factory_module, factory_method_name)
        node = factory_method(node_id)
        return _PythonNodeProxy(node)

    def createNodeProxy(
        self, module_name: str, python_node_class: str
    ) -> _PythonNodeProxy:
        module = importlib.import_module(module_name)
        node_class = getattr(module, python_node_class)
        node = node_class()  # TODO assumes that there is an empty constructor
        return _PythonNodeProxy(node)

    def initializeJavaCallback(self, callback):
        self._callback = callback

    def retrieveNodesAsJson(self, extension_module_name: str) -> str:
        importlib.import_module(extension_module_name)
        node_dicts = [self.resolve_node_dict(n) for n in kn._nodes.values()]
        return json.dumps(node_dicts)

    def resolve_node_dict(self, node: kn._Node):
        d = node.to_dict()
        instance = node.node_factory()
        description = self.extract_description(instance, node.name)
        return {**d, **description}

    def extract_description(self, node: kn.PythonNode, name) -> dict:
        doc = node.__doc__
        lines = doc.splitlines()
        if len(lines) == 0:
            short_description = "Please document your node class with a docstring"
            logging.warning(
                f"No docstring available for node {name}. Please document the node class with a docstring or set __doc__ in the init of the node."
            )
        else:
            short_description = lines[0]
        if len(lines) > 1:
            full_description = "\n".join(lines[1:])
        else:
            full_description = short_description

        param_doc = knp.extract_parameter_docs(node)
        # TODO support for tabs
        # TODO support for ports
        # TODO support for views
        return {
            "short_description": short_description,
            "full_description": full_description,
            "options": param_doc,
        }

    def createNodeFromExtension(
        self, extension_module_name: str, node_id: str
    ) -> _PythonNodeProxy:
        importlib.import_module(extension_module_name)
        node_info = kn._nodes[node_id]
        node = node_info.node_factory()
        return _PythonNodeProxy(node)

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
