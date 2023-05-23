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

from typing import Any, Dict, List, Optional, Type, Union

from knime._backend._mainloop import MainLoop

import knime._backend._gateway as kg
import knime.extension.nodes as kn
import knime.extension.parameter as kp
import knime.api.schema as ks

import knime._arrow._table as kat
import knime._views  # to register the NodeViewSink
import knime.api.table as kt
import importlib
import json
import logging

import traceback
import collections

from py4j.java_gateway import JavaClass
from py4j.java_collections import ListConverter
import py4j.clientserver

# TODO: register extension types


class _PythonPortObject:
    def __init__(self, java_class_name):
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObject"
        ]


class _PythonTablePortObject:
    def __init__(self, java_class_name, sink):
        self._java_class_name = java_class_name
        self._sink = sink

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def getPythonArrowDataSink(self):  # NOSONAR - Java naming conventions
        return self._sink

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonTablePortObject"
        ]


class _PythonPortObjectSpec:
    def __init__(self, java_class_name, data_dict: Dict):
        self._java_class_name = java_class_name
        self._json_string_data = json.dumps(data_dict)
        self._data = data_dict

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def toJsonString(self) -> str:  # NOSONAR - Java naming conventions
        return self._json_string_data

    def toString(self) -> str:  # NOSONAR - Java naming conventions
        """For debugging on the Java side"""
        return self._json_string_data

    @property
    def data(self) -> Dict:
        if self._data is None:
            # to be able to access the data dict when we're accessing a Java class
            self._data = json.loads(self.toJsonString())
        return self._data

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObjectSpec"
        ]


class _PythonBinaryPortObject:
    def __init__(self, java_class_name, filestore_file, data, spec):
        self._java_class_name = java_class_name
        self._data = data
        self._spec = spec
        self._key = filestore_file.get_key()

        with open(filestore_file.get_file_path(), "wb") as f:
            f.write(data)

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def getSpec(self) -> _PythonPortObjectSpec:  # NOSONAR - Java naming conventions
        return self._spec

    def getFileStoreKey(self) -> str:  # NOSONAR - Java naming conventions
        return self._key

    def toString(self) -> str:  # NOSONAR - Java naming conventions
        """For debugging on the Java side"""
        return f"PythonBinaryPortObject[spec={self._spec.toJsonString()}]"

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonBinaryPortObject"
        ]


class _PythonConnectionPortObject:
    def __init__(self, java_class_name, spec):
        self._java_class_name = java_class_name
        self._spec = spec

    def getPid(self) -> int:  # NOSONAR - Java naming conventions
        """Used on the Java side to obtain the Python process ID"""
        import os

        return os.getpid()

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def getSpec(self) -> _PythonPortObjectSpec:  # NOSONAR - Java naming conventions
        return self._spec

    def toString(self) -> str:  # NOSONAR - Java naming conventions
        """For debugging on the Java side"""
        return f"PythonConnectionPortObject[spec={self._spec.toJsonString()}]"

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonConnectionPortObject"
        ]


class _PythonImagePortObject:
    def __init__(self, java_class_name, data):
        import base64

        self._java_class_name = java_class_name
        self._img_bytes = base64.b64encode(data).decode("utf-8")

    def getJavaClassName(self) -> str:  # NOSONAR
        return self._java_class_name

    def getImageBytes(self) -> str:  # NOSONAR
        return self._img_bytes

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonImagePortObject"
        ]


class _FlowVariablesDict(collections.UserDict):
    def __init__(self):
        super().__init__({})
        self._locked = False

    def __setitem__(self, key: str, value) -> None:
        if not isinstance(key, str):
            raise TypeError("the flow variable name must be a string")
        if self._locked and key.startswith("knime"):
            raise ValueError(
                "setting or changing a flow variable that starts with 'knime' is not allowed"
            )
        super().__setitem__(key, value)

    def __delitem__(self, _) -> None:
        raise RuntimeError("deleting flow variables is not allowed")


def _check_attr_is_available(node, attr_name):
    if not hasattr(node, attr_name) or getattr(node, attr_name) is None:
        raise ValueError(f"Attribute {attr_name} is missing in node {node}")


class _PortTypeRegistry:
    # One global dictionary for all connections
    _connection_port_data = {}

    def __init__(self, extension_id: str) -> None:
        self._extension_id = extension_id
        self._port_types_by_object_class = {}
        self._port_types_by_spec_class = {}
        self._port_types_by_id = {}

    def register_port_type(
        self,
        name: str,
        object_class: Type[kn.PortObject],
        spec_class: Type[kn.PortObjectSpec],
        id: Optional[str] = None,
    ):
        if object_class in self._port_types_by_object_class:
            raise ValueError(
                f"There is already a port type with the provided object class '{object_class}' registered."
            )
        if spec_class in self._port_types_by_spec_class:
            raise ValueError(
                f"There is already a port type with the provided spec class '{spec_class}' registered."
            )
        if id is None:
            id = f"{self._extension_id}.{object_class.__module__}.{object_class.__qualname__}"

        if id in self._port_types_by_id:
            raise ValueError(
                f"There is already a port type with the provided id '{id}' registered."
            )

        port_type = kn.PortType(id, name, object_class, spec_class)

        self._port_types_by_object_class[object_class] = port_type
        self._port_types_by_spec_class[spec_class] = port_type
        self._port_types_by_id[id] = port_type
        return port_type

    def spec_to_python(self, spec: _PythonPortObjectSpec, port: kn.Port):
        class_name = spec.getJavaClassName()
        data = json.loads(spec.toJsonString())

        def deserialize_custom_spec():
            spec_id = data["id"]
            assert (
                spec_id == port.type.id
            ), f"Expected input port ID {port.type.id} but got {spec_id}"
            assert (
                spec_id in self._port_types_by_id
            ), f"There is no port type with id '{spec_id}' registered."
            return port.type.spec_class.deserialize(data["data"])

        if class_name == "org.knime.core.data.DataTableSpec":
            assert port.type == kn.PortType.TABLE
            return ks.Schema.deserialize(data)
        elif (
            class_name == "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec"
        ):
            if port.type == kn.PortType.BINARY:
                bpos = ks.BinaryPortObjectSpec.deserialize(data)
                assert (
                    bpos.id == port.id
                ), f"Expected binary input port ID {port.id} but got {bpos.id}"
                return bpos
            else:  # custom spec
                return deserialize_custom_spec()
        elif (
            class_name
            == "org.knime.python3.nodes.ports.PythonTransientConnectionPortObjectSpec"
        ):
            assert port.type not in [kn.PortType.TABLE, kn.PortType.BINARY]
            assert issubclass(port.type.object_class, kn.ConnectionPortObject)
            return deserialize_custom_spec()

        raise TypeError("Unsupported PortObjectSpec found in Python, got " + class_name)

    def spec_from_python(
        self, spec, port: kn.Port, node_id: str, port_idx: int
    ) -> _PythonPortObjectSpec:
        if port.type == kn.PortType.TABLE:
            if isinstance(spec, ks._ColumnarView):
                spec = spec.get()
            if isinstance(spec, ks.Column):
                spec = ks.Schema.from_columns(spec)
            assert isinstance(spec, ks.Schema)
            data = spec.serialize()
            class_name = "org.knime.core.data.DataTableSpec"
        elif port.type == kn.PortType.BINARY:
            assert isinstance(spec, ks.BinaryPortObjectSpec)
            assert (
                port.id == spec.id
            ), f"Expected binary output port ID {port.id} but got {spec.id}"

            data = spec.serialize()
            class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec"
        elif port.type == kn.PortType.IMAGE:
            assert isinstance(spec, ks.ImagePortObjectSpec)
            assert any(
                spec.format == option.value for option in kn.ImageFormat
            ), f"Expected image formats are: {kn.ImageFormat.available_options()}."

            data = spec.serialize()
            class_name = "org.knime.core.node.port.image.ImagePortObjectSpec"
        else:  # custom spec
            assert (
                port.type.id in self._port_types_by_id
            ), f"There is no port type with id '{port.type.id}' registered"
            assert isinstance(
                spec, port.type.spec_class
            ), f"Expected output spec of type {port.type.spec_class} but got spec of type {type(spec)}"
            data = {"id": port.type.id, "data": spec.serialize()}

            if issubclass(port.type.object_class, kn.ConnectionPortObject):
                data["node_id"] = node_id
                data["port_idx"] = port_idx
                class_name = "org.knime.python3.nodes.ports.PythonTransientConnectionPortObjectSpec"
            else:
                class_name = (
                    "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec"
                )

        return _PythonPortObjectSpec(class_name, data)

    def port_object_to_python(self, port_object: _PythonPortObject, port: kn.Port):
        class_name = port_object.getJavaClassName()

        def read_port_object_data() -> Union[Any, kn.PortObject]:
            file = port_object.getFilePath()
            with open(file, "rb") as f:
                return f.read()

        if class_name == "org.knime.core.node.BufferedDataTable":
            assert port.type == kn.PortType.TABLE
            java_source = port_object.getDataSource()
            return kat.ArrowSourceTable(kg.data_source_mapper(java_source))
        elif (
            class_name
            == "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
        ):
            data = read_port_object_data()
            if port.type == kn.PortType.BINARY:
                return data
            else:
                spec = self.spec_to_python(port_object.getSpec(), port)
                return port.type.object_class.deserialize(spec, data)
        elif (
            class_name
            == "org.knime.python3.nodes.ports.PythonTransientConnectionPortObject"
        ):
            assert issubclass(
                port.type.object_class, kn.ConnectionPortObject
            ), f"unexpected port type {port.type}"
            spec = self.spec_to_python(port_object.getSpec(), port)

            data = json.loads(port_object.getSpec().toJsonString())
            key = f'{data["node_id"]}:{data["port_idx"]}'
            if key not in _PortTypeRegistry._connection_port_data:
                raise KeyError(
                    f'No connection data found for node {data["node_id"]}, port {data["port_idx"]}. '
                    + "Please re-execute the upstream node providing the connection."
                )

            connection_data = _PortTypeRegistry._connection_port_data[key]
            return port.type.object_class.deserialize(spec, connection_data)

        raise TypeError("Unsupported PortObjectSpec found in Python, got " + class_name)

    def port_object_from_python(
        self, obj, file_creator, port: kn.Port, node_id: str, port_idx: int
    ) -> _PythonPortObject:
        if port.type == kn.PortType.TABLE:
            if isinstance(obj, kt._TabularView):
                obj = obj.get()
            class_name = "org.knime.core.node.BufferedDataTable"

            java_data_sink = None
            if isinstance(obj, kat.ArrowTable):
                sink = kt._backend.create_sink()
                obj._write_to_sink(sink)
                java_data_sink = sink._java_data_sink
            elif isinstance(obj, kat.ArrowBatchOutputTable):
                java_data_sink = obj._sink._java_data_sink
            else:
                raise TypeError(
                    f"Object for port {port} should be of type Table or BatchOutputTable, but got {type(obj)}"
                )

            return _PythonTablePortObject(class_name, java_data_sink)
        elif port.type == kn.PortType.BINARY:
            if not isinstance(obj, bytes):
                tb = None
                raise TypeError(
                    f"Binary Port can only process objects of type bytes, not {type(obj)}"
                ).with_traceback(tb)

            class_name = (
                "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
            )
            spec = _PythonPortObjectSpec(
                "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec",
                {"id": port.id},
            )
            return _PythonBinaryPortObject(class_name, file_creator(), obj, spec)
        elif port.type == kn.PortType.IMAGE:
            class_name = "org.knime.core.node.port.image.ImagePortObject"
            return _PythonImagePortObject(class_name, obj)
        else:
            assert (
                port.type.id in self._port_types_by_id
            ), f"There is no port type with '{id}' registered"
            assert isinstance(
                obj, port.type.object_class
            ), f"Expected output object of type {port.type.object_class}, got object of type {type(obj)}"
            spec = self.spec_from_python(obj.spec, port, node_id, port_idx)

            if issubclass(port.type.object_class, kn.ConnectionPortObject):
                class_name = (
                    "org.knime.python3.nodes.ports.PythonTransientConnectionPortObject"
                )

                # We store the port object data in a global dict referenced by nodeID and portIdx
                key = f"{node_id}:{port_idx}"
                _PortTypeRegistry._connection_port_data[key] = obj.serialize()
                return _PythonConnectionPortObject(class_name, spec)
            else:
                class_name = (
                    "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
                )
                serialized = obj.serialize()
                return _PythonBinaryPortObject(
                    class_name, file_creator(), serialized, spec
                )


class _PythonNodeProxy:
    def __init__(
        self, node: kn.PythonNode, port_type_registry: _PortTypeRegistry, knime_parser
    ) -> None:
        _check_attr_is_available(node, "input_ports")
        _check_attr_is_available(node, "output_ports")

        self._node = node
        self._num_outports = len(node.output_ports)
        self._port_type_registry = port_type_registry
        self._knime_parser = knime_parser

    def getDialogRepresentation(
        self,
        parameters: str,
        extension_version: str,
        python_dialog_context,
    ):
        self.setParameters(parameters, extension_version)

        dialog_context = kn.DialogCreationContext(
            python_dialog_context, self._get_flow_variables()
        )
        specs = dialog_context.get_input_specs()

        inputs = self._specs_to_python(specs)

        json_forms_dict = {
            "data": kp.extract_parameters(self._node, for_dialog=True),
            "schema": kp.extract_schema(
                self._node, extension_version, inputs, dialog_context
            ),
            "ui_schema": kp.extract_ui_schema(self._node, extension_version),
        }

        self._parse_parameter_descriptions(json_forms_dict["schema"])

        return json.dumps(json_forms_dict)

    def _parse_parameter_descriptions(self, schema_dict):
        """
        Recursively parse "description" fields of the provided JSON dict from Markdown to HTML.

        Parsing is done in-place, no value is returned.
        """
        for key in schema_dict:
            if isinstance(schema_dict[key], dict):
                self._parse_parameter_descriptions(schema_dict[key])
            elif key == "description":
                schema_dict[key] = self._knime_parser.parse_option_description(
                    schema_dict[key]
                )

    def _specs_to_python(self, specs):
        return [
            self._port_type_registry.spec_to_python(spec, port)
            if spec is not None
            else None
            for port, spec in zip(self._node.input_ports, specs)
        ]

    def getParameters(self) -> str:
        parameters_dict = kp.extract_parameters(self._node)
        return json.dumps(parameters_dict)

    def getSchema(self, version=None, specs: List[str] = None) -> str:
        if specs is not None:
            specs = self._specs_to_python(specs)
        schema = kp.extract_schema(self._node, version, specs)
        return json.dumps(schema)

    def setParameters(
        self,
        parameters: str,
        parameters_version: str,
    ) -> None:
        parameters_dict = json.loads(parameters)
        kp.inject_parameters(
            self._node,
            parameters_dict,
            parameters_version,
        )

    def validateParameters(self, parameters: str, saved_version: str) -> None:
        parameters_dict = json.loads(parameters)
        try:
            kp.validate_parameters(self._node, parameters_dict, saved_version)
            return None
        except BaseException as error:
            return str(error)

    def determineCompatibility(
        self, saved_version: str, current_version: str, saved_parameters: str
    ) -> None:
        saved_parameters_dict = json.loads(saved_parameters)
        kp.determine_compatability(
            self._node, saved_version, current_version, saved_parameters_dict
        )

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        self._java_callback = java_callback

    def execute(
        self, input_objects: List[_PythonPortObject], java_exec_context
    ) -> List[_PythonPortObject]:
        _push_log_callback(lambda msg, sev: self._java_callback.log(msg, sev))

        try:
            inputs = [
                self._port_type_registry.port_object_to_python(
                    po, self._node.input_ports[idx]
                )
                for idx, po in enumerate(input_objects)
            ]

            # prepare output table creation
            def create_python_sink():
                java_sink = self._java_callback.create_sink()
                return kg.data_sink_mapper(java_sink)

            kt._backend = kat._ArrowBackend(create_python_sink)

            # execute
            exec_context = kn.ExecutionContext(
                java_exec_context, self._get_flow_variables()
            )

            # TODO: maybe we want to run execute on the main thread? use knime._backend._mainloop
            outputs = self._node.execute(exec_context, *inputs)

            # Return null if the execution was canceled
            if exec_context.is_canceled():
                self._java_callback.set_failure("Canceled", "", False)
                return None

            self._set_flow_variables(exec_context.flow_variables)

            if outputs is None:
                outputs = []

            if not isinstance(outputs, list) and not isinstance(outputs, tuple):
                # single outputs are fine
                outputs = [outputs]

            if (
                hasattr(self._node, "output_view")
                and self._node.output_view is not None
            ):
                out_view = outputs[-1]
                outputs = outputs[:-1]

                # write the view to the sink
                view_sink = kg.data_sink_mapper(self._java_callback.create_view_sink())
                view_sink.display(out_view)

            java_outputs = [
                self._port_type_registry.port_object_from_python(
                    obj,
                    lambda: self._java_callback.create_filestore_file(),
                    self._node.output_ports[idx],
                    java_exec_context.get_node_id(),
                    idx,
                )
                for idx, obj in enumerate(outputs)
            ]

        except Exception as ex:
            self._set_failure(ex, 0)
            return None
        finally:
            if kt._backend is not None:
                kt._backend.close()
                kt._backend = None

            _pop_log_callback()

        return ListConverter().convert(java_outputs, kg.client_server._gateway_client)

    def configure(
        self, input_specs: List[_PythonPortObjectSpec], java_config_context
    ) -> List[_PythonPortObjectSpec]:
        _push_log_callback(lambda msg, sev: self._java_callback.log(msg, sev))
        inputs = self._specs_to_python(input_specs)
        config_context = kn.ConfigurationContext(
            java_config_context, self._get_flow_variables()
        )
        try:
            kp.validate_specs(self._node, inputs)
            # TODO: maybe we want to run execute on the main thread? use knime._backend._mainloop
            outputs = self._node.configure(config_context, *inputs)
        except Exception as ex:
            self._set_failure(ex, 1)
            return None

        self._set_flow_variables(config_context.flow_variables)

        if outputs is None:
            # indicates that downstream nodes can't be configured, yet
            outputs = [None] * self._num_outports
        if not isinstance(outputs, list) and not isinstance(outputs, tuple):
            # single outputs are fine
            outputs = [outputs]

        output_specs = [
            self._port_type_registry.spec_from_python(
                spec, self._node.output_ports[i], java_config_context.get_node_id(), i
            )
            if spec is not None
            else None
            for i, spec in enumerate(outputs)
        ]
        _pop_log_callback()
        return ListConverter().convert(output_specs, kg.client_server._gateway_client)

    def _set_failure(self, ex: Exception, remove_tb_levels=0):
        # Remove levels from the traceback
        tb = ex.__traceback__
        for _ in range(remove_tb_levels):
            tb = ex.__traceback__.tb_next

        # Format the details: Traceback + Error type + Error
        details = "".join(traceback.format_exception(type(ex), value=ex, tb=tb))

        # Set the failure in the Java callback
        self._java_callback.set_failure(
            str(ex), details, isinstance(ex, kn.InvalidParametersError)
        )

    def _get_flow_variables(self):
        from py4j.java_collections import JavaArray

        java_flow_variables = self._java_callback.get_flow_variables()

        flow_variables = _FlowVariablesDict()
        for key, value in java_flow_variables.items():
            if isinstance(value, JavaArray):
                value = [x for x in value]
            flow_variables[key] = value
        flow_variables._locked = True
        return flow_variables

    def _check_flow_variables(self, flow_variables):
        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in flow_variables.keys():
            fv = flow_variables[key]
            try:
                java_flow_variables[key] = fv
            except AttributeError as ex:
                # py4j raises attribute errors of the form "'<type>' object has no attribute '_get_object_id'" if it
                # fails to translate Python objects to Java objects.
                raise TypeError(
                    f"Flow variable '{key}' of type '{type(fv)}' cannot be translated to a valid KNIME flow "
                    f"variable. Please remove the flow variable or change its type to something that can be translated."
                )

    def _set_flow_variables(self, flow_variables):
        self._check_flow_variables(flow_variables)

        LinkedHashMap = JavaClass(  # NOSONAR Java naming conventions apply.
            "java.util.LinkedHashMap", kg.client_server._gateway_client
        )
        java_flow_variables = LinkedHashMap()
        for key in flow_variables.keys():
            flow_variable = flow_variables[key]
            java_flow_variables[key] = flow_variable

        self._java_callback.set_flow_variables(java_flow_variables)

    class Java:
        implements = ["org.knime.python3.nodes.proxy.PythonNodeProxy"]


class _KnimeNodeBackend(kg.EntryPoint, kn._KnimeNodeBackend):
    def __init__(self) -> None:
        super().__init__()
        self._main_loop = MainLoop()
        self._port_type_registry = None
        kn._backend = self

    def register_port_type(
        self,
        name: str,
        object_class: Type[kn.PortObject],
        spec_class: Type[kn.PortObjectSpec],
        id: Optional[str] = None,
    ):
        assert self._port_type_registry is not None, "No extension is loaded."
        return self._port_type_registry.register_port_type(
            name, object_class, spec_class, id
        )

    def loadExtension(
        self, extension_id: str, extension_module: str, extension_version: str
    ) -> None:
        self._port_type_registry = _PortTypeRegistry(extension_id)
        # load the extension, so that it registers its nodes, categories and port types
        importlib.import_module(extension_module)
        kp.set_extension_version(extension_version)

    def initializeJavaCallback(self, callback):
        _push_log_callback(lambda msg, sev: callback.log(msg, sev))

    def retrieveCategoriesAsJson(self) -> str:
        category_dicts = [category.to_dict() for category in kn._categories]
        return json.dumps(category_dicts)

    def retrieveNodesAsJson(self) -> str:
        node_dicts = [self.resolve_node_dict(n) for n in kn._nodes.values()]
        return json.dumps(node_dicts)

    def resolve_node_dict(self, node: kn._Node):
        d = node.to_dict()
        instance = node.node_factory()
        description = self.extract_description(instance, node.name)
        return {**d, **description}

    def extract_description(self, node: kn.PythonNode, name) -> dict:
        node_doc = node.__doc__

        param_doc, tabs_used = kp.extract_parameter_descriptions(node)

        if node_doc is None:
            logging.warning(
                f"No docstring available for node {name}. Please document the node class with a docstring or set __doc__ in the init of the node."
            )
            short_description = full_description = "Missing description."
        else:
            # remove first empty line, for nicer formatted short descriptions
            if node_doc[0] == "\n":
                node_doc = node_doc[1:]
            split_description = node_doc.splitlines()
            short_description = split_description[0]
            if len(split_description) > 1:
                full_description = "\n".join(split_description[1:])
                full_description = self._knime_parser.parse_full_description(
                    full_description
                )
            else:
                full_description = "Missing description."

        # Check short description
        if not len(short_description):
            logging.warning(
                f"No short description available. Create it by placing text right next to starting docstrings."
            )

        if tabs_used:
            tabs = self._knime_parser.parse_tabs(param_doc)
            options = []
        else:
            options = self._knime_parser.parse_options(param_doc)
            tabs = []

        input_ports = self._knime_parser.parse_ports(node.input_ports)
        output_ports = self._knime_parser.parse_ports(node.output_ports)

        return {
            "short_description": short_description,
            "full_description": full_description,
            "options": options,
            "tabs": tabs,
            "input_ports": input_ports,
            "output_ports": output_ports,
        }

    def createNodeFromExtension(self, node_id: str) -> _PythonNodeProxy:
        node_info = kn._nodes[node_id]
        node = node_info.node_factory()
        return _PythonNodeProxy(node, self._port_type_registry, self._knime_parser)

    @property
    def _knime_parser(self):
        # Initialize the _knime_parser once we need it. In __init__ the log callback is
        # not ready and the warning would get lost
        if not hasattr(self, "_knime_parser_instance"):
            try:
                from knime.extension._markdown import KnimeMarkdownParser

                self._knime_parser_instance = KnimeMarkdownParser()
            except ModuleNotFoundError:
                logging.warning(
                    " The 'markdown' Python package is missing from the environment: "
                    + "node descriptions will display Markdown source in plain text instead of being rich-text-formatted."
                )
                self._knime_parser_instance = FallBackMarkdownParser()

        return self._knime_parser_instance

    class Java:
        implements = ["org.knime.python3.nodes.KnimeNodeBackend"]


class KnimeLogHandler(logging.StreamHandler):
    def __init__(self, backend):
        super().__init__(self)
        self.setFormatter(logging.Formatter("%(name)s:%(message)s"))
        self._backend = backend

    def emit(self, record: logging.LogRecord):
        if _log_callback is not None:
            msg = self.format(record)

            if record.levelno >= logging.ERROR:
                severity = "error"
            elif record.levelno >= logging.WARNING:
                severity = "warn"
            elif record.levelno >= logging.INFO:
                severity = "info"
            else:
                severity = "debug"

            _log_callback(msg, severity)


class FallBackMarkdownParser:
    """
    Is used when markdown is not imported.
    """

    def __init__(self):
        pass

    def parse_full_description(self, s):
        return s

    def parse_basic(self, s):
        return s

    def parse_ports(self, ports):
        return [{"name": port.name, "description": port.description} for port in ports]

    def parse_options(self, options):
        return options

    def parse_option_description(self, option_description):
        return option_description

    def parse_tabs(self, tabs):
        return tabs


_log_callback = None
_old_log_callbacks = []


def _push_log_callback(callback):
    global _log_callback
    global _old_log_callbacks
    _old_log_callbacks.append(_log_callback)
    _log_callback = callback


def _pop_log_callback():
    global _log_callback
    global _old_log_callbacks
    callback = _old_log_callbacks.pop()
    _log_callback = callback


if __name__ == "__main__":
    backend = _KnimeNodeBackend()
    kg.connect_to_knime(backend)
    py4j.clientserver.server_connection_stopped.connect(
        lambda *args, **kwargs: backend._main_loop.exit()
    )
    # suppress py4j logs
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(KnimeLogHandler(backend))

    # We enter the main loop - but never enqueue any tasks. It
    # seems as if keeping the main thread alive as long as the
    # py4j connection is established is enough / required to allow
    # other threading operations to work.
    backend._main_loop.enter()
