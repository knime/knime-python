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

from typing import List

import knime_gateway as kg
import knime_node as kn
import knime_parameter as kp
import knime_schema as ks

import knime_node_arrow_table as kat
import knime_node_table as kt
import importlib
import json
import logging
import traceback
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
        assert port.type == kn.PortType.BINARY
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
        if isinstance(spec, ks._ColumnarView):
            spec = spec.get()
        assert isinstance(spec, ks.Schema)
        data = spec.to_knime_dict()
        class_name = "org.knime.core.data.DataTableSpec"
    elif port.type == kn.PortType.BINARY:
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
        return kat.ArrowSourceTable(kg.data_source_mapper(java_source))
    elif (
        class_name
        == "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
    ):
        assert port.type == kn.PortType.BINARY
        file = port_object.getFilePath()
        with open(file, "rb") as f:
            return f.read()

    raise TypeError("Unsupported PortObjectSpec found in Python, got " + class_name)


def _port_object_from_python(obj, file_creator, port: kn.Port) -> _PythonPortObject:
    # TODO: use extension point,
    #       see https://knime-com.atlassian.net/browse/AP-18368
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
        assert isinstance(obj, bytes)
        class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
        return _PythonBinaryPortObject(class_name, file_creator(), obj, port.id)
    else:
        raise ValueError("Configure got unsupported PortObject")


class _PythonNodeProxy:
    def __init__(self, node: kn.PythonNode) -> None:
        self._node = node

    def getDialogRepresentation(
        self,
        parameters: str,
        parameters_version: str,
        specs: List[_PythonPortObjectSpec],
    ):
        self.setParameters(parameters, parameters_version)

        inputs = self._specs_to_python(specs)

        json_forms_dict = {
            "result": {
                "data": kp.extract_parameters(self._node),
                "schema": kp.extract_schema(self._node, inputs),
                "ui_schema": kp.extract_ui_schema(self._node),
            }
        }
        return json.dumps(json_forms_dict)

    def _specs_to_python(self, specs):
        input_ports = (
            self._node.input_ports if self._node.input_ports is not None else []
        )
        return [_spec_to_python(spec, port) for port, spec in zip(input_ports, specs)]

    def getParameters(self) -> str:
        parameters_dict = kp.extract_parameters(self._node)
        return json.dumps(parameters_dict)

    def getSchema(self, specs: List[str] = None) -> str:
        if specs is not None:
            specs = self._specs_to_python(specs)
        schema = kp.extract_schema(self._node, specs)
        return json.dumps(schema)

    def setParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        kp.inject_parameters(self._node, parameters_dict, version)

    def validateParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        try:
            kp.validate_parameters(self._node, parameters_dict, version)
            return None
        except BaseException as error:
            return str(error)

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        self._java_callback = java_callback

    def execute(
        self, input_objects: List[_PythonPortObject], java_exec_context
    ) -> List[_PythonPortObject]:
        _push_log_callback(lambda msg, sev: self._java_callback.log(msg, sev))

        inputs = [
            _port_object_to_python(po, self._node.input_ports[idx])
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
        try:
            outputs = self._node.execute(exec_context, *inputs)
        except Exception as ex:
            self._set_failure(ex, 1)
            return None

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

        if hasattr(self._node, "view") and self._node.view is not None:
            out_view = outputs[-1]
            outputs = outputs[:-1]

            # write the view to the sink
            view_sink = kg.data_sink_mapper(self._java_callback.create_view_sink())
            view_sink.display(out_view)

        java_outputs = [
            _port_object_from_python(
                obj,
                lambda: self._java_callback.create_filestore_file(),
                self._node.output_ports[idx],
            )
            for idx, obj in enumerate(outputs)
        ]

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
            outputs = self._node.configure(config_context, *inputs)
        except Exception as ex:
            self._set_failure(ex, 1)
            return None

        self._set_flow_variables(config_context.flow_variables)

        if outputs is None:
            outputs = []
        if not isinstance(outputs, list) and not isinstance(outputs, tuple):
            # single outputs are fine
            outputs = [outputs]

        output_specs = [
            _spec_from_python(spec, self._node.output_ports[i])
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
        details = "".join(traceback.format_exception(etype=type(ex), value=ex, tb=tb))

        # Set the failure in the Java callback
        self._java_callback.set_failure(
            str(ex), details, isinstance(ex, kn.InvalidParametersError)
        )

    def _get_flow_variables(self):
        from py4j.java_collections import JavaArray

        java_flow_variables = self._java_callback.get_flow_variables()

        flow_variables = {}
        for key, value in java_flow_variables.items():
            if isinstance(value, JavaArray):
                value = [x for x in value]
            flow_variables[key] = value
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
        implements = ["org.knime.python3.nodes.proxy.NodeProxy"]


class _KnimeNodeBackend(kg.EntryPoint):
    def __init__(self) -> None:
        super().__init__()

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
        _push_log_callback(lambda msg, sev: callback.log(msg, sev))

    def retrieveCategoriesAsJson(self, extension_module_name: str) -> str:
        importlib.import_module(extension_module_name)
        category_dicts = [category.to_dict() for category in kn._categories]
        return json.dumps(category_dicts)

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

        param_doc = kp.extract_parameter_descriptions(node)
        options = []
        tabs = []
        for doc in param_doc:
            if "options" in doc:
                tabs.append(doc)
            else:
                options.append(doc)
        # TODO support for tabs
        # TODO support for ports
        # TODO support for views
        return {
            "short_description": short_description,
            "full_description": full_description,
            "options": options,
            "tabs": tabs,
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
        self.setFormatter(logging.Formatter("%(name)s:%(message)s"))
        self._backend = backend

    def emit(self, record: logging.LogRecord):
        if _log_callback is not None:
            msg = self.format(record)

            # Ignore logs from the clientserver to prevent a deadlock
            if record.name == "py4j.clientserver":
                return

            if record.levelno >= logging.ERROR:
                severity = "error"
            elif record.levelno >= logging.WARNING:
                severity = "warn"
            elif record.levelno >= logging.INFO:
                severity = "info"
            else:
                severity = "debug"

            _log_callback(msg, severity)


backend = _KnimeNodeBackend()
kg.connect_to_knime(backend)
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


logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(KnimeLogHandler(backend))
