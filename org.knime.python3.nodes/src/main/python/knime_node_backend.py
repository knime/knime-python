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

from knime_main_loop import MainLoop

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
import collections

from py4j.java_gateway import JavaClass
from py4j.java_collections import ListConverter
import py4j.clientserver

# to allow Version comparisons
from utils import parse, Version

# LOGGER = logging.getLogger(__name__)
LOGGER = logging.getLogger("Python")

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
        if not isinstance(obj, bytes):
            tb = None
            raise TypeError(
                f"Binary Port can only process objects of type bytes, not {type(obj)}"
            ).with_traceback(tb)

        class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
        return _PythonBinaryPortObject(class_name, file_creator(), obj, port.id)
    else:
        raise ValueError("Configure got unsupported PortObject")


def _check_attr_is_available(node, attr_name):
    if not hasattr(node, attr_name) or getattr(node, attr_name) is None:
        raise ValueError(f"Attribute {attr_name} is missing in node {node}")


class _PythonNodeProxy:
    def __init__(self, node: kn.PythonNode) -> None:
        _check_attr_is_available(node, "input_ports")
        _check_attr_is_available(node, "output_ports")

        self._node = node
        self._num_outports = len(node.output_ports)

    def getDialogRepresentation(
        self,
        parameters: str,
        parameters_version: str,
        specs: List[_PythonPortObjectSpec],
        extension_version: str,
    ):
        # import debugpy

        # debugpy.listen(5678)
        # print("Waiting for debugger attach")
        # debugpy.wait_for_client()
        # debugpy.breakpoint()

        LOGGER.warning(
            f" >>> getDialogRepresentation | parameters: {json.loads(parameters)} | parameter version: {parameters_version} | extension version: {extension_version}"
        )

        parameters_version = parse(parameters_version)
        extension_version = parse(extension_version)
        self.setParameters(parameters, extension_version, False)

        inputs = self._specs_to_python(specs)

        json_forms_dict = {
            "data": kp.extract_parameters(
                self._node, for_dialog=True, version=extension_version
            ),
            "schema": kp.extract_schema(self._node, extension_version, inputs),
            "ui_schema": kp.extract_ui_schema(self._node, version=extension_version),
        }
        LOGGER.warning(
            f" >>> getDialogRepresentation | dumping the following for json forms: {json_forms_dict}"
        )
        return json.dumps(json_forms_dict)

    def _specs_to_python(self, specs):
        return [
            _spec_to_python(spec, port) if spec is not None else None
            for port, spec in zip(self._node.input_ports, specs)
        ]

    def getParameters(self, version=None) -> str:
        version = parse(version)
        # LOGGER.warning(f" >>> getParameters before extraction | version: {version}")
        parameters_dict = kp.extract_parameters(self._node, version=version)
        LOGGER.warning(
            f" >>> getParameters | version: {version} | dumping the following for json: {parameters_dict}"
        )
        return json.dumps(parameters_dict)

    def getSchema(self, version=None, specs: List[str] = None) -> str:
        version = parse(version)

        # LOGGER.warning(f" >>> getSchema before extraction | version {version}")

        if specs is not None:
            specs = self._specs_to_python(specs)

        schema = kp.extract_schema(self._node, version, specs)
        # LOGGER.warning(
        #     f" >>> getSchema after extraction | dumping the following schema: {schema}"
        # )
        return json.dumps(schema)

    def setParameters(
        self, parameters: str, extension_version: str, fail_on_missing: bool = False
    ) -> None:
        parameters_dict = json.loads(parameters)
        LOGGER.warning(
            f" >>> setParameters | parameters: {parameters_dict} | extension version {extension_version}."
        )
        kp.inject_parameters(
            self._node, parameters_dict, extension_version, fail_on_missing
        )

    def validateParameters(self, parameters: str, version: str) -> None:
        parameters_dict = json.loads(parameters)
        version = parse(version)
        # LOGGER.warning(
        #     f" >>> validateParameters | parameters: {parameters_dict} | version {version}."
        # )
        try:
            kp.validate_parameters(self._node, parameters_dict, version)
            return None
        except BaseException as error:
            return str(error)

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        # LOGGER.warning(f" >>> initializeJavaCallback")
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
            # TODO: maybe we want to run execute on the main thread? use knime_main_loop
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
                _port_object_from_python(
                    obj,
                    lambda: self._java_callback.create_filestore_file(),
                    self._node.output_ports[idx],
                )
                for idx, obj in enumerate(outputs)
            ]

        except Exception as ex:
            self._set_failure(ex, 0)
            return None

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
            # TODO: maybe we want to run execute on the main thread? use knime_main_loop
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
            _spec_from_python(spec, self._node.output_ports[i])
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


class _KnimeNodeBackend(kg.EntryPoint):
    def __init__(self) -> None:
        super().__init__()
        self._main_loop = MainLoop()

    def initializeJavaCallback(self, callback):
        _push_log_callback(lambda msg, sev: callback.log(msg, sev))

    def retrieveCategoriesAsJson(self, extension_module_name: str) -> str:
        importlib.import_module(extension_module_name)
        category_dicts = [category.to_dict() for category in kn._categories]
        return json.dumps(category_dicts)

    def retrieveNodesAsJson(self, extension_module_name: str) -> str:
        # NOTE: extract
        importlib.import_module(extension_module_name)
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

        try:
            from knime_markdown_parser import KnimeMarkdownParser

            knime_parser = KnimeMarkdownParser()
        except ModuleNotFoundError:
            knime_parser = FallBackMarkdownParser()

        if node_doc is None:
            logging.warning(
                f"No docstring available for node {name}. Please document the node class with a docstring or set __doc__ in the init of the node."
            )
        else:
            split_description = node_doc.splitlines()
            short_description = split_description[0]
            if len(split_description) > 1:
                full_description = "\n".join(split_description[1:])
                full_description = knime_parser.parse_fulldescription(full_description)
            else:
                full_description = "Missing description."

        # Check short description
        if not len(short_description):
            logging.warning(
                f"No short description available. Create it by placing text right next to starting docstrings."
            )

        if tabs_used:
            tabs = knime_parser.parse_tabs(param_doc)
            options = []
        else:
            options = knime_parser.parse_options(param_doc)
            tabs = []

        input_ports = knime_parser.parse_ports(node.input_ports)
        output_ports = knime_parser.parse_ports(node.output_ports)

        return {
            "short_description": short_description,
            "full_description": full_description,
            "options": options,
            "tabs": tabs,
            "input_ports": input_ports,
            "output_ports": output_ports,
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


class FallBackMarkdownParser:
    """
    Is used when markdown is not imported.
    """

    def __init__(self):
        pass

    def parse_fulldescription(self, s):
        return s

    def parse_basic(self, s):
        return s

    def parse_ports(self, ports):
        return [{"name": port.name, "description": port.description} for port in ports]

    def parse_options(self, options):
        return options

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
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(KnimeLogHandler(backend))

    # We enter the main loop - but never enqueue any tasks. It
    # seems as if keeping the main thread alive as long as the
    # py4j connection is established is enough / required to allow
    # other threading operations to work.
    backend._main_loop.enter()
