import py4j.java_collections
from unittest.mock import MagicMock


def setup_backend(
    extension_module: str,
    extension_id: str = "test.extension",
    extension_version: str = "0.0.0",
):
    """
    Sets up a fresh backend and loads the provided extension.
    """
    import _node_backend_launcher
    import sys
    import knime.extension.nodes as kn

    backend = _node_backend_launcher._KnimeNodeBackend()
    kn._nodes.clear()
    kn._categories.clear()
    # ensure that extension_module is freshly imported by backend.loadExtension
    sys.modules.pop("mock_extension", None)
    backend.loadExtension(extension_id, extension_module, extension_version)
    return backend


class ListConverter(object):
    def can_convert(self, object):
        return True

    def convert(self, object, gateway_client):
        return object


class ClientServerMock:
    @property
    def _gateway_client(self):
        None  # NOSONAR: Mock

    @property
    def gateway_property(self):
        None  # NOSONAR: Mock

    @property
    def jvm(self):
        def _getTypeNameForLogicalTypeString(logical_type: str):
            if (
                logical_type
                == '{"value_factory_class":"org.knime.core.data.v2.value.cell.DictEncodedDataCellValueFactory","data_type":{"cell_class":"org.knime.core.data.uri.URIDataCell"}}'
            ):
                return "URI"
            elif (
                logical_type
                == '{"value_factory_class": "org.knime.core.data.v2.value.StringSetValueFactory"}'
            ):
                return "Set (Collection of: String)"
            elif (
                logical_type
                == '{"value_factory_class":"org.knime.core.data.v2.value.SetValueFactory"}'
            ):
                return "Set"
            elif (
                logical_type
                == '{"value_factory_class":"org.knime.core.data.v2.value.cell.DictEncodedDataCellValueFactory","data_type":{"cell_class":"org.knime.core.data.date.DateAndTimeCell"}}'
            ):
                return "Timestamp"
            else:
                raise ValueError()

        mock = MagicMock()
        mock.org = MagicMock()
        mock.org.knime.core.data.v2.ValueFactoryUtils.getTypeNameForLogicalTypeString = _getTypeNameForLogicalTypeString
        return mock


class JavaCallbackMock:
    def get_auth_schema(self, xml_data):
        return "None"

    def get_auth_parameters(self, xml_data):
        return "None"


def _get_flow_variables(self):
    return {}


def _set_flow_variables(self, flow_variables):
    pass  # Mockup


def create_mock_input_port_map(node):
    # only works for static input ports
    input_ports = node.input_ports
    input_portmap = {}
    for idx, port in enumerate(input_ports):
        portmap_name = f"Input {port.name} # {idx}"
        input_portmap[portmap_name] = [1]  # single table is connected
    return input_portmap


py4j.java_collections.ListConverter = ListConverter
import _node_backend_launcher as knb

knb._PythonNodeProxy._get_flow_variables = _get_flow_variables
knb._PythonNodeProxy._set_flow_variables = _set_flow_variables
knb.kg.client_server = ClientServerMock()
