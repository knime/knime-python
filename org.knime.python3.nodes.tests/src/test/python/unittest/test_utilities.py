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
        mock = MagicMock()
        mock.org = MagicMock()
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


py4j.java_collections.ListConverter = ListConverter
import _node_backend_launcher as knb

knb._PythonNodeProxy._get_flow_variables = _get_flow_variables
knb._PythonNodeProxy._set_flow_variables = _set_flow_variables
knb.kg.client_server = ClientServerMock()
