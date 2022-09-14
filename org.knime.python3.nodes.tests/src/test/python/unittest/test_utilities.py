import py4j.java_collections


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


def _get_flow_variables(self):
    return {}


def _set_flow_variables(self, flow_variables):
    pass  # Mockup


py4j.java_collections.ListConverter = ListConverter
import knime_node_backend as knb

knb._PythonNodeProxy._get_flow_variables = _get_flow_variables
knb._PythonNodeProxy._set_flow_variables = _set_flow_variables
knb.kg.client_server = ClientServerMock()
