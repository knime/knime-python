import knime_extension as knext

"""Test extension for testing knime_node_backend"""


class TestPortObjectSpec(knext.PortObjectSpec):
    def __init__(self, data: str) -> None:
        self._data = data

    def to_knime_dict(self) -> dict:
        return {"test_data": self._data}

    @classmethod
    def from_knime_dict(cls, data: dict) -> "TestPortObjectSpec":
        return cls(data["test_data"])

    @property
    def data(self) -> str:
        return self._data


class TestPortObject(knext.PortObject):
    def __init__(self, spec: TestPortObjectSpec, data: str) -> None:
        super().__init__(spec)
        self._data = data

    def serialize(self) -> bytes:
        return self._data.encode()

    @classmethod
    def deserialize(cls, spec: TestPortObjectSpec, storage: bytes) -> "TestPortObject":
        return cls(spec, storage.decode())

    @property
    def data(self) -> str:
        return self._data


test_port_type = knext.port_type("Test Port Type", TestPortObject, TestPortObjectSpec)


@knext.node(
    name="Node With Test Output Port",
    node_type=knext.NodeType.SOURCE,
    icon_path="icon.png",
    category="/",
)
@knext.output_port("Test output port", "Test output port", test_port_type)
class NodeWithTestOutputPort(knext.PythonNode):
    def configure(self, config_context: knext.ConfigurationContext):
        return TestPortObjectSpec("Configure NodeWithTestOutputPort")

    def execute(self, exec_context: knext.ExecutionContext):
        return TestPortObject(TestPortObjectSpec("Execute NodeWithTestOutputPort"))


class TestNode:
    input_ports = []
    output_ports = []


class NodeWithoutDocstring(TestNode):
    pass


class NodeWithOneLineDocstring(TestNode):
    """Node with one line docstring"""

    pass


class NodeWithMultiLineDocstring(TestNode):
    """Node with short description.

    And long description."""

    pass
