import knime_extension as knext

"""Test extension for testing knime.extension._backend"""


class TestPortObjectSpec(knext.PortObjectSpec):
    def __init__(self, data: str) -> None:
        self._data = data

    def serialize(self) -> dict:
        return {"test_data": self._data}

    @classmethod
    def deserialize(cls, data: dict) -> "TestPortObjectSpec":
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


BINARY_PORT_ID = "org.knime.python3.nodes.test.port"
TEST_DESCR = "We read data from here"


@knext.node(
    name="My Node Without Ports",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Node Without Ports",
)
class NodeWithoutPorts:
    def configure(self, config_ctx, *inputs):
        pass  # no implementation needed for this test class

    def execute(self, exec_context, *inputs):
        pass  # no implementation needed for this test class


# test case where the init method adds a port
@knext.node(
    name="My Fourth Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Fourth Node",
)
@knext.output_table(name="Output Data", description="Whatever the node has produced")
class MyPropertyOverridingNode:
    input_ports = [
        knext.Port(
            type=knext.PortType.TABLE,
            name="Overriden Input Data",
            description=TEST_DESCR,
        )
    ] * 3

    def __init__(self):
        self.input_ports = self.input_ports + [
            knext.Port(
                type=knext.PortType.TABLE,
                name="Fourth input table",
                description="We might also read data from there",
            )
        ]

        self.output_ports = self.output_ports + [
            knext.Port(
                type=knext.PortType.TABLE,
                name="New output table",
                description="Blupp",
            )
        ]

    def configure(self, config_ctx, *inputs):
        pass  # no implementation needed for this test class

    def execute(self, exec_context, *inputs):
        pass  # no implementation needed for this test class


@knext.node(
    name="My Test Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Test Node",
)
@knext.input_table(name="Input Data", description=TEST_DESCR)
@knext.input_table(
    name="Second input table", description="We might also read data from there"
)
@knext.output_table(name="Output Data", description="Whatever the node has produced")
@knext.output_table(name="Second output Data", description="Only a column")
@knext.output_binary(
    name="Some output port",
    description="Maybe a model",
    id=BINARY_PORT_ID,
)
@knext.output_view(name="Test View", description="lalala")
class MyTestNode:
    def configure(self, config_ctx, schema_1, schema_2):
        return (
            schema_1,
            knext.Column(knext.string(), "first col of second output table"),
            knext.BinaryPortObjectSpec(id=BINARY_PORT_ID),
        )

    def execute(self, exec_context, table_1, table_2):
        return [table_1, b"random bytes"]


@knext.node(
    name="My Second Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="MyTestNode",
)
class MyTestNode:
    input_ports = [
        knext.Port(
            type=knext.PortType.TABLE,
            name="Input Data",
            description=TEST_DESCR,
        ),
        knext.Port(
            type=knext.PortType.TABLE,
            name="Second input table",
            description="We might also read data from there",
        ),
    ]

    def __init__(self) -> None:
        super().__init__()

        self.output_ports = [
            knext.Port(
                type=knext.PortType.TABLE,
                name="Output Data",
                description="Whatever the node has produced",
            ),
            knext.Port(
                type=knext.PortType.BINARY,
                name="Some output port",
                description="Maybe a model",
                id=BINARY_PORT_ID,
            ),
        ]

    @property
    def output_view(self):
        return knext.ViewDeclaration(
            name="ExampleView", description="White letters on white background"
        )

    def configure(self, config_ctx, schema_1, schema_2):
        return schema_1, knext.BinaryPortTypeSpec(id=BINARY_PORT_ID)

    def execute(self, exec_context, table_1, table_2):
        return [table_1, b"random bytes"]


@knext.node(
    name="My Third Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Third Node",
)
@knext.input_table(name="Input Data", description=TEST_DESCR)
@knext.input_table(
    name="Second input table", description="We might also read data from there"
)
@knext.output_table(name="Output Data", description="Whatever the node has produced")
@knext.output_binary(
    name="Some output port",
    description="Maybe a model",
    id=BINARY_PORT_ID,
)
def my_node_generating_func():
    class MyHiddenNode:
        def configure(self, config_ctx, input):
            return input

        def execute(self, exec_ctx, input):
            return input

    return MyHiddenNode()


@knext.node(
    name="My image node",
    node_type=knext.NodeType.SOURCE,
    icon_path="icon.png",
    category="/",
    id="My image node",
)
@knext.output_image(
    name="PNG Output Image",
    description="Should contain a PNG image.",
)
@knext.output_image(
    name="SVG Output Image",
    description="Should contain a PNG image.",
)
class MyImageNode(knext.PythonNode):
    """My image node

    This node produces two images: a PNG and an SVG. The node has no parameters.
    """

    def configure(self, config_context):
        return (
            knext.ImagePortObjectSpec(knext.ImageFormat.PNG),
            knext.ImagePortObjectSpec(knext.ImageFormat.SVG),
        )

    def execute(self, exec_context):
        import matplotlib.pyplot as plt
        import io

        x = [1, 2, 3, 4, 5]
        y = [1, 2, 3, 4, 5]

        fig, ax = plt.subplots(figsize=(5, 5), dpi=100)
        ax.plot(x, y)

        buffer_png = io.BytesIO()
        buffer_svg = io.BytesIO()

        plt.savefig(buffer_png, format="png")
        plt.savefig(buffer_svg, format="svg")

        return (
            buffer_png.getvalue(),
            buffer_svg.getvalue(),
        )
