from typing import List, Tuple
import knime_node as kn
import knime_table as kt
import knime_schema as ks
import pyarrow as pa

@kn.parameter_group("Awesome Options")
class MyParameterGroup:
    """
    A parameter group for testing.
    The sum of the two contained parameters may not exceed 10.
    """
    first = kn.IntParameter("First Parameter", "The first parameter in the group", 1)
    second = kn.IntParameter("Second Parameter", "The second parameter in the group", 5)

    def validate(self, values: dict):
        if values["first"] + values["second"] > 10:
            raise ValueError("The sum of the parameter exceeds 10")

    


@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
@kn.input_table(name="Input Data", description="We read data from here")
@kn.output_table(name="Output Data", description="Whatever the node has produced")
@kn.output_port(
    name="Output Model",
    description="Whatever the node has produced",
    id="org.knime.python3.nodes.tests.model",
)
class MyNode(kn.PythonNode):
    """My first node

    This node has a description
    """

    some_param = kn.IntParameter(
        "Some Int Parameter", "The answer to everything", 42, min_value=0
    )

    another_param = kn.StringParameter(
        "Some String parameter", "The classic placeholder", "foobar"
    )

    double_param = kn.DoubleParameter("Double Parameter", "Just for test purposes", 1.0)

    boolean_param = kn.BoolParameter("Boolean Parameter", "also just for testing", True)

    param_group = MyParameterGroup()

    def __init__(self) -> None:
        super().__init__()

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        return [
            input_schemas[0],
            ks.BinaryPortObjectSpec("org.knime.python3.nodes.tests.model"),
        ]

    def execute(self, inputs, exec_context):
        return [kt.write_table(inputs[0]), b"RandomTestData"]


@kn.node(
    name="My Second Node", node_type="Predictor", icon_path="icon.png", category="/"
)
@kn.input_table(name="Input Data", description="We read data from here")
@kn.input_port(
    name="model input",
    description="to produce garbage values",
    id="org.knime.python3.nodes.tests.model",
)
@kn.output_table(name="Output Data", description="Whatever the node has produced")
class MySecondNode(kn.PythonNode):
    """My second node

    This node broadcasts the content of its binary port to each row of the input table

    And some more detail.
    """

    def __init__(self) -> None:
        super().__init__()

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        return [
            input_schemas[0].append(ks.Column(type=ks.string(), name="PythonProduced"))
        ]

    def execute(self, inputs, exec_context):
        table = inputs[0].to_pyarrow()
        col = pa.array([inputs[1].decode()] * len(table))
        field = pa.field("AddedColumn", type=pa.string())
        out_table = table.append_column(field, col)
        return [kt.write_table(out_table)]
