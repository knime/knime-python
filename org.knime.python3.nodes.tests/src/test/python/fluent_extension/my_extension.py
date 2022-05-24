from typing import List, Tuple

import knime_node as kn
import knime_node_table as kt
import knime_schema as ks
import knime_views as kv
import pyarrow as pa
import logging

LOGGER = logging.getLogger(__name__)


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
@kn.output_binary(
    name="Output Model",
    description="Whatever the node has produced",
    id="org.knime.python3.nodes.tests.model",
)
@kn.view(name="My pretty view", description="Shows only hello world ;)")
class MyNode:
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

    def configure(self, config_ctx, schema_1):
        LOGGER.warn("Configuring")
        LOGGER.info("Invisible")
        return schema_1, ks.BinaryPortObjectSpec("org.knime.python3.nodes.tests.model")

    def execute(self, exec_context, table):
        LOGGER.warn("Executing")
        return (
            table,
            b"RandomTestData",
            kv.view("<!DOCTYPE html> Hello World"),
        )


@kn.node(
    name="My Second Node", node_type="Predictor", icon_path="icon.png", category="/"
)
@kn.input_table(name="Input Data", description="We read data from here")
@kn.input_binary(
    name="model input",
    description="to produce garbage values",
    id="org.knime.python3.nodes.tests.model",
)
@kn.output_table(name="Output Data", description="Whatever the node has produced")
class MySecondNode:
    """My second node

    This node broadcasts the content of its binary port to each row of the input table

    And some more detail.
    """

    def configure(self, config_ctx, schema, port_spec):
        return schema.append(ks.Column(type=ks.string(), name="PythonProduced"))

    def execute(self, exec_ctx, table, binary):
        table = table.to_pyarrow()
        col = pa.array([binary.decode()] * len(table))
        field = pa.field("AddedColumn", type=pa.string())
        out_table = table.append_column(field, col)
        LOGGER.warn(exec_ctx.flow_variables)
        exec_ctx.flow_variables["test"] = 42
        return kt.Table.from_pyarrow(out_table)


@kn.node(
    name="My Third Node", node_type="Manipulator", icon_path="icon.png", category="/"
)
@kn.input_table(
    name="Input Data", description="The input table. Should contain double columns."
)
@kn.output_table(
    name="Output Data",
    description="The input table plus a column containing the row-wise sum of the values.",
)
class MyThirdNode(kn.PythonNode):
    """My third node

    This node allows to compute the row-wise sums of double columns.
    """

    columns = kn.MultiColumnParameter(
        "Columns",
        "Columns to calculate row-wise sums over.",
        column_filter=lambda c: c.type == ks.double(),
    )
    aggregation = kn.StringParameter(
        "Aggregation",
        "The aggregation to perform on the selected columns.",
        default_value="Sum",
        enum=["Sum", "Product"],
    )

    def configure(self, config_context: kn.ConfigurationContext, spec):
        if self.columns is None:
            # autoconfigure if no columns are specified yet
            self.columns = [c.name for c in spec if c.type == ks.double()]
        # TODO utility functions for unique name generation
        return spec.append(ks.Column(type=ks.double(), name=self.aggregation))

    def execute(self, exec_context: kn.ExecutionContext, table: kt.ReadTable):
        table = table.to_pyarrow()
        selected = table.select(self.columns)
        num_columns = len(self.columns)
        if self.aggregation == "Sum":
            aggregator = pa.compute.add
        elif self.aggregation == "Product":
            aggregator = pa.compute.multiply
        else:
            raise ValueError(
                f"Unsupported aggregation '{self.aggregation}' encountered."
            )
        aggregated = selected.column(0)
        for i in range(1, num_columns):
            if exec_context.is_canceled():
                raise RuntimeError("The execution has been cancelled by the user.")
            exec_context.set_progress(i / num_columns)
            aggregated = aggregator(aggregated, selected.column(i))
        table = table.append_column(
            pa.field(self.aggregation, type=pa.float64()), aggregated
        )
        return kt.Table.from_pyarrow(out_table)
