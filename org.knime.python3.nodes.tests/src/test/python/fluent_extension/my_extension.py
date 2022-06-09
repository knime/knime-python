import logging
import pyarrow as pa
import knime_extension as knext

LOGGER = logging.getLogger(__name__)

pycat = knext.category(
    "/", "pycat", "Python category", "A category defined in Python", "icon.png"
)


@knext.parameter_group("Awesome Options")
class MyParameterGroup:
    """
    A parameter group for testing.
    The sum of the two contained parameters may not exceed 10.
    """

    first = knext.IntParameter("First Parameter", "The first parameter in the group", 1)
    second = knext.IntParameter("Second Parameter", "The second parameter in the group", 5)

    def validate(self, values: dict):
        if values["first"] + values["second"] > 10:
            raise ValueError("The sum of the parameter exceeds 10")


@knext.node(
    name="My Node", node_type=knext.NodeType.LEARNER, icon_path="icon.png", category=pycat
)
@knext.input_table(name="Input Data", description="We read data from here")
@knext.output_table(name="Output Data", description="Whatever the node has produced")
@knext.output_binary(
    name="Output Model",
    description="Whatever the node has produced",
    id="org.knime.python3.nodes.tests.model",
)
@knext.output_view(name="My pretty view", description="Shows only hello world ;)")
class MyNode:
    """My first node

    This node has a description
    """

    some_param = knext.IntParameter(
        "Some Int Parameter", "The answer to everything", 42, min_value=0
    )

    another_param = knext.StringParameter(
        "Some String parameter", "The classic placeholder", "foobar"
    )

    double_param = knext.DoubleParameter("Double Parameter", "Just for test purposes", 1.0)

    boolean_param = knext.BoolParameter("Boolean Parameter", "also just for testing", True)

    param_group = MyParameterGroup()

    def configure(self, config_ctx, schema_1):
        LOGGER.warn("Configuring")
        LOGGER.info("Configure info")
        config_ctx.set_warning("Configure warning")
        return schema_1, knext.BinaryPortObjectSpec("org.knime.python3.nodes.tests.model")

    def execute(self, exec_context, table):
        LOGGER.log(5, "Ignored because loglevel < 10")
        LOGGER.log(15, "Log with level 15 -> INFO")
        LOGGER.debug("Executing - DEBUG")
        LOGGER.info("Executing - INFO")
        LOGGER.warn("Executing - WARN")
        LOGGER.error("Executing - ERROR")
        LOGGER.critical("Executing - CRITICAL")
        try:
            raise ValueError("caught exception")
        except ValueError as e:
            LOGGER.exception(e)

        exec_context.set_warning("Execute warning")
        return (
            table,
            b"RandomTestData",
            knext.view("<!DOCTYPE html> Hello World"),
        )


@knext.node(
    name="My Second Node",
    node_type=knext.NodeType.PREDICTOR,
    icon_path="icon.png",
    category=pycat,
)
@knext.input_table(name="Input Data", description="We read data from here")
@knext.input_binary(
    name="model input",
    description="to produce garbage values",
    id="org.knime.python3.nodes.tests.model",
)
@knext.output_table(name="Output Data", description="Whatever the node has produced")
class MySecondNode:
    """My second node

    This node broadcasts the content of its binary port to each row of the input table

    And some more detail.
    """

    def configure(self, config_ctx, schema, port_spec):
        return schema.append(knext.Column(ktype=knext.string(), name="PythonProduced"))

    def execute(self, exec_ctx, table, binary):
        table = table.to_pyarrow()
        col = pa.array([binary.decode()] * len(table))
        field = pa.field("AddedColumn", type=pa.string())
        out_table = table.append_column(field, col)
        LOGGER.warning(exec_ctx.flow_variables)
        exec_ctx.flow_variables["test"] = 42
        return knext.Table.from_pyarrow(out_table)


@knext.node(
    name="My Third Node",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icon.png",
    category="/",
)
@knext.input_table(
    name="Input Data", description="The input table. Should contain double columns."
)
@knext.output_table(
    name="Output Data",
    description="The input table plus a column containing the row-wise sum of the values.",
)
class MyThirdNode(knext.PythonNode):
    """My third node

    This node allows to compute the row-wise sums of double columns.
    """

    columns = knext.MultiColumnParameter(
        "Columns",
        "Columns to calculate row-wise sums over.",
        column_filter=lambda c: c.ktype == knext.double(),
    )
    aggregation = knext.StringParameter(
        "Aggregation",
        "The aggregation to perform on the selected columns.",
        default_value="Sum",
        enum=["Sum", "Product"],
    )

    def configure(self, config_context: knext.ConfigurationContext, spec):
        if self.columns is None:
            # autoconfigure if no columns are specified yet
            self.columns = [c.name for c in spec if c.ktype == knext.double()]
        # TODO utility functions for unique name generation
        return spec.append(knext.Column(ktype=knext.double(), name=self.aggregation))

    def execute(self, exec_context: knext.ExecutionContext, table: knext.Table):
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
        return knext.Table.from_pyarrow(table)


@knext.node(
    "No-Op", node_type=knext.NodeType.VISUALIZER, icon_path="icon.png", category=pycat
)
class NoInpOupNode(knext.PythonNode):
    """Node without inputs or outputs that does nothing

    Node without inputs or outputs that does nothing
    """

    def configure(self, config_context: knext.ConfigurationContext):
        pass

    def execute(self, exec_context: knext.ExecutionContext):
        pass


@knext.node(name="Failing Node", node_type="Learner", icon_path="icon.png", category=pycat)
@knext.input_table(name="Input Data", description="We read data from here")
@knext.output_table(name="Output Data", description="Whatever the node has produced")
class FailingNode:
    """Failing node

    This node fails
    """

    fail_with_invalid_settings = knext.BoolParameter(
        "Fail with invalid settings",
        "if configure should fail because the settings are invalid",
        False,
    )
    fail_on_configure = knext.BoolParameter(
        "Fail on configure", "if configure should fail", False
    )
    fail_on_execute = knext.BoolParameter(
        "Fail on execute", "if execute should fail", True
    )
    use_exec_context_wrong = knext.BoolParameter(
        "Use exec_context wrong",
        "fails on execute because the exec_context is called with wrong types",
        False,
    )

    def configure(self, config_ctx, schema_1):
        if self.fail_with_invalid_settings:
            raise knext.InvalidParametersError("Invalid parameters")
        if self.fail_on_configure:
            raise ValueError("Foo bar error description (configure)")
        return schema_1

    def execute(self, exec_context: knext.ExecutionContext, table):
        if self.fail_on_execute:
            raise ValueError("Foo bar error description (execute)")
        if self.use_exec_context_wrong:
            exec_context.set_progress("no progress")

        return table
