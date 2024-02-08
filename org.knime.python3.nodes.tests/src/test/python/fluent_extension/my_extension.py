import logging
import pyarrow as pa
import knime_extension as knext

LOGGER = logging.getLogger(__name__)

pycat = knext.category(
    "/", "pycat", "Python category", "A category defined in Python", "icon.png"
)

@knext.node(
    name="My Node",
    node_type=knext.NodeType.LEARNER,
    icon_path="icon.png",
    category=pycat,
)
@knext.input_table_group(name="TABLE", description="Jeallyous Hoe")
class MyNode:
    """My first node

    This node has a description
    """

    def configure(self, config_ctx, schema_1):
        LOGGER.warning("Configuring")
        LOGGER.info("Configure info")
        LOGGER.info(schema_1)
        LOGGER.info(config_ctx)
        config_ctx.set_warning("Configure warning")
        return (
            schema_1,
            knext.BinaryPortObjectSpec("org.knime.python3.nodes.tests.model"),
        )

    def execute(self, exec_context, table):
        LOGGER.log(5, "Ignored because loglevel < 10")
        LOGGER.log(15, "Log with level 15 -> INFO")
        LOGGER.debug("Executing - DEBUG")
        LOGGER.info("Executing - INFO")
        LOGGER.warning("Executing - WARN")
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