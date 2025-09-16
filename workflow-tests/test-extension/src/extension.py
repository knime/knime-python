"""
Test Python extension for debug_knime_yaml_list workflow testing.
"""

import knime.extension as knext


@knext.node(
    name="Test Debug Knime Yaml List Node",
    node_type=knext.NodeType.MANIPULATOR,
    category="/",
    icon_path="",
)
@knext.input_table(name="Input Data", description="Input data table")
@knext.output_table(name="Output Data", description="Output data table")
class TestDebugKnimeYamlListNode:
    """
    A simple test node for debug_knime_yaml_list workflow testing.
    """

    def configure(self, configure_context, input_schema):
        return input_schema

    def execute(self, execute_context, input_table):
        # Simply pass through the input table
        return input_table
