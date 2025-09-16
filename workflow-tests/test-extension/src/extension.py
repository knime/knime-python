"""
Test Python extension for debugSources workflow testing.
"""

import knime.extension as knext


@knext.node(
    name="Test Debug Sources Node",
    node_type=knext.NodeType.MANIPULATOR,
    category="/",
)
@knext.input_table(
    name="Input Data",
    description="Input data table"
)
@knext.output_table(
    name="Output Data", 
    description="Output data table"
)
class TestDebugSourcesNode:
    """
    A simple test node for debugSources workflow testing.
    """
    
    def configure(self, configure_context, input_schema):
        return input_schema
    
    def execute(self, execute_context, input_table):
        # Simply pass through the input table
        return input_table