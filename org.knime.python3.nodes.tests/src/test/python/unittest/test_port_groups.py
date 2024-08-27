from typing import Optional
import unittest
import pandas as pd
import pytest
import knime.extension.nodes as kn
import knime.api.schema as ks
import _node_backend_launcher as knb


class TestPortGroupNames(unittest.TestCase):
    """Tests for ensuring uniqueness of port group names."""

    def test_unique_port_group_names_raises_error(self):
        """Test that creating two input table groups with the same name raises a ValueError."""
        with pytest.raises(ValueError, match="PortGroup named 'data' already exists"):

            @kn.node(
                name="Test Node",
                node_type="Manipulator",
                icon_path="icon.png",
                category="test",
            )
            @kn.input_table_group(name="data", description="First input table group")
            @kn.input_table_group(
                name="data", description="Second input table group with the same name"
            )
            class TestNode:
                pass


class TestPortGroupMapping(unittest.TestCase):
    """Tests for checking correct port group mapping."""

    def test_correct_port_group_mapping(self):
        """Test that port groups are correctly mapped to input and output ports."""

        @kn.node(
            name="Concatenate Node",
            node_type="Manipulator",
            icon_path="icon.png",
            category="test",
        )
        @kn.input_table_group(name="first_group", description="First input table group")
        @kn.input_table_group(
            name="second_group", description="Second input table group"
        )
        @kn.output_table_group(name="output_group", description="Output table group")
        class ConcatenateNode:
            def configure(self, config_context, first_group_specs, second_group_specs):
                return first_group_specs + second_group_specs

            def execute(self, exec_context, first_group_tables, second_group_tables):
                return first_group_tables + second_group_tables

        node_instance = ConcatenateNode()
        self.assertEqual(
            len(node_instance.input_ports), 2, "There should be two input ports."
        )
        self.assertEqual(
            len(node_instance.output_ports), 1, "There should be one output port."
        )
        self.assertEqual(
            node_instance.input_ports[0].name,
            "first_group",
            "First input port group should be named 'first_group'",
        )
        self.assertEqual(
            node_instance.input_ports[1].name,
            "second_group",
            "Second input port group should be named 'second_group'",
        )
        self.assertEqual(
            node_instance.output_ports[0].name,
            "output_group",
            "Output port group should be named 'output_group'",
        )


class TestPortIndices(unittest.TestCase):
    """Tests for checking correct assignment of port indices."""

    def test_port_group_indices_are_assigned_correctly(self):
        """Test that port group indices are assigned correctly."""
        portmap = {"group1": [0, 1], "group2": [2, 3]}
        port_group = kn.PortGroup(name="group1", description="", type="PortType.TABLE")
        expected_indices = [0, 1]
        self.assertEqual(
            knb._get_port_indices(port_group, portmap, 0),
            expected_indices,
            "Port group 'group1' should have indices [0, 1]",
        )

    def test_port_indices_are_assigned_correctly(self):
        """Test that port indices are assigned correctly."""
        portmap = {
            "Input port1 # 0": [1],
            "Input port1 # 1": [2],
            "Input port2 # 1": [3],
        }
        port = kn.Port(name="port1", description="", type="PortType.TABLE")
        expected_indices = [1]
        self.assertEqual(
            knb._get_port_indices(port, portmap, 0),
            expected_indices,
            "Port 'port1' should have index [1]",
        )

    def test_no_match_port_group_returns_empty_list(self):
        """Test that a non-existent port group returns an empty list of indices."""
        portmap = {"group1": [0, 1], "group2": [2, 3]}
        port_group = kn.PortGroup(
            name="nonexistent", description="", type="PortType.TABLE"
        )
        expected_indices = []
        self.assertEqual(
            knb._get_port_indices(port_group, portmap, 0),
            expected_indices,
            "Non-existent port group should return an empty list of indices",
        )


class TestOptionalPorts(unittest.TestCase):
    @kn.node("Test node", kn.NodeType.OTHER, "", "")
    @kn.input_table("Input table", "The non-optional input table.")
    @kn.input_table("Optional input table", "The optional input table.", optional=True)
    @kn.output_table("Output table", "The output table.")
    class NodeWithOptionalInputPort:
        def configure(
            self, ctx, input_port: ks.Schema, optional_port: Optional[ks.Schema]
        ) -> ks.Schema:
            columns = [c for c in input_port]
            if optional_port:
                columns += [c for c in optional_port]
            return ks.Schema.from_columns(columns)

        def execute(
            self, ctx, input_table: kn.Table, optional_table: Optional[kn.Table]
        ) -> kn.Table:
            df = input_table.to_pandas()
            if optional_table:
                df = pd.concat([df, optional_table.to_pandas()])
            return kn.Table.from_pandas(df)

    def test_port_configuration(self):
        node = TestOptionalPorts.NodeWithOptionalInputPort()
        self.assertEqual(len(node.input_ports), 2, "There should be two input ports.")
        self.assertEqual(
            len(node.output_ports), 1, "There should be only one output table."
        )
        first_input = node.input_ports[0]
        self.assertEqual(
            first_input.name, "Input table", "First input should be name 'Input table'"
        )
        self.assertEqual(first_input.optional, False, "The first input is not optional")
        second_input = node.input_ports[1]
        self.assertEqual(
            second_input.name,
            "Optional input table",
            "The second input table should be named 'Optional input table'.",
        )
        self.assertEqual(second_input.optional, True, "The second input is optional")

        output = node.output_ports[0]
        self.assertEqual(
            output.name,
            "Output table",
            "The output table should be named 'Output table'.",
        )
        self.assertEqual(
            output.optional,
            False,
            "The output table is not optional (and optional outputs aren't supported yet).",
        )

    # TODO add tests for spec and port object handling
