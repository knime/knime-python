import unittest
import pytest
import knime.extension.nodes as kn
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
            knb._get_port_indices(port_group, portmap),
            expected_indices,
            "Port group 'group1' should have indices [0, 1]",
        )
        self.assertNotIn(
            "group1",
            portmap,
            "Port group 'group1' should be removed from portmap after assignment",
        )

    def test_port_indices_are_assigned_correctly(self):
        """Test that port indices are assigned correctly."""
        portmap = {
            "Input port1 # 1": [1],
            "Input port1 # 2": [2],
            "Input port2 # 1": [3],
        }
        port = kn.Port(name="port1", description="", type="PortType.TABLE")
        expected_indices = [1]
        self.assertEqual(
            knb._get_port_indices(port, portmap),
            expected_indices,
            "Port 'port1' should have index [1]",
        )
        self.assertNotIn(
            "Input port1 # 1",
            portmap,
            "Port 'port1' should be removed from portmap after assignment",
        )

    def test_no_match_port_group_returns_empty_list(self):
        """Test that a non-existent port group returns an empty list of indices."""
        portmap = {"group1": [0, 1], "group2": [2, 3]}
        port_group = kn.PortGroup(
            name="nonexistent", description="", type="PortType.TABLE"
        )
        expected_indices = []
        self.assertEqual(
            knb._get_port_indices(port_group, portmap),
            expected_indices,
            "Non-existent port group should return an empty list of indices",
        )
