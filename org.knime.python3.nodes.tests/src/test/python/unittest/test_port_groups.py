from typing import Any, Dict, List, Optional
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

    class MockJavaConfigContext:
        def __init__(self, port_map: Dict[str, List[int]]) -> None:
            self._port_map = port_map

        def get_input_port_map(self) -> Dict[str, List[int]]:
            return self._port_map

        def get_node_id(self):
            return "0:1"

    class MockPortTypeRegistry:
        def spec_to_python(
            self, spec: knb._PythonPortObjectSpec, port, java_callback
        ) -> ks.Schema:
            return ks.Schema.deserialize(spec.data)

        def spec_from_python(self, spec: ks.Schema, port, node_id, port_idx):
            return knb._PythonPortObjectSpec(
                "org.knime.core.data.DataTableSpec", spec.serialize()
            )

    class MockJavaCallback:
        def get_flow_variables(self) -> Dict[str, Any]:
            return {}

        def set_flow_variables(self, flow_variables):
            pass

    class MockJavaConverter:
        def convert_list(self, list_):
            return list_

        def create_linked_hashmap(self):
            return {}

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

    def test_configure_optional_present(self):
        self._test_configure(True)

    def test_configure_optional_absent(self):
        self._test_configure(False)

    def _setup_python_node_proxy(self):
        node = TestOptionalPorts.NodeWithOptionalInputPort()
        port_type_registry = TestOptionalPorts.MockPortTypeRegistry()
        python_node_proxy = knb._PythonNodeProxy(
            node=node,
            port_type_registry=port_type_registry,
            knime_parser=None,  # not needed for configure
            extension_version="0.0.1",
            java_converter=TestOptionalPorts.MockJavaConverter(),
        )
        python_node_proxy.initializeJavaCallback(TestOptionalPorts.MockJavaCallback())

        return python_node_proxy

    def _test_configure(self, optional_present: bool):
        python_node_proxy = self._setup_python_node_proxy()
        mandatory_schema = ks.Schema([ks.string()], ["Foo"])
        optional_schema = ks.Schema([ks.string()], ["Bar"])
        input_specs = [
            knb._PythonPortObjectSpec(
                java_class_name="org.knime.core.data.DataTableSpec",
                data_dict=mandatory_schema.serialize(),
            ),
        ]
        port_map = {"Input Input table # 0": [0]}
        if optional_present:
            input_specs.append(
                knb._PythonPortObjectSpec(
                    java_class_name="org.knime.core.data.DataTableSpec",
                    data_dict=optional_schema.serialize(),
                )
            )
            port_map["Optional input table"] = [1]

        java_config_context = TestOptionalPorts.MockJavaConfigContext(port_map)
        output = python_node_proxy.configure(
            input_specs=input_specs, java_config_context=java_config_context
        )[0]

        expected_columns = [ks.Column(ks.string(), "Foo")]

        if optional_present:
            expected_columns.append(ks.Column(ks.string(), "Bar"))

        expected_schema = ks.Schema.from_columns(expected_columns)
        self.assertEqual(
            output.getJavaClassName(),
            "org.knime.core.data.DataTableSpec",
            "The should return a table spec.",
        )
        self.assertEqual(
            output.data, expected_schema.serialize(), "Output schema doesn't match."
        )
