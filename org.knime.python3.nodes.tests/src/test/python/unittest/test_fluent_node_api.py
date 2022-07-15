# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

from typing import List, Tuple, Type
import unittest
import pythonpath
import knime_node as kn
import knime_node_table as kt
import knime_schema as ks


@kn.node(
    name="My Test Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Test Node",
)
@kn.input_table(name="Input Data", description="We read data from here")
@kn.input_table(
    name="Second input table", description="We might also read data from there"
)
@kn.output_table(name="Output Data", description="Whatever the node has produced")
@kn.output_binary(
    name="Some output port",
    description="Maybe a model",
    id="org.knime.python3.nodes.test.port",
)
@kn.output_view(name="Test View", description="lalala")
class MyTestNode:
    def configure(self, config_ctx, schema_1, schema_2):
        return schema_1

    def execute(self, exec_context, table_1, table_2):
        return [table_1, b"random bytes"]


class NodeApiTest(unittest.TestCase):
    node_id = "My Test Node"

    def setUp(self):
        self.node = kn._nodes.get(NodeApiTest.node_id, None)
        self.node_instance = self.node.node_factory()

    def test_node_registration(self):
        self.assertTrue(NodeApiTest.node_id in kn._nodes)

    def test_has_node_view(self):
        self.assertIsNotNone(self.node.views[0])
        self.assertIsNotNone(self.node_instance.output_view)
        self.assertIsInstance(self.node_instance.output_view, kn.ViewDeclaration)

    def test_input_ports(self):
        self.assertEqual(2, len(self.node.input_ports))
        self.assertEqual(kn.PortType.TABLE, self.node.input_ports[0].type)
        self.assertEqual(kn.PortType.TABLE, self.node.input_ports[1].type)

    def test_output_ports(self):
        self.assertEqual(2, len(self.node.output_ports))
        self.assertEqual(kn.PortType.TABLE, self.node.output_ports[0].type)
        self.assertEqual(kn.PortType.BINARY, self.node.output_ports[1].type)


@kn.node(
    name="My Second Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="MyTestNode",
)
class MyTestNode:
    input_ports = [
        kn.Port(
            type=kn.PortType.TABLE,
            name="Input Data",
            description="We read data from here",
        ),
        kn.Port(
            type=kn.PortType.TABLE,
            name="Second input table",
            description="We might also read data from there",
        ),
    ]

    def __init__(self) -> None:
        super().__init__()

        self.output_ports = [
            kn.Port(
                type=kn.PortType.TABLE,
                name="Output Data",
                description="Whatever the node has produced",
            ),
            kn.Port(
                type=kn.PortType.BINARY,
                name="Some output port",
                description="Maybe a model",
                id="org.knime.python3.nodes.test.port",
            ),
        ]

    @property
    def output_view(self):
        return kn.ViewDeclaration(
            name="ExampleView", description="White letters on white background"
        )

    def configure(self, config_ctx, schema_1, schema_2):
        return schema_1, ks.BinaryPortTypeSpec(id="org.knime.python3.nodes.test.port")

    def execute(self, exec_context, table_1, table_2):
        return [table_1, b"random bytes"]


class InstanceAttributePortsTest(unittest.TestCase):
    node_id = "MyTestNode"

    def setUp(self):
        self.node = kn._nodes.get(InstanceAttributePortsTest.node_id, None)

    def test_node_registration(self):
        self.assertTrue(InstanceAttributePortsTest.node_id in kn._nodes)

    def test_has_node_view(self):
        self.assertIsNotNone(self.node.views[0])

    def test_input_ports(self):
        self.assertEqual(2, len(self.node.input_ports))
        self.assertEqual(kn.PortType.TABLE, self.node.input_ports[0].type)
        self.assertEqual(kn.PortType.TABLE, self.node.input_ports[1].type)

    def test_output_ports(self):
        self.assertEqual(2, len(self.node.output_ports))
        self.assertEqual(kn.PortType.TABLE, self.node.output_ports[0].type)
        self.assertEqual(kn.PortType.BINARY, self.node.output_ports[1].type)


class PortTest(unittest.TestCase):
    def test_port_must_have_id(self):
        with self.assertRaises(TypeError):
            p = kn.Port(type=kn.PortType.BINARY, name="A", description="a")

        p = kn.Port(type=kn.PortType.BINARY, name="A", description="a", id="test")
        self.assertEqual("test", p.id)

        # TABLE doesn't need an ID, should not throw.
        t = kn.Port(type=kn.PortType.TABLE, name="A", description="a")
        self.assertIsNone(t.id)


@kn.node(
    name="My Third Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Third Node",
)
@kn.input_table(name="Input Data", description="We read data from here")
@kn.input_table(
    name="Second input table", description="We might also read data from there"
)
@kn.output_table(name="Output Data", description="Whatever the node has produced")
@kn.output_binary(
    name="Some output port",
    description="Maybe a model",
    id="org.knime.python3.nodes.test.port",
)
def my_node_generating_func():
    class MyHiddenNode:
        def configure(self, config_ctx, input):
            return input

        def execute(self, exec_ctx, input):
            return input

    return MyHiddenNode()


class NodeFactoryApiTest(unittest.TestCase):
    node_id = "My Third Node"

    def setUp(self):
        self.node = kn._nodes.get(NodeFactoryApiTest.node_id, None)
        self.node_instance = self.node.node_factory()
        self.node_instance_direct = my_node_generating_func()

    def test_node_registration(self):
        self.assertTrue(NodeFactoryApiTest.node_id in kn._nodes)

    def test_has_no_node_view(self):
        self.assertIsNone(self.node.views[0])
        self.assertIsNone(self.node_instance.output_view)

    def test_input_ports(self):
        for n in [self.node, self.node_instance, self.node_instance_direct]:
            self.assertEqual(2, len(n.input_ports))
            self.assertEqual(kn.PortType.TABLE, n.input_ports[0].type)
            self.assertEqual(kn.PortType.TABLE, n.input_ports[1].type)

    def test_output_ports(self):
        for n in [self.node, self.node_instance, self.node_instance_direct]:
            self.assertEqual(2, len(n.output_ports))
            self.assertEqual(kn.PortType.TABLE, n.output_ports[0].type)
            self.assertEqual(kn.PortType.BINARY, n.output_ports[1].type)


class DoubleInputPortsTest(unittest.TestCase):
    def test_cannot_use_decorator_and_instance_attrib(self):
        with self.assertRaises(ValueError):

            @kn.input_table(name="Input Data", description="We read data from here")
            class MyDummyNode:
                input_ports = [
                    kn.Port(
                        type=kn.PortType.TABLE,
                        name="Overriden Input Data",
                        description="We read data from here",
                    )
                ]


# test case where the init method adds a port
@kn.node(
    name="My Fourth Node",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Fourth Node",
)
@kn.output_table(name="Output Data", description="Whatever the node has produced")
class MyPropertyOverridingNode:
    input_ports = [
        kn.Port(
            type=kn.PortType.TABLE,
            name="Overriden Input Data",
            description="We read data from here",
        )
    ] * 3

    def __init__(self):
        self.input_ports = self.input_ports + [
            kn.Port(
                type=kn.PortType.TABLE,
                name="Fourth input table",
                description="We might also read data from there",
            )
        ]

        self.output_ports = self.output_ports + [
            kn.Port(
                type=kn.PortType.TABLE, name="New output table", description="Blupp",
            )
        ]

    # no config and execute needed for this test class


class OverriddenInputPortsTest(unittest.TestCase):
    node_id = "My Fourth Node"

    def setUp(self):
        self.node = kn._nodes.get(OverriddenInputPortsTest.node_id, None)
        self.node_instance = self.node.node_factory()
        self.node_instance_direct = MyPropertyOverridingNode()

    def test_has_no_node_view(self):
        self.assertIsNone(self.node.views[0])
        self.assertIsNone(self.node_instance.output_view)
        self.assertIsNone(self.node_instance_direct.output_view)

    def test_input_ports(self):
        for n in [self.node, self.node_instance, self.node_instance_direct]:
            self.assertEqual(4, len(n.input_ports))
            self.assertTrue(all(p.type == kn.PortType.TABLE for p in n.input_ports))

    def test_output_ports(self):
        # Both, an instance as well as the "Node" proxy should know that there
        # are two output ports, so that the node shows up properly in KNIME
        for n in [self.node, self.node_instance, self.node_instance_direct]:
            self.assertEqual(2, len(n.output_ports))
            self.assertTrue(all(p.type == kn.PortType.TABLE for p in n.output_ports))


# test case where no ports are defined at all
@kn.node(
    name="My Node Without Ports",
    node_type="Learner",
    icon_path="icon.png",
    category="/",
    id="My Node Without Ports",
)
class NodeWithoutPorts:
    pass
    # no config and execute needed for this test class


class NodeWithoutPortsTest(unittest.TestCase):
    node_id = "My Node Without Ports"

    def setUp(self):
        self.node = kn._nodes.get(NodeWithoutPortsTest.node_id, None)
        self.node_instance = self.node.node_factory()

    def test_has_no_node_view(self):
        self.assertIsNone(self.node.views[0])
        self.assertIsNone(self.node_instance.output_view)

    def test_has_no_input_ports(self):
        self.assertTrue(hasattr(self.node, "input_ports"))
        self.assertEqual(0, len(self.node.input_ports))
        self.assertTrue(hasattr(self.node_instance, "input_ports"))
        self.assertEqual(0, len(self.node_instance.input_ports))

    def test_has_no_output_ports(self):
        self.assertTrue(hasattr(self.node, "output_ports"))
        self.assertEqual(0, len(self.node.output_ports))
        self.assertTrue(hasattr(self.node_instance, "output_ports"))
        self.assertEqual(0, len(self.node_instance.output_ports))

    def test_create_node_proxy(self):
        from knime_node_backend import _PythonNodeProxy

        _PythonNodeProxy(self.node_instance)
        _PythonNodeProxy(self.node)


if __name__ == "__main__":
    unittest.main()
