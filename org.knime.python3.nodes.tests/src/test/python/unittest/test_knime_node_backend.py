import knime_extension as knext
import knime_node_backend

import unittest

class TestNode:
    input_ports = []
    output_ports = []

class NodeWithoutDocstring(TestNode):
    pass

class NodeWithOneLineDocstring(TestNode):
    """Node with one line docstring"""
    pass

class NodeWithMultiLineDocstring(TestNode):
    """Node with short description.
    
       And long description."""
    pass

class KnimeNodeBackendTest(unittest.TestCase):

    def setUp(self):
        self.backend = knime_node_backend._KnimeNodeBackend()

    def test_extract_description_no_docstring(self):
        node = NodeWithoutDocstring()
        description = self.backend.extract_description(node, "NodeWithoutDescription")
        expected = {
            "short_description": "Missing description.",
            "full_description": "Missing description.",
            "options": [],
            "tabs": [],
            "input_ports": [],
            "output_ports": [],
        }
        self.assertEqual(expected, description)

    def test_extract_description_one_line_docstring(self):
        node = NodeWithOneLineDocstring()
        description = self.backend.extract_description(node, "NodeWithOneLineDescription")
        expected = {
            "short_description": "Node with one line docstring",
            "full_description": "Missing description.",
            "options": [],
            "tabs": [],
            "input_ports": [],
            "output_ports": [],
        }
        self.assertEqual(expected, description)

    def test_extract_description_multi_line_docstring(self):
        node = NodeWithMultiLineDocstring()
        description = self.backend.extract_description(node, "NodeWithoutDescription")
        expected = {
            "short_description": "Node with short description.",
            "full_description": "<p>And long description.</p>",
            "options": [],
            "tabs": [],
            "input_ports": [],
            "output_ports": [],
        }
        self.assertEqual(expected, description)

