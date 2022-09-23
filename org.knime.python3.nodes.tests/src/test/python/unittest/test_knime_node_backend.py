import unittest

import knime_node_backend as knb
import knime_extension as knext
import knime_schema as ks
import sys
import tempfile
import json
import os


class AnotherPortObjectSpec(knext.PortObjectSpec):
    def to_knime_dict(self) -> dict:
        return {}

    @classmethod
    def from_knime_dict(data):
        return AnotherPortObjectSpec()


class AnotherPortObject(knext.PortObject):
    def __init__(self, spec: knext.PortObjectSpec) -> None:
        super().__init__(spec)

    def serialize(self) -> bytes:
        return b""

    @classmethod
    def deserialize(self, spec, storage):
        return AnotherPortObject(spec)


def _binary_spec_from_java(id: str, data: dict = None):
    d = {"id": id}
    if data is not None:
        d["data"] = data
    return knb._PythonPortObjectSpec(
        "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec", json.dumps(d)
    )


class KnimeNodeBackendTest(unittest.TestCase):
    def setUp(self):
        self.backend = knb._KnimeNodeBackend()
        import knime_node as kn

        kn._nodes.clear()
        kn._categories.clear()

        # ensure that mock_extension is actually imported by self.backend.loadExtension
        sys.modules.pop("mock_extension", None)
        self.backend.loadExtension("my.test.extension", "mock_extension")

    def test_default_port_type_id(self):
        from mock_extension import NodeWithTestOutputPort

        node = NodeWithTestOutputPort()
        port = node.output_ports[0]
        self.assertEqual(
            "my.test.extension.mock_extension.TestPortObject", port.type.id
        )

    def test_same_object_class_in_multiple_port_types(self):
        with self.assertRaises(ValueError):
            from mock_extension import TestPortObject

            knext.port_type(
                "Another port type", TestPortObject, AnotherPortObjectSpec, "foobar"
            )

    def test_same_spec_class_in_multiple_port_types(self):
        with self.assertRaises(ValueError):
            from mock_extension import TestPortObjectSpec

            knext.port_type(
                "Yet another port type", AnotherPortObject, TestPortObjectSpec, "barfoo"
            )

    def test_custom_spec_from_python(self):
        from mock_extension import TestPortObjectSpec, test_port_type

        port = knext.Port(test_port_type, "Test port", "Test port")
        spec = TestPortObjectSpec("foo")
        pypos = knb._spec_from_python(spec, port)
        self.assertEqual(
            "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec",
            pypos.getJavaClassName(),
        )
        self.assertEqual(
            json.dumps({"id": test_port_type.id, "data": {"test_data": "foo"}}),
            pypos.toJsonString(),
        )

    def test_custom_spec_to_python(self):
        from mock_extension import test_port_type

        port = knext.Port(test_port_type, "Test port", "Test_port")
        java_spec = _binary_spec_from_java(test_port_type.id, {"test_data": "bar"})
        spec = knb._spec_to_python(java_spec, port)
        self.assertEqual("bar", spec.data)

    def test_custom_object_to_python(self):
        from mock_extension import test_port_type

        class MockFromJavaObject:
            def __init__(self, spec, file_path) -> None:
                self.spec = spec
                self.file_path = file_path

            def getJavaClassName(self):
                return (
                    "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
                )

            def getSpec(self):
                return self.spec

            def getFilePath(self):
                return self.file_path

        # _PythonPortObjectSpec happens to have all method needed by the tested method so we use it here
        java_spec = _binary_spec_from_java(
            test_port_type.id, {"test_data": "badabummm"}
        )
        port = knext.Port(test_port_type, "", "")
        # delete=False is necessary to allow the framework to open the file
        with tempfile.NamedTemporaryFile(delete=False) as file:
            try:
                file.write(b"furball")
                file.flush()
                java_obj = MockFromJavaObject(java_spec, file.name)
                obj = knb._port_object_to_python(java_obj, port)
                self.assertEqual("furball", obj.data)
                self.assertEqual("badabummm", obj.spec.data)
            finally:
                file.close()
                os.remove(file.name)

    def test_custom_object_from_python(self):
        from mock_extension import TestPortObject, TestPortObjectSpec, test_port_type

        class MockFileStore:
            def __init__(self, file) -> None:
                self.file = file

            def get_key(self):
                return "fs_key"

            def get_file_path(self):
                return self.file.name

        spec = TestPortObjectSpec("ice")
        obj = TestPortObject(spec, "cream")
        port = knext.Port(test_port_type, "", "")
        # delete=False is necessary to allow the framework to open the file
        with tempfile.NamedTemporaryFile(delete=False) as file:
            try:
                mock_filestore = MockFileStore(file)
                out = knb._port_object_from_python(obj, lambda: mock_filestore, port)
                self.assertEqual("cream", file.read().decode())
                self.assertEqual("fs_key", out.getFileStoreKey())
                self.assertEqual(
                    json.dumps({"id": test_port_type.id, "data": {"test_data": "ice"}}),
                    out.getSpec().toJsonString(),
                )
            finally:
                file.close()
                os.remove(file.name)

    def test_extract_description_no_docstring(self):
        from mock_extension import NodeWithoutDocstring

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
        from mock_extension import NodeWithOneLineDocstring

        node = NodeWithOneLineDocstring()
        description = self.backend.extract_description(
            node, "NodeWithOneLineDescription"
        )
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
        from mock_extension import NodeWithMultiLineDocstring

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
