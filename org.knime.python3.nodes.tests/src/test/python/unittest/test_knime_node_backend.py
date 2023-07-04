import unittest

import _node_backend_launcher as knb
import knime_extension as knext  # old import to test the forwarding import
import knime.extension.nodes as kn
import tempfile
import json
import os
import test_utilities
from typing import Any, Dict


class AnotherPortObjectSpec(knext.PortObjectSpec):
    def serialize(self) -> dict:
        return {}

    @classmethod
    def deserialize(data):
        return AnotherPortObjectSpec()


class AnotherPortObject(knext.PortObject):
    def __init__(self, spec: knext.PortObjectSpec) -> None:
        super().__init__(spec)

    def serialize(self) -> bytes:
        return b""

    @classmethod
    def deserialize(self, spec, storage):
        return AnotherPortObject(spec)


def _binary_spec_from_java(id: str, data: dict = None, **kwargs):
    d = kwargs
    d["id"] = id
    if data is not None:
        d["data"] = data
    return knb._PythonPortObjectSpec(
        "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec", d
    )


class PortTypeRegistryTest(unittest.TestCase):
    class TestPortObjectSpec(knext.PortObjectSpec):
        def __init__(self, data: str) -> None:
            self._data = data

        def serialize(self) -> dict:
            return {"test_data": self._data}

        @classmethod
        def deserialize(cls, data: dict) -> "PortTypeRegistryTest.TestPortObjectSpec":
            return cls(data["test_data"])

        @property
        def data(self) -> str:
            return self._data

    class TestPortObject(knext.PortObject):
        def __init__(
            self, spec: "PortTypeRegistryTest.TestPortObjectSpec", data: str
        ) -> None:
            super().__init__(spec)
            self._data = data

        def serialize(self) -> bytes:
            return self._data.encode()

        @classmethod
        def deserialize(
            cls, spec: "PortTypeRegistryTest.TestPortObjectSpec", storage: bytes
        ) -> "PortTypeRegistryTest.TestPortObject":
            return cls(spec, storage.decode())

        @property
        def data(self) -> str:
            return self._data

    class TestConnectionPortObjectSpec(knext.PortObjectSpec):
        def __init__(self, data: str) -> None:
            self._data = data

        def serialize(self) -> dict:
            return {"test_data": self._data}

        @classmethod
        def deserialize(
            cls, data: dict
        ) -> "PortTypeRegistryTest.TestConnectionPortObjectSpec":
            return cls(data["test_data"])

        @property
        def data(self) -> str:
            return self._data

    class TestConnectionPortObject(knext.ConnectionPortObject):
        def __init__(
            self,
            spec: "PortTypeRegistryTest.TestConnectionPortObjectSpec",
            data: str,
            transient_data=None,  # optional because it could also be set by set_connection_dict
        ) -> None:
            super().__init__(spec)
            self._data = data
            self._transient_data = transient_data

        def to_connection_data(self) -> Any:
            return (self._data, self._transient_data)

        @classmethod
        def from_connection_data(
            cls,
            spec: "PortTypeRegistryTest.TestConnectionPortObjectSpec",
            data: Any,
        ) -> "PortTypeRegistryTest.TestConnectionPortObject":
            return cls(spec, data[0], data[1])

        @property
        def data(self) -> str:
            return self._data

    def setUp(self) -> None:
        self.registry = knb._PortTypeRegistry("test.extension")

    def test_default_port_type_id(self):
        # id is intentionally None to test the default id generation
        port_type = self.registry.register_port_type(
            "Test port type", self.TestPortObject, self.TestPortObjectSpec
        )
        self.assertEqual(
            "test.extension.test_knime_node_backend.PortTypeRegistryTest.TestPortObject",
            port_type.id,
        )

    def test_same_object_class_in_multiple_port_types(self):
        self.registry.register_port_type(
            "First", self.TestPortObject, self.TestPortObjectSpec, "foo.bar"
        )
        with self.assertRaises(ValueError):
            self.registry.register_port_type(
                "Second", self.TestPortObject, AnotherPortObjectSpec, "bar.foo"
            )

    def test_same_spec_class_multiple_port_types(self):
        self.registry.register_port_type(
            "First", self.TestPortObject, self.TestPortObjectSpec, id="foo.bar"
        )
        with self.assertRaises(ValueError):
            self.registry.register_port_type(
                "Second", AnotherPortObject, self.TestPortObjectSpec, "bar.foo"
            )

    def test_same_id_multiple_port_types(self):
        self.registry.register_port_type(
            "First", self.TestPortObject, self.TestPortObjectSpec, id="foo.bar"
        )
        with self.assertRaises(ValueError):
            self.registry.register_port_type(
                "Second", AnotherPortObject, AnotherPortObjectSpec, "foo.bar"
            )

    def get_port_type(
        self, object_class=TestPortObject, spec_class=TestPortObjectSpec, id=None
    ):
        return self.registry.register_port_type(
            "Port type", object_class, spec_class, id=id
        )

    def test_custom_spec_from_python(self):
        test_port_type = self.get_port_type()

        port = knext.Port(test_port_type, "Test port", "Test port")
        spec = self.TestPortObjectSpec("foo")
        pypos = self.registry.spec_from_python(spec, port, "NodeId", 0)
        self.assertEqual(
            "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec",
            pypos.getJavaClassName(),
        )
        self.assertEqual(
            json.dumps({"id": test_port_type.id, "data": {"test_data": "foo"}}),
            pypos.toJsonString(),
        )

    def test_custom_spec_from_python_wrong_spec_class(self):
        test_port_type = self.get_port_type()
        port = knext.Port(test_port_type, "", "")
        spec = AnotherPortObjectSpec()
        with self.assertRaises(AssertionError):
            self.registry.spec_from_python(spec, port, "NodeId", 0)

    def test_custom_spec_from_python_unknown_spec_class(self):
        unknown_port_type = knext.PortType(
            "foo", "name", self.TestPortObject, self.TestPortObjectSpec
        )
        port = knext.Port(unknown_port_type, "", "")
        spec = self.TestPortObjectSpec("foobar")
        with self.assertRaises(AssertionError):
            self.registry.spec_from_python(spec, port, "NodeId", 0)

    def test_custom_spec_to_python_wrong_id(self):
        test_port_type = self.get_port_type(id="foo")
        other_port_type = self.get_port_type(
            AnotherPortObject, AnotherPortObjectSpec, "bar"
        )
        port = knext.Port(test_port_type, "Test port", "Test_port")
        java_spec = _binary_spec_from_java(other_port_type.id, {})
        with self.assertRaises(AssertionError):
            self.registry.spec_to_python(java_spec, port)

    def test_custom_spec_to_python_unknown_id(self):
        # can happen if the developer doesn't register the port type via knext.port_type
        unknown_port_type = knext.PortType(
            "foo", "name", self.TestPortObject, self.TestPortObjectSpec
        )
        port = knext.Port(unknown_port_type, "", "")
        java_spec = _binary_spec_from_java(unknown_port_type.id, {})
        with self.assertRaises(AssertionError):
            self.registry.spec_to_python(java_spec, port)

    def test_custom_spec_to_python(self):
        test_port_type = self.get_port_type()
        port = knext.Port(test_port_type, "Test port", "Test_port")
        java_spec = _binary_spec_from_java(test_port_type.id, {"test_data": "bar"})
        spec = self.registry.spec_to_python(java_spec, port)
        self.assertEqual("bar", spec.data)

    class MockFromJavaObject:
        def __init__(
            self,
            spec,
            file_path,
            class_name="org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject",
        ) -> None:
            self.spec = spec
            self.file_path = file_path
            self._class_name = class_name

        def getJavaClassName(self):
            return self._class_name

        def getSpec(self):
            return self.spec

        def getFilePath(self):
            return self.file_path

    def test_custom_object_to_python(self):
        test_port_type = self.get_port_type()

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
                java_obj = self.MockFromJavaObject(java_spec, file.name)
                obj = self.registry.port_object_to_python(java_obj, port)
                self.assertEqual("furball", obj.data)
                self.assertEqual("badabummm", obj.spec.data)
            finally:
                file.close()
                os.remove(file.name)

    def test_wrong_custom_object_to_python(self):
        java_spec = _binary_spec_from_java("foo.bar")
        port = knext.Port(self.get_port_type(), "", "")

        with tempfile.NamedTemporaryFile(delete=False) as file:
            try:
                file.write(b"furball")
                file.flush()
                java_obj = self.MockFromJavaObject(java_spec, file.name)
                with self.assertRaises(AssertionError):
                    self.registry.port_object_to_python(java_obj, port)
            finally:
                file.close()
                os.remove(file.name)

    def test_unknown_custom_object_to_python(self):
        unknown_port_type = knext.PortType(
            "unknown", "type", AnotherPortObject, AnotherPortObjectSpec
        )
        java_spec = _binary_spec_from_java(unknown_port_type.id, {})
        port = knext.Port(unknown_port_type, "", "")

        with tempfile.NamedTemporaryFile(delete=False) as file:
            try:
                file.write(b"furball")
                file.flush()
                java_obj = self.MockFromJavaObject(java_spec, file.name)
                with self.assertRaises(AssertionError):
                    self.registry.port_object_to_python(java_obj, port)
            finally:
                file.close()
                os.remove(file.name)

    def test_custom_object_from_python(self):
        test_port_type = self.get_port_type()

        class MockFileStore:
            def __init__(self, file) -> None:
                self.file = file

            def get_key(self):
                return "fs_key"

            def get_file_path(self):
                return self.file.name

        spec = self.TestPortObjectSpec("ice")
        obj = self.TestPortObject(spec, "cream")
        port = knext.Port(test_port_type, "", "")
        # delete=False is necessary to allow the framework to open the file
        with tempfile.NamedTemporaryFile(delete=False) as file:
            try:
                mock_filestore = MockFileStore(file)
                out = self.registry.port_object_from_python(
                    obj, lambda: mock_filestore, port, "nodeID", 0
                )
                self.assertEqual("cream", file.read().decode())
                self.assertEqual("fs_key", out.getFileStoreKey())
                self.assertEqual(
                    json.dumps({"id": test_port_type.id, "data": {"test_data": "ice"}}),
                    out.getSpec().toJsonString(),
                )
            finally:
                file.close()
                os.remove(file.name)

    def test_wrong_custom_object_from_python(self):
        obj = AnotherPortObject(AnotherPortObjectSpec())
        port = knext.Port(self.get_port_type(), "", "")
        with self.assertRaises(AssertionError):
            # providing None as file_creator is fine because the method should fail
            # before doing anything with it
            self.registry.port_object_from_python(obj, None, port, "nodeID", 0)

    def test_unknown_custom_object_from_python(self):
        obj = self.TestPortObject(self.TestPortObjectSpec("ice"), "cream")
        port = knext.Port(
            knext.PortType(
                "port", "type", self.TestPortObject, self.TestPortObjectSpec
            ),
            "",
            "",
        )
        with self.assertRaises(AssertionError):
            # providing None as file_creator is fine because the method should fail
            # before doing anything with it
            self.registry.port_object_from_python(obj, None, port, "nodeID", 0)

    def test_connection_port_object_from_python(self):
        test_connection_port_type = self.get_port_type(
            self.TestConnectionPortObject,
            self.TestConnectionPortObjectSpec,
            "test.connection",
        )

        spec = self.TestConnectionPortObjectSpec("ice")
        obj = self.TestConnectionPortObject(
            spec, "cream", "this data is not serialized"
        )
        port = knext.Port(test_connection_port_type, "", "")

        def invalid_factory():
            raise AssertionError("Should not be called")

        out = self.registry.port_object_from_python(
            obj, invalid_factory, port, "nodeID", 0
        )
        self.assertEqual(
            json.dumps(
                {
                    "id": test_connection_port_type.id,
                    "data": {"test_data": "ice"},
                    "node_id": "nodeID",
                    "port_idx": 0,
                }
            ),
            out.getSpec().toJsonString(),
        )
        key = "nodeID:0"
        self.assertTrue(key in knb._PortTypeRegistry._connection_port_data)
        self.assertEqual(
            ("cream", "this data is not serialized"),
            knb._PortTypeRegistry._connection_port_data[key],
            f"{knb._PortTypeRegistry._connection_port_data[key]}",
        )

        knb._PortTypeRegistry._connection_port_data.clear()

    def test_connection_port_object_to_python(self):
        test_connection_port_type = self.get_port_type(
            self.TestConnectionPortObject,
            self.TestConnectionPortObjectSpec,
            "test.connection",
        )

        knb._PortTypeRegistry._connection_port_data.clear()
        knb._PortTypeRegistry._connection_port_data["nodeID:0"] = (
            "furball",
            "this data is not serialized",
        )

        # _PythonPortObjectSpec happens to have all method needed by the tested method so we use it here
        java_spec = _binary_spec_from_java(
            test_connection_port_type.id,
            {"test_data": "badabummm"},
            node_id="nodeID",  # kwargs are inserted into the spec json
            port_idx=0,
        )
        port = knext.Port(test_connection_port_type, "", "")

        java_obj = self.MockFromJavaObject(
            java_spec,
            "no filename needed",
            class_name="org.knime.python3.nodes.ports.PythonTransientConnectionPortObject",
        )
        obj = self.registry.port_object_to_python(java_obj, port)
        self.assertEqual("furball", obj.data)
        self.assertEqual("badabummm", obj.spec.data)
        self.assertEqual("this data is not serialized", obj._transient_data)


class DescriptionParsingTest(unittest.TestCase):
    def setUp(self):
        self.backend = test_utilities.setup_backend("mock_extension")

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


class ValueHoldingPortObject(kn.PortObject):
    def __init__(self, spec: kn.PortObjectSpec, value: str) -> None:
        super().__init__(spec)
        self._value = value

    @property
    def value(self):
        return self._value


class TestPortObject(ValueHoldingPortObject):
    def serialize(self) -> bytes:
        return self._value.encode()

    @classmethod
    def deserialize(cls, spec, data: bytes):
        return cls(spec, data.decode())


class TestFilestorePortObject(ValueHoldingPortObject, kn.FilestorePortObject):
    def write_to(self, file_path):
        with open(file_path, "w") as f:
            f.write(self.value)

    @classmethod
    def read_from(cls, spec, file_path):
        with open(file_path, "r") as f:
            return cls(spec, f.read())


class LoadSavePortObjectTest(unittest.TestCase):
    def test_save_load_ordinary_po(self):
        spec = AnotherPortObjectSpec()
        po = TestPortObject(spec, "foo")
        self._test_save_load_po(po)

    def test_save_load_filestore_po(self):
        spec = AnotherPortObjectSpec()
        po = TestFilestorePortObject(spec, "bar")
        self._test_save_load_po(po)

    def _test_save_load_po(self, port_object: ValueHoldingPortObject):
        with tempfile.TemporaryDirectory() as directory:
            po_path = os.path.join(directory, "po_path")
            kn.save_port_object(port_object, po_path)
            loaded = kn.load_port_object(TestPortObject, port_object.spec, po_path)
            self.assertEqual(port_object.value, loaded.value)
