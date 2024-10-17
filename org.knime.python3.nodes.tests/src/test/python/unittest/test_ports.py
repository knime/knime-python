from dataclasses import dataclass
import json
import unittest
from _ports import (
    JavaPortTypeRegistry,
    _PortObjectContainer,
    _PortObjectSpecContainer,
    PyToKnimeConverterEntry,
)
from knime.extension.ports import (
    NonePythonTransfer,
    StringPythonTransfer,
    BidirectionalPortObjectConverter,
)
from knime.extension.nodes import (
    PortObjectSpec,
    PortObject,
    PortType,
)

import _port_impl as pi

import knime.api.schema as ks


@dataclass
class MockExtensionPortObjectSpec:
    data: str


@dataclass
class MockExtensionPortObject:
    data: str
    spec: MockExtensionPortObjectSpec


class MockExtensionPortObjectConverter(
    BidirectionalPortObjectConverter[
        MockExtensionPortObject,
        StringPythonTransfer,
        MockExtensionPortObjectSpec,
        StringPythonTransfer,
    ]
):
    def __init__(self) -> None:
        super().__init__(MockExtensionPortObject, MockExtensionPortObjectSpec)

    def convert_obj_to_python(
        self,
        transfer: StringPythonTransfer,
        spec: MockExtensionPortObjectSpec,
    ) -> MockExtensionPortObject:
        return MockExtensionPortObject(transfer.getStringRepresentation(), spec)

    def convert_obj_from_python(
        self, port_object: MockExtensionPortObject
    ) -> StringPythonTransfer:
        return StringPythonTransfer(port_object.data)

    def convert_spec_to_python(
        self,
        transfer: StringPythonTransfer,
    ) -> MockExtensionPortObjectSpec:
        return MockExtensionPortObjectSpec(transfer.getStringRepresentation())

    def convert_spec_from_python(
        self,
        spec: MockExtensionPortObjectSpec,
    ) -> StringPythonTransfer:
        return StringPythonTransfer(spec.data)


class TestPortTypeRegistryRegistration(unittest.TestCase):
    """Tests the registration phase of the registry life-cycle."""

    def setUp(self):
        self.registry = JavaPortTypeRegistry()

    def test_register_knime_to_py_obj_converter(self):
        # Arrange
        module_name = "test_ports"
        converter_class_name = "MockExtensionPortObjectConverter"
        obj_java_class_name = "some.java.ClassName"
        spec_java_class_name = "some.java.SpecClassName"
        port_type_name = "SomePortType"

        # Act
        self.registry.register_knime_to_py_converter(
            module_name,
            converter_class_name,
            obj_java_class_name,
            spec_java_class_name,
            port_type_name,
        )

        # Assert
        self.assertIn(obj_java_class_name, self.registry._knime_to_py_by_obj_class)
        self.assertIsInstance(
            self.registry._knime_to_py_by_obj_class[obj_java_class_name],
            MockExtensionPortObjectConverter,
        )
        self.assertIn(obj_java_class_name, self.registry._port_types_by_id)

    def test_register_py_to_knime_obj_converter(
        self,
    ):
        # Arrange
        module_name = "test_ports"
        converter_class_name = "MockExtensionPortObjectConverter"
        obj_java_class_name = "some.java.ClassName"
        spec_java_class_name = "some.java.SpecClassName"
        port_type_name = "SomePortType"

        # Act
        self.registry.register_py_to_knime_converter(
            module_name,
            converter_class_name,
            obj_java_class_name,
            spec_java_class_name,
            port_type_name,
        )

        # Assert
        self.assertIn(obj_java_class_name, self.registry._port_types_by_id)
        entry = self.registry._py_to_knime_for_obj_type[MockExtensionPortObject]
        self.assertIsInstance(
            entry,
            PyToKnimeConverterEntry,
        )
        self.assertIsInstance(entry.converter, MockExtensionPortObjectConverter)
        entry = self.registry._py_to_knime_for_spec_type[MockExtensionPortObjectSpec]
        self.assertIsInstance(
            entry,
            PyToKnimeConverterEntry,
        )
        self.assertIsInstance(
            entry.converter,
            MockExtensionPortObjectConverter,
        )

    def test_register_py_to_knime_converter_duplicate(self):
        # Arrange

        module_name = "test_ports"
        converter_class_name = "MockExtensionPortObjectConverter"
        java_obj_class_name = "some.java.ClassName"
        java_spec_class_name = "some.java.SpecClassName"
        port_type_name = "SomePortType"

        # Register the first converter
        self.registry.register_py_to_knime_converter(
            module_name,
            converter_class_name,
            java_obj_class_name,
            java_spec_class_name,
            port_type_name,
        )

        # Act & Assert
        with self.assertRaises(ValueError):
            self.registry.register_py_to_knime_converter(
                module_name,
                converter_class_name,
                java_obj_class_name,
                java_spec_class_name,
                port_type_name,
            )

    def test_register_knime_to_py_converter_duplicate(self):
        # Arrange

        module_name = "test_ports"
        converter_class_name = "MockExtensionPortObjectConverter"
        java_obj_class_name = "some.java.ClassName"
        java_spec_class_name = "some.java.SpecClassName"
        port_type_name = "SomePortType"

        # Register the first converter
        self.registry.register_knime_to_py_converter(
            module_name,
            converter_class_name,
            java_obj_class_name,
            java_spec_class_name,
            port_type_name,
        )

        # Act & Assert
        with self.assertRaises(ValueError):
            self.registry.register_knime_to_py_converter(
                module_name,
                converter_class_name,
                java_obj_class_name,
                java_spec_class_name,
                port_type_name,
            )


class TestPortTypeRegistryConversion(unittest.TestCase):
    """Tests the conversion phase of the registry life-cycle."""

    def setUp(self):
        self.registry = JavaPortTypeRegistry()
        self.extension_java_spec = "some.java.SpecClassName"
        self.extension_port_type = self.register_all_in_one_converter(
            "test_ports",
            "MockExtensionPortObjectConverter",
            "some.java.ClassName",
            self.extension_java_spec,
            "extension_port",
        )

        self.register_builtin_port_types()

    def register_builtin_port_types(self):
        self.credential_java_spec = "org.knime.core.node.port.CredentialPortObjectSpec"
        self.register_all_in_one_converter(
            "_port_impl",
            "CredentialConverter",
            "org.knime.core.node.port.CredentialPortObject",
            self.credential_java_spec,
            "credential",
        )

        # these are not the actual types but they are only used as ids on the python side anyway
        self.hub_auth_java_spec = (
            "org.knime.core.node.port.HubAuthenticationPortObjectSpec"
        )
        self.register_all_in_one_converter(
            "_port_impl",
            "HubAuthenticationConverter",
            "org.knime.core.node.port.HubAuthenticationPortObject",
            self.hub_auth_java_spec,
            "hub_authentication",
        )

    def register_all_in_one_converter(
        self,
        module_name: str,
        converter_class_name: str,
        java_obj_class: str,  # aka the port type id
        java_spec_class: str,
        port_type_name: str,
    ) -> PortType:
        port_type = self.registry.register_py_to_knime_converter(
            module_name,
            converter_class_name,
            java_obj_class,
            java_spec_class,
            port_type_name,
        )
        self.registry.register_knime_to_py_converter(
            module_name,
            converter_class_name,
            java_obj_class,
            java_spec_class,
            port_type_name,
        )
        return port_type

    def test_can_convert_spec_to_python(self):
        self.assertTrue(
            self.registry.can_convert_spec_to_python(self.extension_java_spec)
        )
        self.assertFalse(self.registry.can_convert_spec_to_python("some.other.Spec"))

    def test_can_convert_obj_to_python(self):
        self.assertTrue(
            self.registry.can_convert_obj_to_python(self.extension_port_type.id)
        )
        self.assertFalse(self.registry.can_convert_obj_to_python("some.other.Class"))

    def test_can_convert_spec_from_python(self):
        self.assertTrue(
            self.registry.can_convert_spec_from_python(
                MockExtensionPortObjectSpec("foo")
            )
        )
        self.assertFalse(self.registry.can_convert_spec_from_python("foo"))

    def test_can_convert_obj_from_python(self):
        self.assertTrue(
            self.registry.can_convert_obj_from_python(
                MockExtensionPortObject("foo", MockExtensionPortObjectSpec("bar"))
            )
        )
        self.assertFalse(self.registry.can_convert_obj_from_python("foo"))

    def test_extension_port_object_from_python(self):
        obj_container = self.registry.port_object_from_python(
            MockExtensionPortObject("foo", MockExtensionPortObjectSpec("bar")),
        )

        self.assertIsInstance(obj_container, _PortObjectContainer)
        transfer = obj_container.getTransfer()
        self.assertIsInstance(transfer, StringPythonTransfer)
        self.assertEqual(transfer.getStringRepresentation(), "foo")

    def test_extension_port_object_to_python(self):
        obj = self._obj_to_python(
            StringPythonTransfer("foo"),
            StringPythonTransfer("bar"),
            self.extension_java_spec,
        )
        self.assertIsInstance(obj, MockExtensionPortObject)
        self.assertEqual(obj.data, "foo")

    def test_extension_spec_from_python(self):
        spec_container = self.registry.spec_from_python(
            MockExtensionPortObjectSpec("bar")
        )

        self.assertIsInstance(spec_container, _PortObjectSpecContainer)
        transfer = spec_container.getTransfer()
        self.assertIsInstance(transfer, StringPythonTransfer)
        self.assertEqual(transfer.getStringRepresentation(), "bar")

    def test_extension_spec_to_python(self):
        transfer = StringPythonTransfer("boink")
        spec = self._spec_to_python(transfer, self.extension_java_spec)
        self.assertIsInstance(spec, MockExtensionPortObjectSpec)
        self.assertEqual(spec.data, "boink")

    def test_credential_spec_to_python(self):
        transfer = StringPythonTransfer(json.dumps({"data": "<dummy_xml>"}))
        spec = self._spec_to_python(transfer, self.credential_java_spec)
        self.assertIsInstance(spec, ks.CredentialPortObjectSpec)
        self.assertEqual(spec._xml_data, "<dummy_xml>")

    def test_credential_obj_to_python(self):
        transfer = StringPythonTransfer("")
        spec_transfer = StringPythonTransfer(json.dumps({"data": "<dummy_xml>"}))
        obj = self._obj_to_python(
            transfer,
            spec_transfer,
            self.credential_java_spec,
        )
        self.assertEqual(obj.spec._xml_data, "<dummy_xml>")

    def test_credential_spec_from_python(self):
        spec_container = self._spec_from_python(
            ks.CredentialPortObjectSpec("<dummy_xml>")
        )
        self.assertIsInstance(spec_container, _PortObjectSpecContainer)
        transfer = spec_container.getTransfer()
        self.assertIsInstance(transfer, StringPythonTransfer)
        self.assertEqual(
            transfer.getStringRepresentation(),
            json.dumps(
                {
                    "data": "<dummy_xml>",
                }
            ),
        )

    def test_credential_obj_from_python(self):
        container = self._obj_from_python(
            pi._PythonCredentialPortObject(ks.CredentialPortObjectSpec("<dummy_xml>")),
        )
        transfer = container.getTransfer()
        self.assertIsInstance(transfer, NonePythonTransfer)

    def test_hub_auth_spec_to_python(self):
        transfer = StringPythonTransfer(
            json.dumps({"data": "<dummy_xml>", "hub_url": "https://hub.knime.com"})
        )
        spec = self._spec_to_python(transfer, self.hub_auth_java_spec)
        self.assertIsInstance(spec, ks.HubAuthenticationPortObjectSpec)
        self.assertEqual(spec._xml_data, "<dummy_xml>")
        self.assertEqual(spec._hub_url, "https://hub.knime.com")

    def test_hub_auth_obj_to_python(self):
        transfer = StringPythonTransfer("")
        spec_transfer = StringPythonTransfer(
            json.dumps({"data": "<dummy_xml>", "hub_url": "https://hub.knime.com"})
        )
        obj = self._obj_to_python(transfer, spec_transfer, self.hub_auth_java_spec)
        self.assertEqual(obj.spec._xml_data, "<dummy_xml>")
        self.assertEqual(obj.spec._hub_url, "https://hub.knime.com")

    def test_hub_auth_spec_from_python(self):
        spec_container = self._spec_from_python(
            ks.HubAuthenticationPortObjectSpec("<dummy_xml>", "https://hub.knime.com"),
        )
        self.assertIsInstance(spec_container, _PortObjectSpecContainer)
        transfer = spec_container.getTransfer()
        self.assertIsInstance(transfer, StringPythonTransfer)
        self.assertEqual(
            transfer.getStringRepresentation(),
            json.dumps(
                {
                    "data": "<dummy_xml>",
                    "hub_url": "https://hub.knime.com",
                }
            ),
        )

    def test_hub_auth_obj_from_python(self):
        container = self._obj_from_python(
            pi._PythonHubAuthenticationPortObject(
                ks.HubAuthenticationPortObjectSpec(
                    "<dummy_xml>", "https://hub.knime.com"
                )
            ),
        )
        transfer = container.getTransfer()
        self.assertIsInstance(transfer, NonePythonTransfer)

    def _spec_to_python(self, transfer, java_spec: str) -> PortObjectSpec:
        spec_container = _PortObjectSpecContainer(transfer, java_spec)
        return self.registry.spec_to_python(spec_container)

    def _spec_from_python(self, spec: PortObjectSpec) -> _PortObjectSpecContainer:
        return self.registry.spec_from_python(spec)

    def _obj_to_python(self, obj_transfer, spec_transfer, java_spec: str) -> PortObject:
        obj_container = _PortObjectContainer(
            obj_transfer,
            self.extension_port_type.id,
            _PortObjectSpecContainer(spec_transfer, java_class_name=java_spec),
        )
        return self.registry.port_object_to_python(obj_container)

    def _obj_from_python(self, obj: PortObject) -> _PortObjectContainer:
        return self.registry.port_object_from_python(obj)
