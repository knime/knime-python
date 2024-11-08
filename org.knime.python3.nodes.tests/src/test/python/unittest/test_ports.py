from dataclasses import dataclass
import json
import unittest
from _ports import (
    JavaPortTypeRegistry,
    _ExtensionPortObject,
    _ExtensionPortObjectSpec,
    RegisteredPortObjectEncoder,
)
from knime.extension.ports import (
    EmptyIntermediateRepresentation,
    StringIntermediateRepresentation,
    PortObjectConverter,
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
    PortObjectConverter[
        MockExtensionPortObject,
        StringIntermediateRepresentation,
        MockExtensionPortObjectSpec,
        StringIntermediateRepresentation,
    ]
):
    def __init__(self) -> None:
        super().__init__(MockExtensionPortObject, MockExtensionPortObjectSpec)

    def decode_object(
        self,
        intermediate_representation: StringIntermediateRepresentation,
        spec: MockExtensionPortObjectSpec,
    ) -> MockExtensionPortObject:
        return MockExtensionPortObject(
            intermediate_representation.getStringRepresentation(), spec
        )

    def encode_object(
        self, port_object: MockExtensionPortObject
    ) -> StringIntermediateRepresentation:
        return StringIntermediateRepresentation(port_object.data)

    def decode_spec(
        self,
        intermediate_representation: StringIntermediateRepresentation,
    ) -> MockExtensionPortObjectSpec:
        return MockExtensionPortObjectSpec(
            intermediate_representation.getStringRepresentation()
        )

    def encode_spec(
        self,
        spec: MockExtensionPortObjectSpec,
    ) -> StringIntermediateRepresentation:
        return StringIntermediateRepresentation(spec.data)


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
        self.assertIn(obj_java_class_name, self.registry._decoder_for_obj_class)
        self.assertIsInstance(
            self.registry._decoder_for_obj_class[obj_java_class_name],
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
        entry = self.registry._encoder_for_obj_type[MockExtensionPortObject]
        self.assertIsInstance(
            entry,
            RegisteredPortObjectEncoder,
        )
        self.assertIsInstance(entry.converter, MockExtensionPortObjectConverter)
        entry = self.registry._encoder_for_spec_type[MockExtensionPortObjectSpec]
        self.assertIsInstance(
            entry,
            RegisteredPortObjectEncoder,
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
        self.assertTrue(self.registry.can_decode_spec(self.extension_java_spec))
        self.assertFalse(self.registry.can_decode_spec("some.other.Spec"))

    def test_can_convert_obj_to_python(self):
        self.assertTrue(
            self.registry.can_decode_port_object(self.extension_port_type.id)
        )
        self.assertFalse(self.registry.can_decode_port_object("some.other.Class"))

    def test_can_convert_spec_from_python(self):
        self.assertTrue(
            self.registry.can_encode_spec(MockExtensionPortObjectSpec("foo"))
        )
        self.assertFalse(self.registry.can_encode_spec("foo"))

    def test_can_convert_obj_from_python(self):
        self.assertTrue(
            self.registry.can_encode_port_object(
                MockExtensionPortObject("foo", MockExtensionPortObjectSpec("bar"))
            )
        )
        self.assertFalse(self.registry.can_encode_port_object("foo"))

    def test_extension_port_object_from_python(self):
        obj_container = self.registry.encode_port_object(
            MockExtensionPortObject("foo", MockExtensionPortObjectSpec("bar")),
        )

        self.assertIsInstance(obj_container, _ExtensionPortObject)
        intermediate_representation = obj_container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, StringIntermediateRepresentation
        )
        self.assertEqual(intermediate_representation.getStringRepresentation(), "foo")

    def test_extension_port_object_to_python(self):
        obj = self._obj_to_python(
            StringIntermediateRepresentation("foo"),
            StringIntermediateRepresentation("bar"),
            self.extension_java_spec,
        )
        self.assertIsInstance(obj, MockExtensionPortObject)
        self.assertEqual(obj.data, "foo")

    def test_extension_spec_from_python(self):
        spec_container = self.registry.encode_spec(MockExtensionPortObjectSpec("bar"))

        self.assertIsInstance(spec_container, _ExtensionPortObjectSpec)
        intermediate_representation = spec_container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, StringIntermediateRepresentation
        )
        self.assertEqual(intermediate_representation.getStringRepresentation(), "bar")

    def test_extension_spec_to_python(self):
        intermediate_representation = StringIntermediateRepresentation("boink")
        spec = self._spec_to_python(
            intermediate_representation, self.extension_java_spec
        )
        self.assertIsInstance(spec, MockExtensionPortObjectSpec)
        self.assertEqual(spec.data, "boink")

    def test_credential_spec_to_python(self):
        intermediate_representation = StringIntermediateRepresentation(
            json.dumps({"data": "<dummy_xml>"})
        )
        spec = self._spec_to_python(
            intermediate_representation, self.credential_java_spec
        )
        self.assertIsInstance(spec, ks.CredentialPortObjectSpec)
        self.assertEqual(spec._xml_data, "<dummy_xml>")

    def test_credential_obj_to_python(self):
        intermediate_representation = StringIntermediateRepresentation("")
        spec_transfer = StringIntermediateRepresentation(
            json.dumps({"data": "<dummy_xml>"})
        )
        obj = self._obj_to_python(
            intermediate_representation,
            spec_transfer,
            self.credential_java_spec,
        )
        self.assertEqual(obj.spec._xml_data, "<dummy_xml>")

    def test_credential_spec_from_python(self):
        spec_container = self._spec_from_python(
            ks.CredentialPortObjectSpec("<dummy_xml>")
        )
        self.assertIsInstance(spec_container, _ExtensionPortObjectSpec)
        intermediate_representation = spec_container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, StringIntermediateRepresentation
        )
        self.assertEqual(
            intermediate_representation.getStringRepresentation(),
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
        intermediate_representation = container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, EmptyIntermediateRepresentation
        )

    def test_hub_auth_spec_to_python(self):
        intermediate_representation = StringIntermediateRepresentation(
            json.dumps({"data": "<dummy_xml>", "hub_url": "https://hub.knime.com"})
        )
        spec = self._spec_to_python(
            intermediate_representation, self.hub_auth_java_spec
        )
        self.assertIsInstance(spec, ks.HubAuthenticationPortObjectSpec)
        self.assertEqual(spec._xml_data, "<dummy_xml>")
        self.assertEqual(spec._hub_url, "https://hub.knime.com")

    def test_hub_auth_obj_to_python(self):
        intermediate_representation = StringIntermediateRepresentation("")
        spec_transfer = StringIntermediateRepresentation(
            json.dumps({"data": "<dummy_xml>", "hub_url": "https://hub.knime.com"})
        )
        obj = self._obj_to_python(
            intermediate_representation, spec_transfer, self.hub_auth_java_spec
        )
        self.assertEqual(obj.spec._xml_data, "<dummy_xml>")
        self.assertEqual(obj.spec._hub_url, "https://hub.knime.com")

    def test_hub_auth_spec_from_python(self):
        spec_container = self._spec_from_python(
            ks.HubAuthenticationPortObjectSpec("<dummy_xml>", "https://hub.knime.com"),
        )
        self.assertIsInstance(spec_container, _ExtensionPortObjectSpec)
        intermediate_representation = spec_container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, StringIntermediateRepresentation
        )
        self.assertEqual(
            intermediate_representation.getStringRepresentation(),
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
        intermediate_representation = container.getIntermediateRepresentation()
        self.assertIsInstance(
            intermediate_representation, EmptyIntermediateRepresentation
        )

    def _spec_to_python(
        self, intermediate_representation, java_spec: str
    ) -> PortObjectSpec:
        spec_container = _ExtensionPortObjectSpec(
            intermediate_representation, java_spec
        )
        return self.registry.decode_spec(spec_container)

    def _spec_from_python(self, spec: PortObjectSpec) -> _ExtensionPortObjectSpec:
        return self.registry.encode_spec(spec)

    def _obj_to_python(self, obj_transfer, spec_transfer, java_spec: str) -> PortObject:
        obj_container = _ExtensionPortObject(
            obj_transfer,
            self.extension_port_type.id,
            _ExtensionPortObjectSpec(spec_transfer, java_class_name=java_spec),
        )
        return self.registry.decode_port_object(obj_container)

    def _obj_from_python(self, obj: PortObject) -> _ExtensionPortObject:
        return self.registry.encode_port_object(obj)
