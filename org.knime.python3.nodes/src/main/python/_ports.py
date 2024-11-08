from dataclasses import dataclass
import importlib
import inspect
from typing import (
    Dict,
    Optional,
    Type,
    Union,
)
import knime.extension.nodes as kn
from knime.extension.ports import (
    PortObjectDecoder,
    PortObjectEncoder,
    PortObjectSpecIntermediateRepresentation,
    PortObjectIntermediateRepresentation,
)


JavaClassName = str


class _ExtensionPortObjectSpec:
    def __init__(
        self,
        intermediate_representation: PortObjectSpecIntermediateRepresentation,
        java_class_name: JavaClassName,
    ) -> None:
        self._intermediate_representation = intermediate_representation
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> JavaClassName:
        return self._java_class_name

    def getIntermediateRepresentation(self) -> PortObjectSpecIntermediateRepresentation:
        return self._intermediate_representation

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.ExtensionPortObjectConverters$PythonExtensionPortObjectSpec"
        ]


class _ExtensionPortObject:
    def __init__(
        self,
        intermediate_representation: PortObjectIntermediateRepresentation,
        java_class_name: JavaClassName,
        extension_spec: Optional[_ExtensionPortObjectSpec] = None,
    ) -> None:
        self._intermediate_representation = intermediate_representation
        self._extension_spec = extension_spec
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> JavaClassName:
        return self._java_class_name

    def getIntermediateRepresentation(self) -> PortObjectIntermediateRepresentation:
        return self._intermediate_representation

    def getSpec(self) -> _ExtensionPortObjectSpec:
        return self._extension_spec

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.ExtensionPortObjectConverters$PythonExtensionPortObject"
        ]


@dataclass
class RegisteredPortObjectEncoder:
    converter: PortObjectEncoder
    java_obj_class_name: JavaClassName
    java_spec_class_name: JavaClassName


class JavaPortTypeRegistry:
    def __init__(
        self,
    ) -> None:
        self._decoder_for_obj_class: Dict[JavaClassName, PortObjectDecoder] = {}
        self._decoder_for_spec_class: Dict[JavaClassName, PortObjectDecoder] = {}
        self._encoder_for_spec_type: Dict[Type, RegisteredPortObjectEncoder] = {}
        self._encoder_for_obj_type: Dict[Type, RegisteredPortObjectEncoder] = {}

        self._port_types_by_id: Dict[str, kn.PortType] = {}

    def register_knime_to_py_converter(
        self,
        module_name: str,
        converter_class_name: str,
        java_obj_class_name: JavaClassName,
        java_spec_class_name: JavaClassName,
        port_type_name: str,
    ) -> kn.PortType:
        if java_obj_class_name in self._decoder_for_obj_class:
            raise ValueError(
                f"Converter for object class {java_obj_class_name} already registered."
            )
        if java_spec_class_name in self._decoder_for_spec_class:
            raise ValueError(
                f"Converter for spec class {java_spec_class_name} already registered."
            )
        decoder: PortObjectDecoder = self._load_converter_from_module(
            module_name, converter_class_name
        )
        self._decoder_for_obj_class[java_obj_class_name] = decoder

        self._decoder_for_spec_class[java_spec_class_name] = decoder

        return self._register_java_port_type(
            java_obj_class_name, port_type_name, decoder
        )

    def register_py_to_knime_converter(
        self,
        module_name: str,
        converter_class_name: str,
        java_obj_class_name: JavaClassName,
        java_spec_class_name: JavaClassName,
        port_type_name: str,
    ) -> kn.PortType:
        encoder: PortObjectEncoder = self._load_converter_from_module(
            module_name, converter_class_name
        )
        if encoder.object_type in self._encoder_for_obj_type:
            raise ValueError(
                f"Converter for object type {encoder.object_type} already registered."
            )
        if encoder.spec_type in self._encoder_for_spec_type:
            raise ValueError(
                f"Converter for spec type {encoder.spec_type} already registered."
            )
        port_type = self._register_java_port_type(
            java_obj_class_name, port_type_name, encoder
        )
        registered_encoder = RegisteredPortObjectEncoder(
            encoder,
            java_obj_class_name,
            java_spec_class_name,
        )
        self._encoder_for_obj_type[encoder.object_type] = registered_encoder
        self._encoder_for_spec_type[encoder.spec_type] = registered_encoder

        return port_type

    def _register_java_port_type(
        self,
        java_class_name: JavaClassName,
        port_type_name: str,
        converter: Union[PortObjectEncoder, PortObjectDecoder],
    ) -> kn.PortType:
        if java_class_name not in self._port_types_by_id:
            self._port_types_by_id[java_class_name] = kn.PortType(
                java_class_name,
                port_type_name,
                converter.object_type,
                converter.spec_type,
            )

        return self._port_types_by_id[java_class_name]

    def _load_converter_from_module(
        self,
        module_name: str,
        converter_class_name: str,
    ) -> Union[PortObjectDecoder, PortObjectEncoder]:
        python_module = importlib.import_module(module_name)
        converter_class = getattr(python_module, converter_class_name)
        return converter_class()

    def get_port_type_for_id(self, id: str) -> kn.PortType:
        return self._port_types_by_id.get(id)

    def decode_spec(
        self,
        container: _ExtensionPortObjectSpec,
    ) -> kn.PortObjectSpec:
        converter = self._decoder_for_spec_class.get(container.getJavaClassName())
        return converter.decode_spec(
            intermediate_representation=container.getIntermediateRepresentation(),
        )

    def can_decode_spec(self, java_class_name: JavaClassName) -> bool:
        return java_class_name in self._decoder_for_spec_class

    def can_decode_port_object(self, java_class_name: JavaClassName) -> bool:
        return java_class_name in self._decoder_for_obj_class

    def can_encode_spec(self, spec: kn.PortObjectSpec) -> bool:
        return (
            self._find_encoder_for_type(type(spec), self._encoder_for_spec_type)
            is not None
        )

    def can_encode_port_object(self, obj: kn.PortObject) -> bool:
        return (
            self._find_encoder_for_type(type(obj), self._encoder_for_obj_type)
            is not None
        )

    def decode_port_object(
        self,
        extension_port_object: _ExtensionPortObject,
    ) -> kn.PortObject:
        decoder = self._decoder_for_obj_class[extension_port_object.getJavaClassName()]
        extension_spec = extension_port_object.getSpec()
        spec = self.decode_spec(extension_spec) if extension_spec else None
        return decoder.decode_object(
            intermediate_representation=extension_port_object.getIntermediateRepresentation(),
            spec=spec,
        )

    def encode_spec(
        self,
        spec: kn.PortObjectSpec,
    ) -> _ExtensionPortObjectSpec:
        entry = self._find_encoder_for_type(type(spec), self._encoder_for_spec_type)
        if entry is None:
            raise ValueError(f"No converter found for type {type(spec)}")
        intermediate_representation = entry.converter.encode_spec(
            spec=spec,
        )
        return _ExtensionPortObjectSpec(
            intermediate_representation, entry.java_spec_class_name
        )

    def encode_port_object(self, obj: kn.PortObject) -> _ExtensionPortObject:
        entry = self._find_encoder_for_type(type(obj), self._encoder_for_obj_type)
        if entry is None:
            raise ValueError(f"No converter found for type {type(obj)}")
        intermediate_representation = entry.converter.encode_object(
            port_object=obj,
        )
        spec_container = self.encode_spec(obj.spec) if hasattr(obj, "spec") else None
        return _ExtensionPortObject(
            intermediate_representation, entry.java_obj_class_name, spec_container
        )

    def _find_encoder_for_type(
        self, type_: Type, registry: Dict[Type, RegisteredPortObjectEncoder]
    ) -> RegisteredPortObjectEncoder:
        super_types = inspect.getmro(type_)
        for super_type in super_types:
            if super_type in registry:
                return registry[super_type]
