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
    KnimeToPyPortObjectConverter,
    PyToKnimePortObjectConverter,
    PythonPortObjectSpecTransfer,
    PythonPortObjectTransfer,
)


JavaClassName = str


class _PortObjectSpecContainer:
    def __init__(
        self,
        transfer: PythonPortObjectSpecTransfer,
        java_class_name: JavaClassName,
    ) -> None:
        self._transfer = transfer
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> str:
        return self._java_class_name

    def getTransfer(self) -> PythonPortObjectSpecTransfer:
        return self._transfer

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.ExtensionPortObjectConverter$PyToKnimeSpecContainer"
        ]


class _PortObjectContainer:
    def __init__(
        self,
        transfer: PythonPortObjectTransfer,
        java_class_name: JavaClassName,
        spec_container: Optional[_PortObjectSpecContainer] = None,
    ) -> None:
        self._transfer = transfer
        self._spec_container = spec_container
        self._java_class_name = java_class_name

    def getJavaClassName(self) -> str:
        return self._java_class_name

    def getTransfer(self) -> PythonPortObjectTransfer:
        return self._transfer

    def getSpecContainer(self) -> _PortObjectSpecContainer:
        return self._spec_container

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.ExtensionPortObjectConverter$PyToKnimeObjContainer"
        ]


@dataclass
class PyToKnimeConverterEntry:
    converter: PyToKnimePortObjectConverter
    java_obj_class_name: JavaClassName
    java_spec_class_name: JavaClassName


class JavaPortTypeRegistry:
    def __init__(
        self,
    ) -> None:
        self._knime_to_py_by_obj_class: Dict[
            JavaClassName, KnimeToPyPortObjectConverter
        ] = {}
        self._knime_to_py_by_spec_class: Dict[
            JavaClassName, KnimeToPyPortObjectConverter
        ] = {}
        self._py_to_knime_for_spec_type: Dict[Type, PyToKnimeConverterEntry] = {}
        self._py_to_knime_for_obj_type: Dict[Type, PyToKnimeConverterEntry] = {}

        self._port_types_by_id: Dict[str, kn.PortType] = {}

    def register_knime_to_py_converter(
        self,
        module_name: str,
        converter_class_name: str,
        java_obj_class_name: JavaClassName,
        java_spec_class_name: JavaClassName,
        port_type_name: str,
    ) -> kn.PortType:
        if java_obj_class_name in self._knime_to_py_by_obj_class:
            raise ValueError(
                f"Converter for object class {java_obj_class_name} already registered."
            )
        if java_spec_class_name in self._knime_to_py_by_spec_class:
            raise ValueError(
                f"Converter for spec class {java_spec_class_name} already registered."
            )
        converter: KnimeToPyPortObjectConverter = self._load_converter_from_module(
            module_name, converter_class_name
        )
        self._knime_to_py_by_obj_class[java_obj_class_name] = converter

        self._knime_to_py_by_spec_class[java_spec_class_name] = converter

        return self._register_java_port_type(
            java_obj_class_name, port_type_name, converter
        )

    def register_py_to_knime_converter(
        self,
        module_name: str,
        converter_class_name: str,
        java_obj_class_name: JavaClassName,
        java_spec_class_name: JavaClassName,
        port_type_name: str,
    ) -> kn.PortType:
        converter: PyToKnimePortObjectConverter = self._load_converter_from_module(
            module_name, converter_class_name
        )
        if converter.object_type in self._py_to_knime_for_obj_type:
            raise ValueError(
                f"Converter for object type {converter.object_type} already registered."
            )
        if converter.spec_type in self._py_to_knime_for_spec_type:
            raise ValueError(
                f"Converter for spec type {converter.spec_type} already registered."
            )
        port_type = self._register_java_port_type(
            java_obj_class_name, port_type_name, converter
        )
        converter_entry = PyToKnimeConverterEntry(
            converter,
            java_obj_class_name,
            java_spec_class_name,
        )
        self._py_to_knime_for_obj_type[converter.object_type] = converter_entry
        self._py_to_knime_for_spec_type[converter.spec_type] = converter_entry

        return port_type

    def _register_java_port_type(
        self,
        java_class_name: JavaClassName,
        port_type_name: str,
        converter: Union[PyToKnimePortObjectConverter, KnimeToPyPortObjectConverter],
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
    ) -> Union[KnimeToPyPortObjectConverter, PyToKnimePortObjectConverter]:
        python_module = importlib.import_module(module_name)
        converter_class = getattr(python_module, converter_class_name)
        return converter_class()

    def get_port_type_for_id(self, id: str) -> kn.PortType:
        return self._port_types_by_id.get(id)

    def spec_to_python(
        self,
        container: _PortObjectSpecContainer,
    ) -> kn.PortObjectSpec:
        converter = self._knime_to_py_by_spec_class.get(container.getJavaClassName())
        return converter.convert_spec_to_python(
            transfer=container.getTransfer(),
        )

    def can_convert_spec_to_python(self, java_class_name: JavaClassName) -> bool:
        return java_class_name in self._knime_to_py_by_spec_class

    def can_convert_obj_to_python(self, java_class_name: JavaClassName) -> bool:
        return java_class_name in self._knime_to_py_by_obj_class

    def can_convert_spec_from_python(self, spec: kn.PortObjectSpec) -> bool:
        return (
            self._find_py_to_knime_converter_for_type(
                type(spec), self._py_to_knime_for_spec_type
            )
            is not None
        )

    def can_convert_obj_from_python(self, obj: kn.PortObject) -> bool:
        return (
            self._find_py_to_knime_converter_for_type(
                type(obj), self._py_to_knime_for_obj_type
            )
            is not None
        )

    def port_object_to_python(
        self,
        container: _PortObjectContainer,
    ) -> kn.PortObject:
        converter = self._knime_to_py_by_obj_class[container.getJavaClassName()]
        spec_container = container.getSpecContainer()
        spec = self.spec_to_python(spec_container) if spec_container else None
        return converter.convert_obj_to_python(
            transfer=container.getTransfer(), spec=spec
        )

    def spec_from_python(
        self,
        spec: kn.PortObjectSpec,
    ) -> _PortObjectSpecContainer:
        entry = self._find_py_to_knime_converter_for_type(
            type(spec), self._py_to_knime_for_spec_type
        )
        if entry is None:
            raise ValueError(f"No converter found for type {type(spec)}")
        transfer = entry.converter.convert_spec_from_python(
            spec=spec,
        )
        return _PortObjectSpecContainer(transfer, entry.java_spec_class_name)

    def port_object_from_python(self, obj: kn.PortObject) -> _PortObjectContainer:
        entry = self._find_py_to_knime_converter_for_type(
            type(obj), self._py_to_knime_for_obj_type
        )
        if entry is None:
            raise ValueError(f"No converter found for type {type(obj)}")
        transfer = entry.converter.convert_obj_from_python(
            port_object=obj,
        )
        spec_container = (
            self.spec_from_python(obj.spec) if hasattr(obj, "spec") else None
        )
        return _PortObjectContainer(transfer, entry.java_obj_class_name, spec_container)

    def _find_py_to_knime_converter_for_type(
        self, type_: Type, registry: Dict[Type, PyToKnimeConverterEntry]
    ) -> PyToKnimeConverterEntry:
        super_types = inspect.getmro(type_)
        for super_type in super_types:
            if super_type in registry:
                return registry[super_type]
