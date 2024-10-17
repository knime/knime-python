from abc import ABC, abstractmethod
from typing import Generic, Optional, Type, TypeVar


class PythonTransfer(ABC):
    pass


class PythonPortObjectSpecTransfer(PythonTransfer):
    pass


class PythonPortObjectTransfer(PythonTransfer):
    pass


class StringPythonTransfer(PythonPortObjectSpecTransfer, PythonPortObjectTransfer):
    def __init__(self, representation: str) -> None:
        self._representation = representation

    def getStringRepresentation(self) -> str:
        return self._representation


class NonePythonTransfer(PythonPortObjectTransfer):
    pass


SPEC = TypeVar("SPEC")
OBJ = TypeVar("OBJ")
SPEC_TRANSFER = TypeVar("SPEC_TRANSFER", bound=PythonPortObjectSpecTransfer)
OBJ_TRANSFER = TypeVar("OBJ_TRANSFER", bound=PythonPortObjectTransfer)


class KnimeToPyPortObjectConverter(
    ABC, Generic[OBJ, OBJ_TRANSFER, SPEC, SPEC_TRANSFER]
):
    # TODO allow to specify the types via init, or make them abstract properties or both?
    # I also investigated if we can extract the types from the generics but it seems more tricky than I thought
    def __init__(
        self,
        object_type: Optional[Type[OBJ]] = None,
        spec_type: Optional[Type[SPEC]] = None,
    ) -> None:
        self._spec_type = spec_type
        self._object_type = object_type

    @abstractmethod
    def convert_spec_to_python(self, transfer: SPEC_TRANSFER) -> SPEC:
        pass

    @abstractmethod
    def convert_obj_to_python(self, transfer: OBJ_TRANSFER, spec: SPEC) -> OBJ:
        pass

    @property
    def object_type(self) -> Type[OBJ]:
        # TODO complain if the type is None e.g. if devs don't provide it init or as a property
        return self._object_type

    @property
    def spec_type(self) -> Type[SPEC]:
        return self._spec_type


class PyToKnimePortObjectConverter(
    ABC, Generic[OBJ, OBJ_TRANSFER, SPEC, SPEC_TRANSFER]
):
    def __init__(
        self, object_type: Optional[Type[OBJ]] = None, spec_type: Optional[Type] = None
    ) -> None:
        self._object_type = object_type
        self._spec_type = spec_type

    @abstractmethod
    def convert_obj_from_python(self, port_object: OBJ) -> OBJ_TRANSFER:
        pass

    @property
    def object_type(self) -> Type[OBJ]:
        return self._object_type

    @abstractmethod
    def convert_spec_from_python(self, spec: SPEC, port_info) -> SPEC_TRANSFER:
        pass

    @property
    def spec_type(self) -> Type[SPEC]:
        return self._spec_type


class BidirectionalPortObjectConverter(
    KnimeToPyPortObjectConverter[OBJ, OBJ_TRANSFER, SPEC, SPEC_TRANSFER],
    PyToKnimePortObjectConverter[OBJ, OBJ_TRANSFER, SPEC, SPEC_TRANSFER],
):
    pass
