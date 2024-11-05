from abc import ABC, abstractmethod
from typing import Generic, Optional, Type, TypeVar


class IntermediateRepresentation(ABC):
    pass


class PortObjectSpecIntermediateRepresentation(IntermediateRepresentation):
    pass


class PortObjectIntermediateRepresentation(IntermediateRepresentation):
    pass


class StringIntermediateRepresentation(
    PortObjectSpecIntermediateRepresentation, PortObjectIntermediateRepresentation
):
    def __init__(self, representation: str) -> None:
        self._representation = representation

    def getStringRepresentation(self) -> str:
        return self._representation


class EmptyIntermediateRepresentation(PortObjectIntermediateRepresentation):
    pass


SPEC = TypeVar("SPEC")
OBJ = TypeVar("OBJ")
SPEC_IR = TypeVar("SPEC_IR", bound=PortObjectSpecIntermediateRepresentation)
OBJ_IR = TypeVar("OBJ_IR", bound=PortObjectIntermediateRepresentation)


class PortObjectDecoder(ABC, Generic[OBJ, OBJ_IR, SPEC, SPEC_IR]):
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
    def decode_spec(self, intermediate_representation: SPEC_IR) -> SPEC:
        pass

    @abstractmethod
    def decode_object(self, intermediate_representation: OBJ_IR, spec: SPEC) -> OBJ:
        pass

    @property
    def object_type(self) -> Type[OBJ]:
        # TODO complain if the type is None e.g. if devs don't provide it init or as a property
        return self._object_type

    @property
    def spec_type(self) -> Type[SPEC]:
        return self._spec_type


class PortObjectEncoder(ABC, Generic[OBJ, OBJ_IR, SPEC, SPEC_IR]):
    def __init__(
        self, object_type: Optional[Type[OBJ]] = None, spec_type: Optional[Type] = None
    ) -> None:
        self._object_type = object_type
        self._spec_type = spec_type

    @abstractmethod
    def encode_object(self, port_object: OBJ) -> OBJ_IR:
        pass

    @property
    def object_type(self) -> Type[OBJ]:
        return self._object_type

    @abstractmethod
    def encode_spec(self, spec: SPEC, port_info) -> SPEC_IR:
        pass

    @property
    def spec_type(self) -> Type[SPEC]:
        return self._spec_type


class PortObjectConverter(
    PortObjectDecoder[OBJ, OBJ_IR, SPEC, SPEC_IR],
    PortObjectEncoder[OBJ, OBJ_IR, SPEC, SPEC_IR],
):
    pass
