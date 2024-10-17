import json
from typing import Generic, Optional, Type, TypeVar
import knime.api.schema as ks
import datetime as dt
import knime.extension.ports as kpo


#### Credentials


class _PythonCredentialPortObject:
    def __init__(self, spec: ks.CredentialPortObjectSpec):
        self._spec = spec

    @property
    def spec(self) -> ks.CredentialPortObjectSpec:
        return self._spec

    def get_auth_schema(self) -> str:
        return self._spec.auth_schema

    def get_auth_parameters(self) -> str:
        return self._spec.auth_parameters

    def get_expires_after(self) -> Optional[dt.datetime]:
        return self._spec.expires_after


CREDENTIAL_OBJ = TypeVar("CREDENTIAL_OBJ", bound=_PythonCredentialPortObject)

CREDENTIAL_SPEC = TypeVar("CREDENTIAL_SPEC", bound=ks.CredentialPortObjectSpec)


class CredentialConverter(
    Generic[CREDENTIAL_OBJ, CREDENTIAL_SPEC],
    kpo.BidirectionalPortObjectConverter[
        CREDENTIAL_OBJ,
        kpo.StringPythonTransfer,
        CREDENTIAL_SPEC,
        kpo.StringPythonTransfer,
    ],
):
    def __init__(
        self,
        obj_type: Type[CREDENTIAL_OBJ] = _PythonCredentialPortObject,
        spec_type: Type[CREDENTIAL_SPEC] = ks.CredentialPortObjectSpec,
    ) -> None:
        super().__init__(obj_type, spec_type)

    def convert_obj_to_python(
        self, transfer: kpo.NonePythonTransfer, spec: CREDENTIAL_SPEC
    ) -> CREDENTIAL_OBJ:
        return self.object_type(spec)

    def convert_obj_from_python(
        self, port_object: CREDENTIAL_OBJ
    ) -> kpo.NonePythonTransfer:
        return kpo.NonePythonTransfer()

    def convert_spec_to_python(
        self, transfer: kpo.StringPythonTransfer
    ) -> CREDENTIAL_SPEC:
        data = transfer.getStringRepresentation()
        return self.spec_type.deserialize(json.loads(data))

    def convert_spec_from_python(
        self, spec: CREDENTIAL_SPEC
    ) -> kpo.StringPythonTransfer:
        return kpo.StringPythonTransfer(json.dumps(spec.serialize()))


#### Hub Authentication


class _PythonHubAuthenticationPortObject(_PythonCredentialPortObject):
    @property
    def spec(self) -> ks.HubAuthenticationPortObjectSpec:
        return self._spec


class HubAuthenticationConverter(
    CredentialConverter[
        _PythonHubAuthenticationPortObject, ks.HubAuthenticationPortObjectSpec
    ]
):
    def __init__(self) -> None:
        super().__init__(
            _PythonHubAuthenticationPortObject, ks.HubAuthenticationPortObjectSpec
        )
