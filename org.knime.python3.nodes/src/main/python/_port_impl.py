import json
from typing import Callable, Dict, Optional, Union
import knime.api.schema as ks
import knime.extension.nodes as kn
import knime._arrow._table as kat
import knime._backend._gateway as kg
import knime.api.table as kt
import datetime as dt


class _PythonPortObjectSpec:
    def __init__(self, java_class_name, data_dict: Dict):
        self._java_class_name = java_class_name
        self._json_string_data = json.dumps(data_dict)
        self._data = data_dict

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def toJsonString(self) -> str:  # NOSONAR - Java naming conventions
        return self._json_string_data

    def toString(self) -> str:  # NOSONAR - Java naming conventions
        """For debugging on the Java side"""
        return self._json_string_data

    @property
    def data(self) -> Dict:
        if self._data is None:
            # to be able to access the data dict when we're accessing a Java class
            self._data = json.loads(self.toJsonString())
        return self._data

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObjectSpec"
        ]


def _extract_data(spec):
    return json.loads(spec.toJsonString())


##### Table


class _PythonTablePortObject:
    def __init__(self, java_class_name, sink):
        self._java_class_name = java_class_name
        self._sink = sink

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def getPythonArrowDataSink(self):  # NOSONAR - Java naming conventions
        return self._sink

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonTablePortObject"
        ]


_bdt_java_type = "org.knime.core.node.BufferedDataTable"


def schema_to_python(spec, **kwargs) -> ks.Schema:
    data = _extract_data(spec)
    return ks.Schema.deserialize(data)


def schema_from_python(
    spec: Union[ks.Column, ks.Schema], **kwargs
) -> _PythonPortObjectSpec:
    if isinstance(spec, ks.Column):
        spec = ks.Schema.from_columns(spec)
    assert isinstance(spec, ks.Schema)
    data = spec.serialize()
    class_name = "org.knime.core.data.DataTableSpec"
    return _PythonPortObjectSpec(java_class_name=class_name, data_dict=data)


def table_to_python(obj, port: kn.Port, **kwargs):
    assert port.type == kn.PortType.TABLE
    java_source = obj.getDataSource()
    return kat.ArrowSourceTable(kg.data_source_mapper(java_source))


def table_from_python(obj: kt.Table, **kwargs):
    java_data_sink = None
    if isinstance(obj, kat.ArrowTable):
        sink = kt._backend.create_sink()
        obj._write_to_sink(sink)
        java_data_sink = sink._java_data_sink
    elif isinstance(obj, kat.ArrowBatchOutputTable):
        sink = obj._sink
        java_data_sink = sink._java_data_sink
    else:
        raise TypeError(
            f"Object should be of type Table or BatchOutputTable, but got {type(obj)}"
        )

    return _PythonTablePortObject(_bdt_java_type, java_data_sink)


##### raw binary


class _PythonBinaryPortObject:
    def __init__(self, java_class_name, filestore_file, spec):
        self._java_class_name = java_class_name
        self._spec = spec
        self._key = filestore_file.get_key()

    @classmethod
    def from_bytes(
        cls,
        java_class_name: str,
        filestore_file,
        data: bytes,
        spec: _PythonPortObjectSpec,
    ) -> "_PythonBinaryPortObject":
        with open(filestore_file.get_file_path(), "wb") as f:
            f.write(data)
        return cls(java_class_name, filestore_file, spec)

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return self._java_class_name

    def getSpec(self) -> _PythonPortObjectSpec:  # NOSONAR - Java naming conventions
        return self._spec

    def getFileStoreKey(self) -> str:  # NOSONAR - Java naming conventions
        return self._key

    def toString(self) -> str:  # NOSONAR - Java naming conventions
        """For debugging on the Java side"""
        return f"PythonBinaryPortObject[spec={self._spec.toJsonString()}]"

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonBinaryPortObject"
        ]


def binary_spec_to_python(spec, port: kn.Port, **kwargs) -> ks.BinaryPortObjectSpec:
    data = _extract_data(spec)
    bpos = ks.BinaryPortObjectSpec.deserialize(data)
    assert (
        bpos.id == port.id
    ), f"Expected binary input port ID {port.id} but got {bpos.id}"
    return bpos


def binary_spec_from_python(
    spec: ks.BinaryPortObjectSpec, port: kn.Port, **kwargs
) -> _PythonPortObjectSpec:
    assert (
        port.id == spec.id
    ), f"Expected binary output port ID {port.id} but got {spec.id}"

    data = spec.serialize()
    class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec"
    return _PythonPortObjectSpec(class_name, data_dict=data)


def binary_obj_to_python(obj, **kwargs) -> bytes:
    file = obj.getFilePath()
    with open(file, "rb") as f:
        return f.read()


def binary_obj_from_python(
    obj: bytes, port: kn.Port, file_creator: Callable, **kwargs
) -> _PythonBinaryPortObject:
    if not isinstance(obj, bytes):
        tb = None
        raise TypeError(
            f"Binary Port can only process objects of type bytes, not {type(obj)}"
        ).with_traceback(tb)

    class_name = "org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject"
    spec = _PythonPortObjectSpec(
        "org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec",
        {"id": port.id},
    )
    return _PythonBinaryPortObject.from_bytes(class_name, file_creator(), obj, spec)


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

    def getSpec(self) -> _PythonPortObjectSpec:  # NOSONAR - Java naming conventions
        # wrap as a PythonPortObjectSpec
        java_class_name = (
            "org.knime.python3.nodes.ports.PythonPortObjects$PythonPortObjectSpec"
        )
        spec = _PythonPortObjectSpec(
            java_class_name,
            self._spec.serialize(),
        )
        return spec

    def getJavaClassName(self) -> str:  # NOSONAR - Java naming conventions
        return "org.knime.credentials.base.CredentialPortObject"

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonCredentialPortObject"
        ]

    def toString(self):  # NOSONAR - Java naming conventions
        return f"PythonCredentialPortObject[spec={self._spec.serialize()}]"


def credential_spec_to_python(
    spec, java_callback, **kwargs
) -> ks.CredentialPortObjectSpec:
    data = _extract_data(spec)
    return ks.CredentialPortObjectSpec.deserialize(data, java_callback)


def credential_spec_from_python(spec, **kwargs) -> _PythonPortObjectSpec:
    assert isinstance(spec, ks.CredentialPortObjectSpec)
    data = spec.serialize()
    class_name = "org.knime.credentials.base.CredentialPortObjectSpec"
    return _PythonPortObjectSpec(class_name, data_dict=data)


def credential_obj_to_python(
    obj, port, java_callback, **kwargs
) -> _PythonCredentialPortObject:
    spec = credential_spec_to_python(obj.getSpec(), port, java_callback)
    return _PythonCredentialPortObject(spec)


def credential_obj_from_python(obj, **kwargs):
    # the _PythonCredentialPortObject is already compatible with Java
    return obj


#### Hub Authentication


class _PythonHubAuthenticationPortObject(_PythonCredentialPortObject):
    def __init__(self, spec: ks.HubAuthenticationPortObjectSpec):
        super().__init__(spec)

    @property
    def spec(self) -> ks.HubAuthenticationPortObjectSpec:
        return self._spec


def hub_auth_spec_to_python(
    spec, java_callback, **kwargs
) -> ks.HubAuthenticationPortObjectSpec:
    data = _extract_data(spec)
    return ks.HubAuthenticationPortObjectSpec.deserialize(data, java_callback)


def hub_auth_obj_to_python(
    obj, java_callback, **kwargs
) -> _PythonHubAuthenticationPortObject:
    spec = hub_auth_spec_to_python(obj.getSpec(), java_callback=java_callback)
    return _PythonHubAuthenticationPortObject(spec)


#### Images


class _PythonImagePortObject:
    def __init__(self, data):
        import base64

        if isinstance(data, str):  # string potentially representing an SVG image
            data = data.encode("utf-8")
        self._img_bytes = base64.b64encode(data).decode("utf-8")

    # TODO is this needed? The extension point contains that information on java side
    def getJavaClassName(self) -> str:  # NOSONAR
        return "org.knime.core.node.port.image.ImagePortObject"

    def getImageBytes(self) -> str:  # NOSONAR
        return self._img_bytes

    class Java:
        implements = [
            "org.knime.python3.nodes.ports.PythonPortObjects$PurePythonImagePortObject"
        ]


def image_spec_from_python(
    spec: ks.ImagePortObjectSpec, **kwargs
) -> _PythonPortObjectSpec:
    assert isinstance(spec, ks.ImagePortObjectSpec)
    # TODO this assertion should happen in the spec
    assert any(
        spec.format == option.value for option in kn.ImageFormat
    ), f"Expected image formats are: {kn.ImageFormat.available_options()}."
    data = spec.serialize()
    class_name = "org.knime.core.node.port.image.ImagePortObjectSpec"
    return _PythonPortObjectSpec(java_class_name=class_name, data_dict=data)


def image_obj_from_python(obj, **kwargs) -> _PythonImagePortObject:
    return _PythonImagePortObject(obj)
