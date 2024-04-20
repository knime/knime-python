# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

"""
Type system and schema definition for KNIME tables.

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

# --------------------------------------------------------------------
# Types
# --------------------------------------------------------------------
from abc import ABC, abstractmethod
import datetime as dt
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Type, Union

import logging
from enum import Enum, unique

LOGGER = logging.getLogger(__name__)


class KnimeType:
    """
    A base class representing a KnimeType.
    """

    def __init__(self):
        """
        Initialize a KnimeType object.

        Raises
        ------
        TypeError
            If the object is created directly instead of being subclassed.
        """
        if type(self) == KnimeType:
            raise TypeError(f"{type(self)} is not supposed to be created directly")

    def __repr__(self) -> str:
        # needed for nice list-of-type printing. __str__ should be implemented by subclasses
        return str(self)


@unique
class PrimitiveTypeId(Enum):
    """
    Primitive data types known to KNIME
    """

    INT = "int32"
    LONG = "int64"
    STRING = "string"
    DOUBLE = "double"
    BOOL = "bool"
    BLOB = "blob"
    NULL = "null"


@unique
class DictEncodingKeyType(Enum):
    """
    Key types that can be used for dictionary encoding.
    """

    INT = "INT_KEY"
    LONG = "LONG_KEY"
    BYTE = "BYTE_KEY"


class _PrimitiveTypeSingletonsMetaclass(type):
    """
    The metaclass for PrimitiveType ensures that only a single instance is created per type.
    """

    _instances_per_type = {}

    def __call__(
        cls,
        dtype: PrimitiveTypeId,
        dict_encoding_key: DictEncodingKeyType = None,
        *args,
        **kwargs,
    ):
        key = (dtype, dict_encoding_key)
        if key in cls._instances_per_type:
            return cls._instances_per_type[key]

        obj = cls.__new__(cls)
        obj.__init__(dtype, dict_encoding_key, *args, **kwargs)
        cls._instances_per_type[key] = obj
        return obj


class PrimitiveType(KnimeType, metaclass=_PrimitiveTypeSingletonsMetaclass):
    """
    Each data type is an instance of `PrimitiveType`. Due to the metaclass, we only
    ever create a single instance per type, so each data type is also a singleton.

    Primitive types have a type id (`INT`, `LONG`, `BOOL`, ...) and a dictionary encoding key
    type which is not `None` if the type is stored using dictionary encoding. This is currently
    only allowed for `STRING` and `BLOB` types.
    """

    def __init__(self, type_id: PrimitiveTypeId, key_type: DictEncodingKeyType = None):
        """
        Construct a PrimitiveType from a type_id and its dictionary encoding key_type.
        Multiple invocations of this constructor with the same arguments will return the
        same instance instead of a new object with the same configuration.

        Parameters
        ----------
        type_id : any
            A primitive type identifier.
        key_type : Any, optional
            The key_type for dictionary encoding or None to disable dict encoding.
        """
        if not isinstance(type_id, PrimitiveTypeId):
            raise TypeError(
                f"{self.__class__.__name__} expected a {PrimitiveTypeId} instance but got {type_id}"
            )
        if key_type is not None and not isinstance(key_type, DictEncodingKeyType):
            raise TypeError(
                f"key_type must be a valid DictEncodingKeyType, got {key_type}"
            )
        if (
            key_type is not None
            and type_id != PrimitiveTypeId.STRING
            and type_id != PrimitiveTypeId.BLOB
        ):
            raise TypeError(
                f"Dictionary only works for strings and binary blobs, not {type_id.value}"
            )
        self._type_id = type_id
        self._key_type = key_type

    @property
    def dict_encoding_key_type(self) -> Optional[DictEncodingKeyType]:
        """
        Get the encoding key type of a dictionary.

        Returns
        -------
        Optional[DictEncodingKeyType]
            The encoding key type of the dictionary, or None if not set.
        """
        return self._key_type

    def __str__(self) -> str:
        if self._key_type is not None:
            return f"{self._type_id.value}[dict_encoding={self._key_type.value}]"
        else:
            return self._type_id.value

    @property
    def plain_type(self) -> "PrimitiveType":
        """
        Returns a PrimitiveType with disabled dict encoding.

        Returns
        -------
        PrimitiveType
            A PrimitiveType object with disabled dict encoding.
        """
        return PrimitiveType(self._type_id)


class ListType(KnimeType):
    def __init__(self, inner_type: KnimeType):
        """
        Initialize a ListType object.

        Parameters
        ----------
        inner_type : KnimeType
            The inner type of the ListType object.

        Raises
        ------
        TypeError
            If the input inner_type is not of type KnimeType.

        """
        if not isinstance(inner_type, KnimeType):
            raise TypeError(
                f"Cannot create list type with inner type {inner_type}, must be a KnimeType"
            )
        self._inner_type = inner_type

    @property
    def inner_type(self) -> KnimeType:
        return self._inner_type

    def __eq__(self, other: object) -> bool:
        return (
            other.__class__ == self.__class__ and self._inner_type == other._inner_type
        )

    def __str__(self) -> str:
        return f"list<{str(self._inner_type)}>"

    def __hash__(self):
        return hash(str(self))


class StructType(KnimeType):
    def __init__(self, inner_types: Iterable[KnimeType]):
        """
        Initialize a struct type with inner types.

        Parameters
        ----------
        inner_types : Iterable[KnimeType]
            An iterable of KnimeType objects representing the inner types of the struct.

        Raises
        ------
        TypeError
            If any of the inner types is not a KnimeType.

        """
        for t in inner_types:
            if not isinstance(t, KnimeType):
                raise TypeError(
                    f"Cannot create struct type with inner type {t}, must be a KnimeType"
                )

        self._inner_types = inner_types

    @property
    def inner_types(self) -> Iterable[KnimeType]:
        return self._inner_types

    def __eq__(self, other: object) -> bool:
        return other.__class__ == self.__class__ and all(
            i == o for i, o in zip(self._inner_types, other._inner_types)
        )

    def __str__(self) -> str:
        return f"struct<{', '.join(str(t) for t in self._inner_types)}>"

    def __hash__(self):
        return hash(str(self))


class LogicalType(KnimeType):
    """
    A KNIME LogicalType allows to attach a logical meaning to an underlying physical storage_type
    type. This could e.g. be that a Python datetime object is stored as int64, so the logical_type
    is a date but the storage_type is int64.

    The logical_type attribute contains a JSON encoded description of the Java class in KNIME that
    can understand this kind of value. It is used to specify how KNIME reads data coming from Python.

    Some LogicalTypes, such as date and time formats, are also implemented on the Python
    side. For these, the value_type property represents the Python type, and these
    logical types can be created using the helper method knime.api.schema.logical(value_type) below.

    To allow multiple Python types to map to the same KNIME logical type, a
    proxy_type_converter can be provided. The proxy type converter is also a PythonValueFactory
    and must take care of the full type conversion, such that it populates the appropriate storage
    type of the underlying logical type.
    """

    def __init__(
        self,
        logical_type,
        storage_type: KnimeType,
        proxy_type_converter: Optional = None,
    ):
        """
        Construct a LogicalType from a logical_type, a storage_type and an optional proxy_type_converter.

        Parameters:
            logical_type : str
                The JSON encoded definition of the type in KNIME.
            storage_type : str
                The KnimeType that is actually used to store the data of this extension type.
            proxy_type_converter : Optional
                A proxy type that can be used on the Python side to read or write the values which are internally treated like the original logical type.
        """
        self._logical_type = logical_type
        self._storage_type = storage_type
        self._proxy_type_converter = proxy_type_converter

    @property
    def logical_type(self):
        """
        The JSON encoded definition of the type in KNIME.
        """
        return self._logical_type

    @property
    def storage_type(self) -> KnimeType:
        """
        The KnimeType that is actually used to store the data of this extension type.
        """
        return self._storage_type

    @property
    def value_type(self) -> str:
        """
        String representation of the type of the values as they are represented in Python.

        Returns
        -------
        str
            The string representation of the type.

        Raises
        ------
        ValueError
            If no PythonValueFactory has been registered for this logical type.
        """
        import knime.api.types as kt

        return kt.get_value_factory_bundle_for_java_value_factory(
            self.logical_type
        ).python_type

    @property
    def proxy_type(self) -> Type:
        """
        Optional: a proxy type that can be used on the Python side to read or write
        the values which are internally treated like the original logical type.
        """
        if self._proxy_type_converter:
            return self._proxy_type_converter.compatible_type
        else:
            return None

    def __eq__(self, other: object) -> bool:
        return (
            other.__class__ == self.__class__
            and other.logical_type == self.logical_type
            and other.storage_type == self.storage_type
            and other.proxy_type == self.proxy_type
        )

    def __str__(self) -> str:
        try:
            import knime.api.types as kt

            bundle = kt.get_value_factory_bundle_for_java_value_factory(
                self.logical_type
            )
        except ValueError:
            return (
                f"extension<logical={self.logical_type}, storage={self.storage_type}>"
            )
        type_name = bundle.python_type
        proxy_type = self.proxy_type
        if proxy_type:
            return f"extension<proxy={proxy_type}, internal={type_name}>"
        return f"extension<{type_name}>"

    def __hash__(self):
        return hash(str(self))

    def to_pyarrow(self):
        """
        Convert the logical type to PyArrow format.

        This function converts the logical type of a value to PyArrow format. It uses the Knime library for accessing the logical type's value factory bundle and data specification. If the logical type is not recognized or an error occurs while retrieving the data specification, a ValueError is raised.

        Returns
        -------
        Kat.ProxyExtensionType or Kat.LogicalTypeExtensionType
            The converted logical type in PyArrow format.

        Raises
        ------
        ValueError
            If the logical type is unknown or an error occurs while retrieving the data specification.


        Parameters
        ----------
        self : object
            The instance of the class containing the logical type.

        Returns
        -------
        Kat.ProxyExtensionType or Kat.LogicalTypeExtensionType
            The converted logical type.

        """
        import knime.api.types as kt
        import knime._arrow._types as kat

        try:
            # decode the storage type of this value_type from the info provided with the java value factory
            bundle = kt.get_value_factory_bundle_for_java_value_factory(
                self.logical_type
            )
        except (KeyError, AttributeError):
            raise ValueError(
                f"Unknown value_type. Are you missing an extension? Possible value types: {kt.get_python_type_list()}"
            )
        data_spec = bundle.data_spec_json
        pa_storage_type = kat.data_spec_to_arrow(data_spec)

        if self._proxy_type_converter:
            return kat.ProxyExtensionType(
                self._proxy_type_converter, pa_storage_type, self.logical_type
            )
        else:
            return kat.LogicalTypeExtensionType(
                converter=bundle.value_factory,
                storage_type=pa_storage_type,
                java_value_factory=self.logical_type,
            )

    def to_pandas(self):
        """
        Convert the logical type to a Pandas readable datatype.

        """
        return self.to_pyarrow().to_pandas_dtype()

    @staticmethod
    def supported_value_types():
        """
        Returns a string with all possible value types in your current environment and tips and examples how to get them.
        """
        import knime.api.types as kt

        return f"""
            The value types, which are currently available in your environment.
            If you lack some value type, it might not have been installed and imported yet.

            Examples:
            ---------
            # You see in the following list of value types \"<class 'datetime.time'>\"
            import datetime
            value_type = datetime.time

            import datetime as dt
            value_type = dt.time

            List of available value types:
            {kt.get_python_type_list()}
            
            Besides that, the following primitive value types are available:
            knime.api.schema.int32()
            knime.api.schema.int64()
            knime.api.schema.double()
            knime.api.schema.bool_()
            knime.api.schema.string()
            knime.api.schema.blob()
            knime.api.schema.list_(inner_type)
            knime.api.schema.struct(*inner_types)"""


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def int32():
    """
    Create a KNIME integer type with 32 bits.
    """
    return PrimitiveType(PrimitiveTypeId.INT)


def int64():
    """
    Create a KNIME integer type with 64 bits
    """
    return PrimitiveType(PrimitiveTypeId.LONG)


def double():
    """
    Create a KNIME floating point type with double precision (64 bits).
    """
    return PrimitiveType(PrimitiveTypeId.DOUBLE)


def bool_():
    """
    Create a KNIME boolean type.
    """
    return PrimitiveType(PrimitiveTypeId.BOOL)


def null():
    """
    Create a KNIME null (void) type.

    Notes
    -----
    Tables coming from KNIME into a pure-Python node's configure method will never have a "null" column data type, as they are represented in KNIME using the most general data type. However, a table can have a column with type "null" in the execute method of a pure-Python node and in a Python script node, because there the data is available.
    """
    return PrimitiveType(PrimitiveTypeId.NULL)


def string(dict_encoding_key_type: DictEncodingKeyType = None):
    """
    Create a KNIME string type.

    Parameters
    ----------
    dict_encoding_key_type : DictEncodingKeyType, optional
        The key type to use for dictionary encoding. If this is
        None (the default), no dictionary encoding will be used.
        Dictionary encoding helps to reduce storage space and read/write
        performance for columns with repeating values such as categorical data.
    """
    return PrimitiveType(PrimitiveTypeId.STRING, dict_encoding_key_type)


def blob(dict_encoding_key_type: DictEncodingKeyType = None):
    """
    Create a KNIME blob type for binary data of variable length.

    Parameters
    ----------
    dict_encoding_key_type : DictEncodingKeyType, optional
        The key type to use for dictionary encoding. If this is
        None (the default), no dictionary encoding will be used.
        Dictionary encoding helps to reduce storage space and read/write
        performance for columns with repeating values such as categorical data.
    """
    return PrimitiveType(PrimitiveTypeId.BLOB, dict_encoding_key_type)


def list_(inner_type: KnimeType):
    """
    Create a KNIME type that is a list of the given inner types

    Parameters
    ----------
    inner_type : KnimeType
        The type of the elements in the list. Must be a KnimeType.
    """
    return ListType(inner_type)


def struct(*inner_types):
    """
    Create a KNIME structured data type where each given argument represents
    a field of the struct.

    Parameters
    ----------
    inner_types : list
        The argument list of this method defines the fields
        in this structured data type. Each inner type must be a
        KNIME type
    """
    return StructType(inner_types)


def logical(value_type) -> LogicalType:
    """
    Create a KNIME logical data type of the given Python value type.

    Parameters
    ----------
    value_type : type
        The type of the values inside this column. A knime.api.types.PythonValueFactory
        must be registered for this type.

    Raises
    ------
    TypeError
        If no PythonValueFactory has been registered for this value type
        with `knime.api.types.register_python_value_factory`.
    """
    import knime.api.types as kt

    try:
        proxy_type = value_type
        proxy_value_factory, value_type = kt.get_proxy_by_python_type(proxy_type)
    except KeyError:
        # we're not dealing with a proxy type here.
        proxy_value_factory = None

    try:
        bundle = kt.get_value_factory_bundle_for_python_type(value_type)
        specs = bundle.data_spec_json
        traits = bundle.data_traits
        logical_type = _dict_to_knime_type(specs, traits)

        if proxy_value_factory:
            logical_type._proxy_type_converter = proxy_value_factory

        return logical_type
    except Exception as e:
        raise TypeError(
            f"""
            Could not find registered KNIME extension type for Python logical type {value_type}. 
            Call knime.api.schema.LogicalType.supported_value_types() to get a list of supported types and use one of these.
            """,
            e,
        )


def datetime(
    date: Optional[bool] = True,
    time: Optional[bool] = True,
    timezone: Optional[bool] = False,
) -> LogicalType:
    """
    Currently, KNIME supports the following date/time formats:
        - Local DateTime (date=True, time=True, timezone=False)
        - Local Date (date=True, time=False, timezone=False)
        - Local Time (date=False, time=True, timezone=False)
        - Zoned DateTime (date=True, time=True, timezone=True)

    Parameters
    ----------
    date : Optional[bool]
        Whether the column contains a date.
    time : Optional[bool]
        Whether the column contains a time.
    timezone : Optional[bool]
        Whether the column contains a timezone.

    Returns
    -------
    LogicalType
        A LogicalType representing the given date/time format.

    Raises
    ------
    ValueError
        If the combination of date, time and timezone is not supported or the datetime types are not registered in KNIME.
    """
    import knime.api.types as kt

    if not date and not time:
        raise ValueError("Either date or time must be True")
    if timezone and not time:
        raise ValueError("Timezone is only supported if time is True")

    if date and time and not timezone:
        value_factory_string = "LocalDateTimeValueFactory"
    elif date and time and timezone:
        value_factory_string = "ZonedDateTimeValueFactory2"
    elif date and not time:
        value_factory_string = "LocalDateValueFactory"
    else:
        value_factory_string = "LocalTimeValueFactory"

    logical_type_string = _knime_datetime_type(value_factory_string)
    storage_type = _knime_datetime_logical_to_ktype[logical_type_string]

    if logical_type_string not in kt._java_value_factory_to_bundle:
        raise ValueError(
            f"""
            Could not find registered KNIME extension type for datetime format {logical_type_string}. 
            Call knime.api.schema.LogicalType.supported_value_types() to get a list of supported types.
            """
        )

    return LogicalType(logical_type=logical_type_string, storage_type=storage_type)


def _knime_datetime_type(name):
    """
    Return a JSON string representing the factory class for a KNIME datetime type.

    Parameters
    ----------
    name : str
        The name of the datetime type.

    Returns
    -------
    str
        A JSON string representing the factory class for the datetime type.
    """
    return '{"value_factory_class":"org.knime.core.data.v2.time.' + name + '"}'


_knime_datetime_logical_to_ktype = {
    _knime_datetime_type("LocalTimeValueFactory"): int64(),
    _knime_datetime_type("LocalDateValueFactory"): int64(),
    _knime_datetime_type("LocalDateTimeValueFactory"): StructType([int64(), int64()]),
    _knime_datetime_type("DurationValueFactory"): StructType([int64(), int32()]),
    _knime_datetime_type("ZonedDateTimeValueFactory2"): StructType(
        [int64(), int64(), int32(), string()]
    ),
}


class PortObjectSpec(ABC):
    """
    Base protocol for port object specs.

    A `PortObjectSpec` must support conversion from/to a dictionary which is then
    encoded as JSON and sent to/from KNIME.
    """

    @abstractmethod
    def serialize(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data: Dict, java_callback=None):
        """
        Deserialize the port object.

        Parameters
        ----------
        data : Dict
            The data dict created by serialize
        java_callback : any
            A callback handler that allows to make callbacks to Java.
            Was introduced in 5.3 and can be omitted in derived classes.
        Returns
        ----------
        PortObjectSpec
            The deserialized PortObjectSpec
        """
        pass


class BinaryPortObjectSpec(PortObjectSpec):
    """
    Port object spec for simple binary port objects.

    BinaryPortObjectSpecs have an ID that is used to ensure
    that only ports with equal ID can be connected.
    """

    def __init__(self, id: str) -> None:
        """
        Create a BinaryPortObjectSpec.

        Parameters
        ----------
        id : str
            The id of this binary port.
        """
        self._id = id

    @property
    def id(self) -> str:
        return self._id

    def serialize(self) -> dict:
        return {"id": self._id}

    @classmethod
    def deserialize(cls, data):
        # spec is optional therefore we use get instead of __get_item__
        return cls(data["id"])


class ImagePortObjectSpec(PortObjectSpec):
    """
    Port object spec for image port objects.

    ImagePortObjectSpec objects require the format specified via
    `knext.ImageFormat.PNG` or `knext.ImageFormat.SVG`.
    """

    def __init__(self, format: Union[str, Enum]) -> None:
        """
        Create an ImagePortObjectSpec

        Parameters
        ----------
        format : Union[str, Enum]
            The format of the image expected to pass through the port.
        """
        self._format = format if isinstance(format, str) else format.value

    @property
    def format(self) -> str:
        return self._format

    def serialize(self) -> dict:
        return {"format": self._format}

    @classmethod
    def deserialize(cls, data):
        # spec is optional therefore we use get instead of __get_item__
        return cls(data["format"])


class CredentialPortObjectSpec(PortObjectSpec):
    """
    Port object spec for credential port objects.
    """

    def __init__(self, xml_data: Optional[str], java_callback) -> None:
        """
        Create a CredentialPortObjectSpec.

        Parameters
        ----------
        xml_data : Optional[str]
            The xml data of the credentials.
        """
        self._xml_data = xml_data
        self._java_callback = java_callback

    def _get_auth_schema(self) -> str:
        """
        Returns the authentication schema associated with the XML data.

        Returns
        -------
        str
            The authentication schema.

        Notes
        -----
        This method should only be called internally.
        """
        return self._java_callback.get_auth_schema(self._xml_data)

    def _get_auth_parameters(self) -> str:
        """
        Get the authentication parameters.

        Returns
        -------
        str
            The authentication parameters.
        """
        return self._java_callback.get_auth_parameters(self._xml_data)

    def _get_expires_after(self) -> Optional[dt.datetime]:
        """
        Get the optional expiry time of the access token. If the expiry time is not set, None is returned.

        Returns
        -------
        datetime
            The optional expiry time of the access token.
        """
        expiry_date = self._java_callback.get_expires_after(self._xml_data)
        if expiry_date is None:
            return None
        _start_of_epoch = dt.datetime(1970, 1, 1)
        micro_of_day = expiry_date.getNanoSeconds() // 1000  # here we lose precision

        res = _start_of_epoch + dt.timedelta(
            seconds=expiry_date.getEpochSeconds(), microseconds=micro_of_day
        )
        return res

    @property
    def auth_schema(self) -> str:
        """
        Get the auth scheme to use, e.g. "Basic" or "Bearer".

        Parameters
        ----------
        None

        Returns
        -------
        str
            The auth scheme to use, e.g. "Basic" or "Bearer".

        Raises
        ------
        ValueError
            If the credentials are not valid or accessible.

        """
        from py4j.protocol import Py4JJavaError

        try:
            return self._get_auth_schema()
        except Py4JJavaError as ex:
            raise ValueError(
                f"Could not get auth schema from {self.__class__.__name__}. Check if the credentials are valid and "
                f"accessible. Maybe execute the upstream nodes?"
            ) from ex

    @property
    def auth_parameters(self) -> str:
        """
        Get the auth parameters to use, e.g. an access token.

        Returns
        -------
        str
            The parameter(s) to use, e.g. an access token.

        Raises
        ------
        ValueError
            If the credentials are not valid or accessible.

        """
        from py4j.protocol import Py4JJavaError

        try:
            return self._get_auth_parameters()
        except Py4JJavaError as ex:
            raise ValueError(
                f"Could not get auth parameters from {self.__class__.__name__}. Check if the credentials are valid and "
                f"accessible. Maybe execute the upstream nodes?"
            ) from ex

    @property
    def expires_after(self) -> Optional[dt.datetime]:
        """
        Get the optional expiry time of the access token.

        Returns
        -------
        str
            The optional expiry time of the access token.

        Raises
        ------
        ValueError
            If the credentials are not valid or accessible.

        """
        from py4j.protocol import Py4JJavaError

        try:
            return self._get_expires_after()
        except Py4JJavaError as ex:
            raise ValueError(
                f"Could not get expires after from {self.__class__.__name__}. Check if the credentials are valid and "
                f"accessible. Maybe execute the upstream nodes?"
            ) from ex

    def serialize(self) -> dict:
        return {"data": self._xml_data}

    @classmethod
    def deserialize(cls, data: dict, java_callback=None):
        """
        Deserialize the CredentialPortObjectSpec from the data.

        Parameters
        ----------
        data : dict
            Must contain key 'data' which maps to a xml string with the necessary information to get the credentials
            from java.

        Returns
        -------
        CredentialPortObjectSpec
            Containing the xml_data
        """
        # spec is optional therefore we use get instead of __get_item__
        xml_data = data.get("data")
        return cls(xml_data, java_callback)


class HubAuthenticationPortObjectSpec(CredentialPortObjectSpec):
    def __init__(self, xml_data: Optional[str], java_callback, hub_url: str) -> None:
        super().__init__(xml_data, java_callback)
        self._hub_url = hub_url

    @property
    def hub_url(self) -> str:
        return self._hub_url

    def serialize(self) -> dict:
        data = super().serialize()
        data["hub_url"] = self.hub_url
        return data

    @classmethod
    def deserialize(cls, data: dict, java_callback=None):
        xml_data = data.get("data")
        hub_url = data.get("hub_url")
        return cls(xml_data, java_callback, hub_url)


class WorkflowPortInfo:
    def __init__(self, type_name: str, type_id: str, schema: "Schema") -> None:
        self._type_name = type_name
        self._type_id = type_id
        self._schema = schema

    @property
    def type_name(self) -> str:
        return self._type_name

    @property
    def type_id(self) -> str:
        return self._type_id

    @property
    def schema(self) -> "Schema":
        return self._schema

    @classmethod
    def deserialize(cls, data: dict):
        if "table_spec" in data:
            schema = Schema.deserialize(data["table_spec"])
        else:
            schema = None

        return cls(data["type_name"], data["type_id"], schema)


class WorkflowPortObjectSpec(PortObjectSpec):
    def __init__(
        self,
        name: str,
        inputs: Dict[str, WorkflowPortInfo],
        outputs: Dict[str, WorkflowPortInfo],
    ) -> None:
        super().__init__()
        self._name = name
        self._inputs = inputs
        self._outputs = outputs

    @property
    def name(self) -> str:
        return self._name

    @property
    def inputs(self) -> Dict[str, WorkflowPortInfo]:
        return self._inputs

    @property
    def outputs(self) -> Dict[str, WorkflowPortInfo]:
        return self._outputs

    def serialize(self) -> Dict:
        return {"name": self.name, "inputs": self.inputs, "outputs": self.outputs}

    @classmethod
    def deserialize(cls, data: Dict, java_callback=None):
        inputs = {
            key: WorkflowPortInfo.deserialize(value)
            for key, value in data["inputs"].items()
        }
        outputs = {
            key: WorkflowPortInfo.deserialize(value)
            for key, value in data["outputs"].items()
        }
        return cls(data["name"], inputs, outputs)


# --------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------
class Column:
    """
    A column inside a table schema consists of the KNIME datatype, a column name,
    and optional metadata.
    """

    ktype: KnimeType
    name: str
    metadata: Dict

    def __init__(self, ktype: Union[KnimeType, Type], name: str, metadata: dict = None):
        """
        Construct a Column from type, name and optional metadata.

        Parameters
        ----------
        ktype : Union[KnimeType, Type]
            The KNIME type of the column or a type which can be converted via knime.api.schema.logical(ktype) to a KNIME type.
            Raises a TypeError if the type is not a KNIME type or cannot be converted to a KNIME type.
        name : str
            The name of the column. May not be empty. Raises a ValueError if the name is empty.
        metadata : dict, optional
            Metadata of this column.

        Returns
        -------
        Column
            The constructed column.

        Raises
        ------
        TypeError
            If the type is not a KNIME type or cannot be converted to a KNIME type.
        ValueError
            If the name is empty.
        """
        if not isinstance(ktype, KnimeType):
            try:
                ktype = logical(ktype)
            except TypeError as e:
                raise TypeError(
                    f"Could not create column with type {ktype}, please use a supported column type",
                    e,
                )
        if not isinstance(name, str):
            raise TypeError(f"Column name must be of type string, but got {type(name)}")
        if name.strip() == "":
            raise ValueError("Column name may not be empty")

        self.ktype = ktype
        self.name = name
        self.metadata = metadata

    def __str__(self) -> str:
        metastr = "" if self.metadata is None else f", {self.metadata}"
        return f"{self.__class__.__name__}<'{self.name}', {self.ktype}{metastr}>"

    def __eq__(self, other) -> bool:
        return (
            self.ktype == other.ktype
            and self.name == other.name
            and (
                (self.metadata is None and other.metadata is None)
                or (self.metadata == other.metadata)
            )
        )


class _Columnar(ABC):
    """
    Base interface for columnar data structures like Schema and Table,
    providing __getitem__ access as well as insert and append methods
    which return a View into the "real" object.
    """

    @property
    @abstractmethod
    def num_columns(self) -> int:
        """Get the number of columns in the dataset."""
        pass

    @property
    @abstractmethod
    def column_names(self) -> list:
        """Get the names of the columns in a dataset."""
        pass

    def insert(self, other: "_Columnar", at: int) -> "_Columnar":
        """
        Insert a column or another `_Columnar` object (e.g. Table, Schema) into the current `_Columnar` object
        at a specific position.

        Parameters
        ----------
        other : `_Columnar` or `Column`
            The column or `_Columnar` object to be inserted.
        at : int
            The index at which the insertion should occur.

        Returns
        -------
        `_Columnar`
            The `_Columnar` object after the insertion.

        Raises
        ------
        TypeError
            If `other` is not of type `_Columnar` or `Column`.

        Notes
        -----
        The insertion is done in-place, meaning the current `_Columnar` object is modified.

        """
        n = self.num_columns
        permuted_indices = list(range(n))
        new_columns = 1 if isinstance(other, Column) else other.num_columns
        permuted_indices[at:at] = [i + n for i in range(new_columns)]
        return self.append(other)[permuted_indices]

    def remove(self, slicing: Union[str, int, List[str]]):
        """
        Implements remove method for Columnar data structures.
        The input can be a column index, a column name or a list of column names.

        If the input is a column index, the column with that index will be removed.
        If it is a column name, then the first column with matching name is removed.
        Passing a list of column names will filter out all (including duplicate) columns with matching names.

        Parameters
        -----------

        slicing : int | list | str
            Can be of type integer representing the index in column_names to remove.
            Or a list of strings removing every column matching from that list.
            Or a string of which first occurrence is removed from the column_names.

        Returns
        --------
        _ColumnarView: A View missing the columns to be removed.

        Raises
        -------
        ValueError: If no matching column is found given a list or str.
        IndexError: If column is accessed by integer and is out of bounds.
        TypeError: If the key is neither an integer nor a string or list of strings.
        """

        if isinstance(slicing, List):
            if any(slice not in self.column_names for slice in slicing):
                raise ValueError(
                    f"The following values ({' '.join([col for col in self.column_names if col not in slicing])})"
                    + f" did not match any in {type(self)}"
                )
            _columns = [column for column in self.column_names if column not in slicing]

        elif isinstance(slicing, int):
            if slicing >= len(self.column_names) or slicing < 0:
                raise IndexError(
                    f"Index out of bounds. Choose an index between 0,{len(self.column_names)}."
                )
            _columns = self.column_names[:slicing] + self.column_names[slicing + 1 :]

        elif isinstance(slicing, str):
            _columns = self.column_names
            _columns.remove(slicing)

        else:
            raise TypeError(f"Could not match input type {type(slicing)}.")

        return _ColumnarView(delegate=self, operation=_ColumnSlicingOperation(_columns))

    def __getitem__(
        self, slicing: Union[slice, List[int], List[str]]
    ) -> "_ColumnarView":
        """
        Creates a view of this Table or Schema by slicing columns. The slicing syntax is similar to that of numpy arrays,
        but columns can also be addressed as index lists or via a list of column names.

        Parameters
        ----------
        slicing : int, str, slice, list
            A column index, a column name, a slice object, a list of column indices, or a list of column names.
            For single indices, the view will create a "Column" object. For slices or lists of indices,
            a new Schema will be returned.

        Returns
        --------
        _ColumnarView
            A representation of a slice of the original Schema or Table.

        Examples
        --------
        >>> # Get columns 1,2,3,4
        ... sliced_schema = schema[1:5]

        >>> # Get the columns "name" and "age"
        ... sliced_schema = schema[["name", "age"]]
        """
        # we need to return IndexError already here if slicing is an int to end for-loops,
        # otherwise `for i in columnar_obj` will run forever
        if isinstance(slicing, int) and slicing >= self.num_columns:
            raise IndexError(
                f"Index {slicing} is too large for {self.__class__.__name__} with {self.num_columns} columns"
            )

        return _ColumnarView(delegate=self, operation=_ColumnSlicingOperation(slicing))

    def append(
        self, other: Union["_Columnar", Sequence["_Columnar"]]
    ) -> "_ColumnarView":
        """
        Append another `_Columnar` object (e.g. Table, Schema) or a sequence of `_Columnar` objects to the current
        `_Columnar` object.

        Parameters
        ----------
        other : Union["_Columnar", Sequence["_Columnar"]]
            The `_Columnar` object or a sequence of `_Columnar` objects to be appended.

        Returns
        -------
        _ColumnarView
            A `_ColumnarView` object representing the current `_Columnar` object after the append operation.

        """
        return _ColumnarView(delegate=self, operation=_AppendOperation(other))

    @abstractmethod
    def _select_columns(self, selection):
        """
        Implement column slicing here.
        """
        pass

    @abstractmethod
    def _append(self, other: "_Columnar") -> "_Columnar":
        """
        Implement append here
        """
        pass


class _ColumnarView(_Columnar):
    """
    A `_ColumnarView` is created whenever operations such as slicing or appending
    are applied to an object that implements `_Columnar`, which are `Schema` and `Table`.

    Those operations are performed lazily, because especially on tables they can
    involve allocating and copying large amounts of memory and copying.

    If you need the materialized result of the operation, call the `.get()` method.
    """

    def __init__(self, delegate: _Columnar, operation: "_ColumnarOperation"):
        self._delegate = delegate
        self._operation = operation
        self._cache = None  # prevent computing the same "result" multiple times

    @property
    def delegate(self) -> _Columnar:
        return self._delegate

    @property
    def operation(self) -> "_ColumnarOperation":
        return self._operation

    @property
    def num_columns(self):
        return self.get().num_columns

    @property
    def column_names(self):
        return self.get().column_names

    def __str__(self):
        return f"ColumnarView<delegate={self._delegate}, op={self._operation}>"

    def get(self, cache=True) -> _Columnar:
        """
        Dispatches the application of all operations (slicing, appending, etc.) and
        returns a table or schema that is backed by data.

        The result is cached internally to prevent re-evaluating the operation,
        unless this View is wrapped inside another view. Then only the outermost
        view is cached.
        """
        if self._cache is not None:
            return self._cache

        if isinstance(self._delegate, _ColumnarView):
            input = self._delegate.get(cache=False)
        else:
            input = self._delegate
        out = self._operation.apply(input)
        if cache:
            self._cache = out
        return out

    def _select_columns(self, selection):
        raise NotImplementedError(
            "Cannot execute column selection on a view, do that on real data instead!"
        )

    def _append(self, other: "_Columnar") -> "_Columnar":
        raise NotImplementedError(
            "Cannot execute 'append' on a view, do that on real data instead!"
        )

    def __getattr__(self, name):
        # as a last resort, create and call the delegate
        return getattr(self.get(), name)

    def __eq__(self, other):
        if isinstance(other, _ColumnarView):
            other = other.get()

        return self.get() == other


# --------------------------------------------------------------
# Operations
# --------------------------------------------------------------
class _ColumnarOperation(ABC):
    @abstractmethod
    def apply(self, input: _Columnar) -> _Columnar:
        # The input should NOT be a view
        pass


class _ColumnSlicingOperation(_ColumnarOperation):
    def __init__(self, col_slice):
        self._col_slice = col_slice

    def apply(self, input):
        return input._select_columns(self._col_slice)

    def __str__(self):
        return f"ColumnSlicingOp({self._col_slice})"


class _AppendOperation(_ColumnarOperation):
    def __init__(self, other):
        self._other = other

    def apply(self, input):
        if isinstance(self._other, _ColumnarView):
            other = self._other.get()
        else:
            other = self._other

        return input._append(other)

    def __str__(self):
        return f"AppendOp({self._other})"


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------
class Schema(_Columnar, PortObjectSpec):
    """
    A schema defines the data types and names of the columns inside a table.
    Additionally, it can hold metadata for the individual columns.
    """

    @classmethod
    def from_columns(cls, columns: Union[Sequence[Column], Column]):
        """
        Create a schema from a single column or a list of columns.

        Parameters
        ----------
        columns : Union[Sequence[Column], Column]
            A single column or a list of columns.

        Returns
        -------
        Schema
            The constructed schema.
        """

        if isinstance(columns, Column):  # single column is wrapped in a list
            columns = [columns]

        try:
            for col in iter(
                columns
            ):  # check if sequence is iterable and only contains columns
                if not isinstance(col, Column):
                    raise ValueError(
                        f"Can only instantiate a schema from columns, not {type(col)}"
                    )
        except TypeError:
            raise TypeError(f"Columns needs to be an iterable, but is {type(columns)}")
        except ValueError as e:
            raise TypeError(e)  # e is used such that type(col) is preserved

        if len(columns) == 0:
            return cls([], [], [])

        ktypes, names, metadata = zip(*[(c.ktype, c.name, c.metadata) for c in columns])
        return cls(ktypes, names, metadata)

    @classmethod
    def from_types(
        cls,
        ktypes: List[Union[KnimeType, Type]],
        names: List[str],
        metadata: List = None,
    ):
        """
        Create a schema from a list of column data types, names and metadata.

        Parameters
        ----------
        ktypes : List[Union[KnimeType, Type]]
            A list of KNIME types or types known to KNIME.
        names : List[str]
            A list of column names.
        metadata : List, optional

        Returns
        -------
        Schema
            The constructed schema.
        """
        return cls(ktypes, names, metadata)

    def __init__(
        self,
        ktypes: List[Union[KnimeType, Type]],
        names: List[str],
        metadata: List = None,
    ):
        """
        Create a schema from a list of column data types, names and metadata.
        """
        if not isinstance(ktypes, Sequence) or not all(
            isinstance(t, KnimeType) or issubclass(t, KnimeType) for t in ktypes
        ):
            try:
                for t in ktypes:
                    try:
                        logical(t)
                    except:
                        raise TypeError(
                            f"""
                            Schema expected types to be a sequence of KNIME types or types known to KNIME,
                            but got {type(ktypes)}: {ktypes}.
                            Type {t} is not known to KNIME.
                            """
                        )
            except:  # ktypes not iterable
                raise TypeError(
                    f"""
                    Schema expected types to be a sequence of KNIME types or types known to KNIME,
                    but got {type(ktypes)}: {ktypes}.
                    """
                )

        if (not isinstance(names, list) and not isinstance(names, tuple)) or not all(
            isinstance(n, str) for n in names
        ):
            raise TypeError(
                f"Schema expected names to be a sequence of strings, but got {type(names)}"
            )

        if len(ktypes) != len(names):
            raise ValueError(
                f"Number of types must match number of names, but {len(ktypes)} != {len(names)}"
            )

        if metadata is not None:
            if not isinstance(metadata, Sequence):
                # DOESN'T WORK: or not all(m is None or isinstance(m, str) for m in metadata):
                raise TypeError(
                    "Schema expected Metadata to be None or a sequence of strings or Nones"
                )

            if len(ktypes) != len(metadata):
                raise ValueError(
                    f"Number of types must match number of metadata fields, but {len(ktypes)} != {len(metadata)}"
                )
        else:
            metadata = [None] * len(ktypes)

        self._columns = [Column(t, n, m) for t, n, m in zip(ktypes, names, metadata)]

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self._columns]

    @property
    def num_columns(self):
        return len(self._columns)

    def __iter__(self) -> Iterator[Column]:
        yield from self._columns

    def _select_columns(self, index) -> Union[Column, "Schema"]:
        if isinstance(index, int):
            while index < 0:
                index += len(self._columns)
            if index >= len(self._columns):
                raise IndexError(
                    f"Index {index} does not exist in schema with {self.num_columns} columns"
                )
            return self._columns[index]
        elif isinstance(index, str):
            for c in self._columns:
                if c.name == index:
                    return c
            raise IndexError(f"Schema has no column named '{index}'")
        elif isinstance(index, slice):
            return self.__class__.from_columns(self._columns[index])
        elif isinstance(index, list):
            columns = []
            for col in index:
                if isinstance(col, str):
                    try:
                        columns.append(self.column_names.index(col))
                    except ValueError:
                        raise IndexError(
                            f"Invalid column selection, '{col}' is not available in {self}"
                        )
                elif isinstance(col, int):
                    if not 0 <= col < self.num_columns:
                        raise IndexError(f"Column index {col} out of bounds")
                    columns.append(col)
                else:
                    raise IndexError(f"Invalid column index {col}")
            return self.__class__.from_columns([self._columns[c] for c in columns])
        else:
            raise TypeError(
                f"{self.__class__.__name__} can only be indexed by int, string, slice, list of int, "
                f"or list of string, not {type(index)}"
            )

    def __eq__(self, other) -> bool:
        if not other.__class__ == self.__class__:
            return False

        if len(self._columns) != len(other._columns):
            return False

        return all(a == b for a, b in zip(self._columns, other._columns))

    def _append(self, other: Union["Schema", Column, Sequence["Column"]]) -> "Schema":
        """Create a new schema by adding another schema, a sequence of columns or a column to the end"""
        cols = self._columns.copy()
        if isinstance(other, _ColumnarView):
            other = other.get()

        if isinstance(other, self.__class__):
            cols.extend(other._columns)
        elif isinstance(other, Column):
            cols.append(other)
        elif isinstance(other, Sequence):
            for col in other:  # check if list only contains columns
                if not isinstance(col, Column):
                    raise TypeError(
                        f"A column list to append can only contain columns, not {type(col)}"
                    )
            schema = self.__class__.from_columns(
                other
            )  # create another schema to extend
            cols.extend(schema)
        else:
            raise TypeError(
                f"Can only append columns, column lists or schemas to this schema, not {type(other)}"
            )
        return self.__class__.from_columns(cols)

    def __str__(self) -> str:
        sep = ",\n\t"
        return (
            f"{self.__class__.__name__}<\n\t{sep.join(str(c) for c in self._columns)}>"
        )

    def __repr__(self) -> str:
        return str(self)

    def serialize(self) -> Dict:
        """
        Convert this Schema into dict which can then be JSON encoded and sent to KNIME
        as result of a node's configure() method.
        Because KNIME expects a row key column as first column of the schema, but we don't
        include this in the KNIME Python table schema, we insert a row key column here.

        Raises
        ------
            RuntimeError: if duplicate column names are detected

        """
        col_names = self.column_names
        if len(col_names) != len(set(col_names)):
            raise RuntimeError(
                "Duplicate column names detected, please make sure all columns have unique names"
            )

        row_key_type = LogicalType(_row_key_type, string())
        schema_with_row_key = _wrap_primitive_types(self).insert(
            Column(row_key_type, "RowKey"), at=0
        )

        return _schema_to_knime_dict(schema_with_row_key.get())

    @classmethod
    def deserialize(cls, table_schema: dict) -> "Schema":
        """
        Construct a Schema from a dict that was retrieved from KNIME in JSON encoded form
        as the input to a node's configure() method.
        KNIME provides table information with a RowKey column at the beginning, which we drop before
        returning the created schema.

        """
        specs = table_schema["schema"]["specs"]
        traits = table_schema["schema"]["traits"]
        names = table_schema["columnNames"]
        metadata = table_schema["columnMetaData"]

        ktypes = [_dict_to_knime_type(s, t) for s, t in zip(specs, traits)]
        row_key_type = LogicalType(_row_key_type, string())
        if ktypes[0] == row_key_type:
            schema_without_row_key = cls(ktypes[1:], names[1:], metadata[1:])
        else:
            LOGGER.warning(
                "Did not find RowKey column when creating Schema from KNIME dict"
            )
            schema_without_row_key = cls(ktypes, names, metadata)
        return _unwrap_primitive_types(schema_without_row_key)


# ---------------------------------------------------------------------------------
# Logical Type handling


def _knime_logical_type(name):
    return '{"value_factory_class":"org.knime.core.data.v2.value.' + name + '"}'


_row_key_type = _knime_logical_type("DefaultRowKeyValueFactory")

_knime_type_to_logical_type = {
    int32(): _knime_logical_type("IntValueFactory"),
    int64(): _knime_logical_type("LongValueFactory"),
    string(): _knime_logical_type("StringValueFactory"),
    bool_(): _knime_logical_type("BooleanValueFactory"),
    double(): _knime_logical_type("DoubleValueFactory"),
    null(): _knime_logical_type("VoidValueFactory"),
    list_(int32()): _knime_logical_type("IntListValueFactory"),
    list_(int64()): _knime_logical_type("LongListValueFactory"),
    list_(string()): _knime_logical_type("StringListValueFactory"),
    list_(bool_()): _knime_logical_type("BooleanListValueFactory"),
    list_(double()): _knime_logical_type("DoubleListValueFactory"),
}
_logical_type_to_knime_type = dict(
    (l, k) for k, l in _knime_type_to_logical_type.items()
)
_logical_list_type = _knime_logical_type("ListValueFactory")


def _unwrap_primitive_type(dtype: KnimeType) -> KnimeType:
    """
    Removes all known logical types to simplify the type.
    If the type is unknown or does not contain a logical type, it
    will be returned unmodified.
    """
    # extension types don't need unwrapping, we use the PythonValueFactory in our LogicalType if available
    if (
        isinstance(dtype, LogicalType)
        and dtype.logical_type in _logical_type_to_knime_type
    ):
        dtype = _logical_type_to_knime_type[dtype.logical_type]
    elif (
        isinstance(dtype, LogicalType)
        and dtype.logical_type == _logical_list_type
        and isinstance(dtype.storage_type, ListType)
    ):
        dtype = list_(_unwrap_primitive_type(dtype.storage_type.inner_type))
    return dtype


def _wrap_primitive_type(dtype: KnimeType) -> KnimeType:
    """
    Wraps all primitive types in their according KNIME logical type.
    If the type is unknown, it will be returned unmodified.
    """
    # no need to wrap extension types -> happens in logical(value_type)

    if dtype in _knime_type_to_logical_type:
        dtype = LogicalType(_knime_type_to_logical_type[dtype], dtype)
    elif isinstance(dtype, ListType):
        wrapped_inner = _wrap_primitive_type(dtype.inner_type)
        dtype = LogicalType(_logical_list_type, list_(wrapped_inner))
    return dtype


def _unwrap_primitive_types(schema: Schema) -> Schema:
    """
    A table schema as it is coming from KNIME contains all columns as "logical types",
    because they have a logical type trait (and java_value_factory) attached to it.
    Here we unwrap all logical types that are known to us and present them as
    primitive types to our users.
    """
    unwrapped_columns = []
    for c in schema:
        ktype = _unwrap_primitive_type(c.ktype)
        unwrapped_columns.append(Column(ktype, c.name, c.metadata))
    return schema.__class__.from_columns(unwrapped_columns)


def _wrap_primitive_types(schema: Schema) -> Schema:
    """
    Given a schema with primitive types, we wrap all columns - with types that
    we understand - in logical types to attach a "java_value_factory"
    to them. This is needed to be able to read the Schema on the KNIME side.
    """
    wrapped_columns = []
    for c in schema:
        ktype = _wrap_primitive_type(c.ktype)
        wrapped_columns.append(Column(ktype, c.name, c.metadata))
    return schema.__class__.from_columns(wrapped_columns)


# ---------------------------------------------------------------------------------
# Serialization helpers


def _create_knime_type_from_id(type_id: str) -> KnimeType:
    """
    Create a KNIME type object based on a given type ID.
    """
    if type_id == "string":
        return string()
    elif type_id == "int":
        return int32()
    elif type_id == "long":
        return int64()
    elif type_id == "double":
        return double()
    elif type_id == "boolean":
        return bool_()
    elif type_id == "variable_width_binary":
        return blob()
    elif type_id == "void":
        return null()


def _dict_to_knime_type(spec, traits) -> KnimeType:
    """
    Convert a dictionary specification and traits into a KnimeType.
    """
    if traits["type"] == "simple":
        if "traits" in traits and "dict_encoding" in traits["traits"]:
            key = DictEncodingKeyType(traits["traits"]["dict_encoding"])
            if spec == "string":
                return string(key)
            elif spec == "variable_width_binary":
                return blob(key)
        storage_type = _create_knime_type_from_id(spec)
    elif traits["type"] == "list":
        assert spec["type"] == "list"
        inner_spec = spec["inner_type"]
        inner_traits = traits["inner"]
        storage_type = list_(_dict_to_knime_type(inner_spec, inner_traits))
    elif traits["type"] == "struct":
        assert spec["type"] == "struct"
        inner_specs = spec["inner_types"]
        inner_traits = traits["inner"]
        storage_type = struct(
            *(_dict_to_knime_type(s, t) for s, t in zip(inner_specs, inner_traits))
        )

    if "traits" in traits and "logical_type" in traits["traits"]:
        logical_type = traits["traits"]["logical_type"]
        return LogicalType(logical_type, storage_type)

    return storage_type


_knime_to_type_str = {
    int32(): "int",
    int64(): "long",
    string(): "string",
    bool_(): "boolean",
    double(): "double",
    blob(): "variable_width_binary",
    null(): "void",
}


def _knime_type_to_dict(ktype) -> tuple:
    """
    Converts a KNIME type to a dictionary representation.
    """
    traits = {}

    if isinstance(ktype, LogicalType):
        traits["traits"] = {"logical_type": ktype.logical_type}
        ktype = ktype.storage_type
    else:
        traits["traits"] = {}

    if isinstance(ktype, ListType):
        inner_spec, inner_traits = _knime_type_to_dict(ktype.inner_type)
        traits["type"] = "list"
        traits["inner"] = inner_traits
        spec = {"type": "list", "inner_type": inner_spec}
    elif isinstance(ktype, StructType):
        inner_specs, inner_traits = zip(
            *[_knime_type_to_dict(i) for i in ktype.inner_types]
        )
        traits["type"] = "struct"
        traits["inner"] = list(inner_traits)
        spec = {"type": "struct", "inner_types": list(inner_specs)}
    else:
        traits["type"] = "simple"
        if (
            isinstance(ktype, PrimitiveType)
            and (
                ktype._type_id == PrimitiveTypeId.STRING
                or ktype._type_id == PrimitiveTypeId.BLOB
            )
            and ktype.dict_encoding_key_type is not None
        ):
            traits["traits"]["dict_encoding"] = ktype.dict_encoding_key_type.value
            ktype = (
                ktype.plain_type
            )  # to look up the data type without key in the _knime_to_type_str dict

        try:
            spec = _knime_to_type_str[ktype]
        except KeyError:
            raise KeyError(f"Could not find spec for type: {ktype}")

    return spec, traits


def _schema_to_knime_dict(schema):
    """
    Converts a schema to a KNIME dictionary format.

    """
    specs, traits = zip(*[_knime_type_to_dict(c.ktype) for c in schema])
    return {
        "schema": {"specs": specs, "traits": traits},
        "columnNames": [c.name for c in schema],
        "columnMetaData": [c.metadata for c in schema],
    }
