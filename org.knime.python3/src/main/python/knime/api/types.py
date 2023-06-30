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
Defines the Python equivalent to a ValueFactory and related utility method/classes.

@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""
import warnings
from abc import ABC, abstractmethod
from contextlib import contextmanager
import importlib
import json
from typing import List, Tuple, Type, Callable
import logging

LOGGER = logging.getLogger(__name__)


class PythonValueFactory:
    def __init__(self, compatible_type):
        """
        Create a PythonValueFactory that can perform special encoding/
        decoding for the values represented by this ValueFactory.

        Args:
            compatible_type:
                The class of the value, for which this factory is created.
        """
        self._compatible_type = compatible_type

    @property
    def compatible_type(self):
        return self._compatible_type

    def decode(self, storage):
        return storage

    def encode(self, value):
        return value

    def needs_conversion(self):
        return True


class FileStoreHandler:
    """
    Interface for a file store handler that must be provided from the Java side
    """

    def file_store_key_to_absolute_path(file_store_key: str) -> str:
        """
        Returns the absolute path of the file addressed by the file_store_key
        """
        pass

    def create_file_store() -> Tuple[str, str]:
        """
        Returns a tuple (absolute_file_name, file_store_key)
        """
        pass


class FileStorePythonValueFactory(PythonValueFactory):
    """
    A PythonValueFactory that stores big data in separate files

    A FileStorePythonValueFactory reads from files (so called file stores) while
    decoding values and writes to files when encoding values. This is useful for values
    that can become large because storing them inside the table is inefficient.

    Subclasses should implement read() and write().

    Args:
        compatible_type:
            The class of the value, for which this factory is created.
    """

    def __init__(self, compatible_type):
        PythonValueFactory.__init__(self, compatible_type)

    @abstractmethod
    def read(self, file_paths: List, table_data):
        """
        Reads the value at the given file paths locations.

        Args:
            file_paths: list of file paths for the file stores of this value
            table_data: additional data that was stored in the table

        Returns:
            the read value
        """
        pass

    @abstractmethod
    def write(self, file_store_creator: Callable[[], str], value):
        """
        Writes the given value to file stores.

        Implementations should call the ``file_store_creator`` without arguments to
        create new file stores in which the value can be stored. The method can return
        additional data that should be stored in the table.

        Args:
            file_store_creator: callable that can be called without arguments and
                returns a file path that the caller should write to
            value: value that should be written

        Returns:
            object with additional data
        """
        pass

    def decode(self, storage):
        if storage is None:
            return None

        from knime.api.table import _backend

        file_store_keys = storage["0"]
        if file_store_keys is None or len(file_store_keys) == 0:
            file_paths = []
        else:
            file_store_handler = _backend.file_store_handler
            file_paths = [
                file_store_handler.file_store_key_to_absolute_path(key)
                for key in file_store_keys.split(";")
            ]

        # NB: additional data is stored at location "1" in the struct
        return self.read(file_paths, storage["1"])

    def encode(self, value):
        if value is None:
            return None

        from knime.api.table import _backend

        file_store_handler = _backend.file_store_handler

        file_store_keys = []

        def file_store_creator():
            path, key = file_store_handler.create_file_store()
            file_store_keys.append(key)
            return path

        tableData = self.write(file_store_creator, value)

        return {
            "0": ";".join(file_store_keys) if len(file_store_keys) > 0 else None,
            "1": tableData,
        }


class TableOrFileStorePythonValueFactory(FileStorePythonValueFactory):
    """
    A FileStorePythonValueFactory that stores the same data in the table or in a file

    When encoding, subclasses can decide if a value should be stored in the table or in
    a file by implementing ``should_be_stored_in_filestore()``. The subclass only has
    to implement on kind of serialization in ``serialize()`` and ``deserialize()``.
    These methods are used for both cases.

    Args:
        compatible_type:
            The class of the value, for which this factory is created.
    """

    def __init__(self, compatible_type):
        FileStorePythonValueFactory.__init__(self, compatible_type)

    @abstractmethod
    def deserialize(self, input: "io.BytesIO"):
        """
        Deserialize a value from the given BytesIO which can be in memory bytes or
        stored on disk.

        Return:
            The deserialized value
        """
        pass

    @abstractmethod
    def serialize(self, value, output: "io.BytesIO"):
        """
        Serialize the value into the given output BytesIO stream. Must be readable
        in the same way by the corresponding ``deserialize()`` method.

        Do NOT close the output, this will be done externally.
        """
        pass

    def should_be_stored_in_filestore(self, value):
        return True

    def write(self, file_store_creator, value):
        if self.should_be_stored_in_filestore(value):
            # Store in file store
            file_name = file_store_creator()
            with open(file_name, "wb") as f:
                self.serialize(value, f)

            # TODO: implement hashcode method which is equal to java side
            return None
        else:
            # Store in table
            import io

            with io.BytesIO() as bytes_io:
                self.serialize(value, bytes_io)
                return bytes_io.getvalue()

    def read(self, file_paths, table_data):
        if len(file_paths) > 0:
            # Data is stored in file store
            bytes_io = open(file_paths[0], "rb")
        else:
            # Data is stored in table
            import io

            bytes_io = io.BytesIO(table_data)

        value = self.deserialize(bytes_io)
        bytes_io.close()
        return value


class FallbackPythonValueFactory(PythonValueFactory):
    def __init__(self):
        super().__init__(None)

    def needs_conversion(self):
        return False


def encode_inner_elements(value):
    """
    Encodes the elements of a list or set by using the encode method of the converter of the first notnull element.
    """
    # get first notnull element
    elem = next((item for item in value if item is not None), None)
    if elem is None:
        return value
    try:
        # get the correct converter
        inner_type = type(elem)
        bundle = get_value_factory_bundle_for_python_type(inner_type)
        inner_converter = get_converter(bundle.logical_type)
        return [inner_converter.encode(v) for v in value]
    except TypeError:
        # if we do not have a converter for the type we just return without conversion
        return value


class SetValueFactory(PythonValueFactory):
    def __init__(self):
        PythonValueFactory.__init__(self, set)

    def encode(self, value):
        if value is None:
            return None
        return encode_inner_elements(value)


class ListValueFactory(PythonValueFactory):
    def __init__(self):
        PythonValueFactory.__init__(self, list)

    def encode(self, value):
        if value is None:
            return None
        return encode_inner_elements(value)


class PythonValueFactoryBundle:
    def __init__(
        self,
        java_value_factory: str,
        data_spec_json: str,
        data_traits: str,
        python_module: str,
        python_value_factory_name: str,
        python_value_type_name: str,
    ):
        self._java_value_factory = java_value_factory
        self._data_spec_json = json.loads(data_spec_json)
        self._value_factory = None
        self._data_traits = json.loads(data_traits)
        self._python_module = python_module
        self._python_value_factory_name = python_value_factory_name
        self._python_value_type_name = python_value_type_name

    @property
    def java_value_factory(self) -> str:
        """Also called 'logical type'."""
        return self._java_value_factory

    @property
    def logical_type(self) -> str:
        """Also called 'java value factory'."""
        return self.java_value_factory

    @property
    def data_spec_json(self) -> dict:
        return self._data_spec_json

    @property
    def value_factory(self) -> PythonValueFactory:
        if self._value_factory == None:
            value_factory = _get_converter_or_value_factory(
                _get_module(self._python_module), self._python_value_factory_name
            )
            self._value_factory = value_factory
        return self._value_factory

    @property
    def data_traits(self) -> dict:
        return self._data_traits

    @property
    def python_type(self):
        """String representation of the python type."""
        return self._python_value_type_name


def _get_module(module_name):
    try:
        return importlib.import_module(module_name)
    except AttributeError:
        # If instead of a string we got a module
        return importlib.import_module(module_name.__name__)
    except ImportError:
        msg = f"The module {module_name} does not exist. Do you have the necessary extensions installed?"
        raise ValueError(msg)


def get_python_type_from_name(name):
    """Returns the python type for the given name.
    Args:
        name: String in the format module.qualname

    Returns: python type

    """
    module_name, type_name = name.rsplit(".", 1)
    module = _get_module(module_name)
    return getattr(module, type_name)


def _get_converter_or_value_factory(module, class_name):
    try:
        clazz = getattr(module, class_name)
    except AttributeError:
        raise ValueError(
            f"The module {module.__name__} does not have a value factory or converter called '{class_name}'."
        )
    if (
        not issubclass(clazz, PythonValueFactory)
        and not issubclass(clazz, ToPandasColumnConverter)
        and not issubclass(clazz, FromPandasColumnConverter)
    ):
        raise ValueError(
            f"{class_name} in {module.__name__} is not compatible, must be of type "
            + "knime.api.types.PythonValueFactory or knime.api.types.ToPandasColumnConverter or"
            + " knime.api.types.ToPandasColumnConverter"
        )
    return clazz()


_java_value_factory_to_bundle = {}

_python_type_to_bundle = {}

# Proxy types are alternative Python logical types that map to the same
# Java ValueFactory as the "original" logical type by being converted
# to the same storage type internally.
_python_proxy_type_to_factory_info = {}


def get_proxy_by_python_type(
    python_type: Type,
) -> Tuple[PythonValueFactory, Type]:
    if not isinstance(python_type, type):
        raise TypeError(f" The Python type '{python_type}' is not a type.")
    complete_type = str(python_type.__module__) + "." + str(python_type.__qualname__)
    try:
        (
            python_module,
            python_value_factory_name,
            orig_python_type_name,
        ) = _python_proxy_type_to_factory_info[complete_type]
    except AttributeError:
        raise TypeError(
            f"The Python type '{python_type}' is not registered. A list of valid types can be obtained via get_python_type_list()."
        )
    orig_bundle = get_value_factory_bundle_for_python_type_name(orig_python_type_name)
    orig_value_factory = orig_bundle.value_factory
    proxy_value_factory = _get_converter_or_value_factory(
        _get_module(python_module), python_value_factory_name
    )
    return (
        proxy_value_factory,
        orig_value_factory.compatible_type,
    )


def get_python_type_list():
    return _python_type_to_bundle.keys()


def register_python_value_factory(
    python_module,
    python_value_factory_name,
    data_spec_json,
    data_traits,
    python_value_type_name,
    is_default_python_representation=True,
):
    """
    Creates a bundle containing python value factory (e.g. SmilesValueFactory),
    java value factory (a.k.a. logical type), python type name  (e.g. 'knime.types.chemistry.SmilesValue'),
    as well as data_traits and data_spec_json.

    Args:
        python_module: The module containing the factory
        python_value_factory_name: The factory to be registered
        data_spec_json: A dict used to create a PythonValueFactoryBundle
        data_traits: A dict used to get the logical_type,
            e.g. '{"value_factory_class":"org.knime.chem.types.SmilesCellValueFactory"}'
            (holds information on where to find the Java pendant to the
            Python value factory)
        python_value_type_name: The name of the value type, which is handled by the
            factory; the name is taken from the plugin.xml of the module containing
            the factory
        is_default_python_representation: only false, if an alternative representation
            is provided; note that we assume that a default (i.e. normal) Python
            representation is already given
    """
    unpacked_data_traits = json.loads(data_traits)["traits"]
    logical_type = unpacked_data_traits["logical_type"]

    if is_default_python_representation:
        value_factory_bundle = PythonValueFactoryBundle(
            logical_type,
            data_spec_json,
            data_traits,
            python_module,
            python_value_factory_name,
            python_value_type_name,
        )
        _java_value_factory_to_bundle[logical_type] = value_factory_bundle
        _python_type_to_bundle[python_value_type_name] = value_factory_bundle

    else:
        proxy_python_value_type_name = python_value_type_name
        orig_bundle = get_value_factory_bundle_for_java_value_factory(logical_type)
        orig_python_type_name = orig_bundle.python_type

        _python_proxy_type_to_factory_info[proxy_python_value_type_name] = (
            python_module,
            python_value_factory_name,
            orig_python_type_name,
        )


_fallback_value_factory = FallbackPythonValueFactory()


def get_converter(logical_type):
    try:
        return get_value_factory_bundle_for_java_value_factory(
            logical_type
        ).value_factory
    except ValueError:
        if logical_type in list_like_value_factories.keys():
            return list_like_value_factories[logical_type]
        LOGGER.debug(
            f"The fallback value factory is used for the following type: {logical_type}"
        )
        return _fallback_value_factory


def _knime_primitive_type(name):
    return '{"value_factory_class":"org.knime.core.data.v2.value.' + name + '"}'


list_like_value_factories = {
    _knime_primitive_type("ListValueFactory"): ListValueFactory(),
    _knime_primitive_type("SetValueFactory"): SetValueFactory(),
}


def get_value_factory_bundle_for_java_value_factory(
    logical_type: str,
) -> PythonValueFactoryBundle:
    if logical_type in _java_value_factory_to_bundle:
        return _java_value_factory_to_bundle[logical_type]
    raise ValueError(
        f"The logical type {logical_type} is not compatible with any registered Python value factory."
    )


def get_value_factory_bundle_for_python_type_name(
    python_type_name: str,
) -> PythonValueFactoryBundle:
    if python_type_name in _python_type_to_bundle:
        return _python_type_to_bundle[python_type_name]
    raise ValueError(
        f"The python type name {python_type_name} is not compatible with any registered Python value factory."
    )


def get_value_factory_bundle_for_python_type(
    python_type: type,
) -> PythonValueFactoryBundle:
    complete_type = str(python_type.__module__) + "." + str(python_type.__qualname__)
    if complete_type in _python_type_to_bundle:
        return _python_type_to_bundle[complete_type]
    raise TypeError(
        f"The python type {python_type} is not compatible with any registered Python value factory."
    )


# ---------------------------------------------------------------
# Pandas Column Converters
#
# TODO: where should we put those? We don't want to expose that we use Arrow and
#       how we convert to Pandas, but for some extension types (GeoSpatial) we need
#       to inject some specialties...
# ---------------------------------------------------------------
class FromPandasColumnConverter(ABC):
    """
    Convert a column inside a Pandas DataFrame before it gets converted
    into a pyarrow Table or RecordBatch with pyarrows "from_pandas" method.

    Note: additional module imports should only occur in the convert_column method,
          as each module import leads to slower startup time of the Python process.
    """

    # to suppress specific warnings while converting the data, overwrite this list in the converter's implementation
    warnings_to_suppress: List[str] = None

    @abstractmethod
    def can_convert(self, dtype) -> bool:
        return False

    @abstractmethod
    def convert_column(
        self, data_frame: "pandas.dataframe", column_name: str
    ) -> "pandas.Series":
        pass

    @contextmanager
    def warning_manager(self):
        """
        The contextlib.contextmanager decorates a function which yields exactly once.
        Everything before yield is considered to be __enter__ section and everything after,
        to be __exit__ section.
        To suppress specific warnings while converting the data, overwrite the suppress_warnings list
        in the converter's implementation

        """
        if self.warnings_to_suppress is not None:
            for warning in self.warnings_to_suppress:
                warnings.filterwarnings("ignore", message=warning)
        yield
        if self.warnings_to_suppress is not None:
            for warning in self.warnings_to_suppress:
                warnings.filterwarnings("default", message=warning)


class ToPandasColumnConverter(ABC):
    """
    Convert a column inside a Pandas DataFrame right after it was converted from
    a pyarrow Table or RecordBatch, before it gets returned from knime.scripting._deprecated._table's to_pandas().

    Note: additional module imports should only occur in the convert_column method,
          as each module import leads to slower startup time of the Python process.
    """

    # to suppress specific warnings while converting the data, overwrite this list in the converter's implementation
    warnings_to_suppress: List[str] = None

    @abstractmethod
    def can_convert(self, dtype) -> bool:
        return False

    @abstractmethod
    def convert_column(
        self, data_frame: "pandas.dataframe", column_name: str
    ) -> "pandas.Series":
        pass

    @contextmanager
    def warning_manager(self):
        """
        The contextlib.contextmanager decorates a function which yields exactly once.
        Everything before yield is considered to be __enter__ section and everything after,
        to be __exit__ section.
        To suppress specific warnings while converting the data, overwrite the suppress_warnings list
        in the converter's implementation

        """
        if self.warnings_to_suppress is not None:
            for warning in self.warnings_to_suppress:
                warnings.filterwarnings("ignore", message=warning)
        yield
        if self.warnings_to_suppress is not None:
            for warning in self.warnings_to_suppress:
                warnings.filterwarnings("default", message=warning)


_from_pandas_column_converters = {}
_to_pandas_column_converters = {}


def get_first_matching_from_pandas_col_converter(dtype):
    # find matching column converter and load if needed
    try:
        type_str = json.loads(dtype._logical_type)["value_factory_class"]
    except AttributeError:  # dtype is not a kap.PandasLogicalTypeExtensionType
        type_str = type(dtype).__module__ + "." + type(dtype).__qualname__
    try:
        converter = _from_pandas_column_converters[type_str]
    except KeyError:
        return None

    mod = _get_module(converter[0])
    return _get_converter_or_value_factory(mod, converter[1])


def get_first_matching_to_pandas_col_converter(dtype):
    # find matching column converter and load if needed
    import knime._arrow._types as kat

    if not isinstance(dtype, kat.LogicalTypeExtensionType):
        return None

    value_factory_class_name = json.loads(dtype.logical_type)["value_factory_class"]

    try:
        converter = _to_pandas_column_converters[value_factory_class_name]
    except KeyError:
        return None

    mod = _get_module(converter[0])
    return _get_converter_or_value_factory(mod, converter[1])
