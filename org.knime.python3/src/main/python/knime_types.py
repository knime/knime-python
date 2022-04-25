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

from abc import ABC, abstractmethod
import importlib
import json
import knime_schema as ks

class PythonValueFactory:
    def __init__(self, compatible_type):
        """
        Create a PythonValueFactory that can perform special encoding/
        decoding for the values represented by this ValueFactory.

        Args: 
            compatible_type:
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

    def can_convert(self, value):
        return type(value) == self._compatible_type


class FallbackPythonValueFactory(PythonValueFactory):
    def __init__(self):
        super().__init__(None)

    def needs_conversion(self):
        return False


class PythonValueFactoryBundle:
    def __init__(self, java_value_factory, data_spec_json, value_factory, data_traits):
        self._java_value_factory = java_value_factory
        self._data_spec_json = json.loads(data_spec_json)
        self._value_factory = value_factory
        self._data_traits = json.loads(data_traits)

    @property
    def java_value_factory(self):
        return self._java_value_factory

    @property
    def data_spec_json(self):
        return self._data_spec_json

    @property
    def value_factory(self):
        return self._value_factory

    @property
    def data_traits(self):
        return self._data_traits


_java_value_factory_to_bundle = {}

_bundles = []

_python_type_to_java_value_factory = {}
_java_value_factory_to_python_type = {}

def register_python_value_factory(
    python_module, python_value_factory_name, data_spec_json, data_traits,
):
    module = importlib.import_module(python_module)
    value_factory_class = getattr(module, python_value_factory_name)
    value_factory = value_factory_class()
    unpacked_data_traits = json.loads(data_traits)["traits"]
    logical_type = unpacked_data_traits["logical_type"]
    value_factory_bundle = PythonValueFactoryBundle(
        logical_type, data_spec_json, value_factory, data_traits
    )
    _java_value_factory_to_bundle[logical_type] = value_factory_bundle
    _bundles.append(value_factory_bundle)
    python_type = value_factory.compatible_type
    
    _java_value_factory_to_python_type[logical_type] = python_type
    _python_type_to_java_value_factory[python_type] = logical_type


_fallback_value_factory = FallbackPythonValueFactory()


def get_converter(logical_type):
    try:
        return _java_value_factory_to_bundle[logical_type].value_factory
    except KeyError:
        return _fallback_value_factory


def get_value_factory_bundle_for_type(value):
    for bundle in _bundles:
        if bundle.value_factory.can_convert(value):
            return bundle

    raise ValueError(
        f"The value {value} is not compatible with any registered PythonValueFactory."
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

    @abstractmethod
    def can_convert(self, dtype) -> bool:
        return False

    @abstractmethod
    def convert_column(self, column: "pandas.Series") -> "pandas.Series":
        pass


class ToPandasColumnConverter(ABC):
    """
    Convert a column inside a Pandas DataFrame right after it was converted from
    a pyarrow Table or RecordBatch, before it gets returned from knime_table's to_pandas().

    Note: additional module imports should only occur in the convert_column method,
          as each module import leads to slower startup time of the Python process.
    """

    @abstractmethod
    def can_convert(self, dtype) -> bool:
        return False

    @abstractmethod
    def convert_column(self, column: "pandas.Series") -> "pandas.Series":
        pass


_from_pandas_column_converters = []
_to_pandas_column_converters = []


def register_from_pandas_column_converter(converter_class):
    """
    Use this to decorate a class that can be used as column converter.

    Example::
        @knime_types.register_from_pandas_column_converter
        class MyColumnConverter(knime_types.FromPandasColumnConverter):
            ...
    """
    assert issubclass(converter_class, FromPandasColumnConverter)
    _from_pandas_column_converters.append(converter_class())
    return converter_class


def register_to_pandas_column_converter(converter_class):
    """
    Use this to decorate a class that can be used as column converter.

    Example::
        @knime_types.register_to_pandas_column_converter
        class MyColumnConverter(knime_types.ToPandasColumnConverter):
            ...
    """
    assert issubclass(converter_class, ToPandasColumnConverter)
    _to_pandas_column_converters.append(converter_class())
    return converter_class


def get_first_matching_from_pandas_col_converter(dtype):
    for c in _from_pandas_column_converters:
        if c.can_convert(dtype):
            return c
    return None


def get_first_matching_to_pandas_col_converter(dtype):
    for c in _to_pandas_column_converters:
        if c.can_convert(dtype):
            return c
    return None
