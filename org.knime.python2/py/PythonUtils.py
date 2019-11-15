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
@author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
@author Patrick Winter, KNIME GmbH, Konstanz, Germany
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

# This should be the first statement in each module that makes specific demands on the Python environment.
import EnvironmentHelper

if EnvironmentHelper.is_python3():
    import importlib
else:
    import imp

if EnvironmentHelper.is_tslib_available():
    from EnvironmentHelper import Timestamp
    from EnvironmentHelper import NaT
else:
    Timestamp = None
    NaT = None

import base64
import inspect
import math
import numpy
import os

from datetime import datetime

# list of equivalent types
EQUIVALENT_TYPES = []

# fill EQUIVALENT_TYPES
if not EnvironmentHelper.is_python3():
    EQUIVALENT_TYPES.append([unicode, str])  # Available in Python 2.
    EQUIVALENT_TYPES.append([int, long])  # Available in Python 2.

if EnvironmentHelper.is_tslib_available():
    EQUIVALENT_TYPES.append([datetime, Timestamp])

# Do not change those to sets. We need __eq__ comparison (overridden by numpy.dtypes), not __hash__.
# See https://docs.scipy.org/doc/numpy-1.14.2/reference/arrays.scalars.html.
_BOOLEAN_TYPES = ('bool', 'bool_', 'bool8')

_DOUBLE_TYPES = ('float', 'float_', 'double',
                 'float64')

_FLOAT_TYPES = ('half', 'single', 'float16', 'float32')

_INTEGER_TYPES = ('byte', 'short', 'int',
                  'intc', 'int_', 'longlong',
                  'intp', 'int8', 'int16',
                  'int32', 'int64', 'ubyte',
                  'ushort', 'uintc', 'uint',
                  'ulonglong', 'uintp', 'uint8',
                  'uint16', 'uint32', 'uint64')


class Simpletype:
    """
    Enum containing ids for all simple types, i.e. types that can be serialized directly using the serialization
    library.
    """
    BOOLEAN = 1
    BOOLEAN_LIST = 2
    BOOLEAN_SET = 3
    INTEGER = 4
    INTEGER_LIST = 5
    INTEGER_SET = 6
    LONG = 7
    LONG_LIST = 8
    LONG_SET = 9
    DOUBLE = 10  # np.float64 and Python floats
    DOUBLE_LIST = 11
    DOUBLE_SET = 12
    STRING = 13
    STRING_LIST = 14
    STRING_SET = 15
    BYTES = 16
    BYTES_LIST = 17
    BYTES_SET = 18
    FLOAT = 19  # np.float32 and smaller numpy floats
    FLOAT_LIST = 20
    FLOAT_SET = 21


def is_collection(data_type):
    """
    Checks if the given type is a collection type.
    """
    return (data_type is list  #
            or data_type is tuple  #
            or data_type is set  #
            or data_type is frozenset  #
            or data_type is dict)


def is_missing(value):
    """
    Checks if the given value is None, NaN or NaT.
    """
    return value is None or is_nat(value) or is_nan(value)


def is_nan(value):
    """
    Checks if the given value is NaN.
    """
    try:
        return math.isnan(value)
    except BaseException:
        return False


def is_nat(value):
    """
    Checks if the given value is NaT.
    """
    if EnvironmentHelper.is_tslib_available():
        return value is NaT
    else:
        return False


def is_numpy_type(data_type):
    """
    Checks if the given type is a numpy type, or, if it's an instance of a class, checks if its class is a numpy type.
    """
    if not inspect.isclass(data_type):
        data_type = type(data_type)
    return data_type.__module__ == numpy.__name__


def is_boolean_type(data_type):
    return data_type in _BOOLEAN_TYPES


def is_double_type(data_type):
    return data_type in _DOUBLE_TYPES


def is_float_type(data_type):
    return data_type in _FLOAT_TYPES


def is_integer_type(data_type):
    """
    Note, "integer" is to be understood in its mathematical sense, i.e., as a number without a fractional component.
    This does include, but is _not_ equivalent to, the unsigned 32-bit numeric data type.
    """
    return data_type in _INTEGER_TYPES


def fits_into_java_integer(data_type):
    """
    :param data_type: It is assumed that is_integer_type returns True for this argument.
    """
    return data_type.itemsize < 4 or (data_type.itemsize == 4 and data_type.kind == 'i')


def fits_into_java_long(data_type):
    """
    :param data_type: It is assumed that is_integer_type returns True for this argument.
    """
    return data_type.itemsize < 8 or (data_type.itemsize == 8 and data_type.kind == 'i')


def is_unsigned_byte_type(data_type):
    """
    "unsigned byte" refers to all flavors of the unsigned numeric data type of 8 bits of size.
    """
    return numpy.dtype('uint8') == data_type


def types_are_equivalent(type_1, type_2):
    """
    Checks if the two given types are equivalent based on the equivalence list and the equivalence of numpy types to
    python types.
    """
    for pair in EQUIVALENT_TYPES:
        if type_1 is pair[0] and type_2 is pair[1]:
            return True
        if type_1 is pair[1] and type_2 is pair[0]:
            return True
    if is_collection(type_1) or is_collection(type_2):
        return type_1 is type_2
    if is_numpy_type(type_1) or is_numpy_type(type_2):
        return numpy.issubdtype(type_1, type_2) and numpy.issubdtype(type_2, type_1)
    else:
        return type_1 is type_2


def get_type_string(data_object):
    """
    Gets the name of an object's type.
    NOTE: The name of an object's type is not the fully qualified one returned by the type() function but just the last
    piece of it (e.g. 'time' instead of 'datetime.time').
    """
    if hasattr(data_object, '__module__'):
        return data_object.__module__ + '.' + data_object.__class__.__name__
    else:
        return data_object.__class__.__name__


def object_to_string(data_object):
    """
    Convert data_object to a (possibly truncated) string representation.
    """
    if EnvironmentHelper.is_python3():
        try:
            object_as_string = str(data_object)
        except Exception:
            return ''
    else:
        try:
            object_as_string = unicode(data_object)
        except UnicodeDecodeError:
            object_as_string = '(base64 encoded)\n' + base64.b64encode(data_object)
        except Exception:
            return ''
    return (object_as_string[:996] + '\n...') if len(object_as_string) > 1000 else object_as_string


def value_to_simpletype_value(value, simpletype):
    """
    Convert a value into a given {@link Simpletype}.
    @param value the value to convert
    @param simpletype the {@link Simpletype} to convert into
    """
    if value is None:
        return None
    elif simpletype == Simpletype.BOOLEAN:
        return bool(value)
    elif simpletype == Simpletype.BOOLEAN_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                value[i] = bool(value[i])
        return value
    elif simpletype == Simpletype.BOOLEAN_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                value_set.add(bool(inner_value))
        return value_set
    elif simpletype == Simpletype.INTEGER:
        return int(value)
    elif simpletype == Simpletype.INTEGER_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                value[i] = int(value[i])
        return value
    elif simpletype == Simpletype.INTEGER_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                value_set.add(int(inner_value))
        return value_set
    elif simpletype == Simpletype.LONG:
        return int(value)
    elif simpletype == Simpletype.LONG_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                value[i] = int(value[i])
        return value
    elif simpletype == Simpletype.LONG_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                value_set.add(int(inner_value))
        return value_set
    elif simpletype == Simpletype.DOUBLE or simpletype == Simpletype.FLOAT:
        float_value = float(value)
        return float_value
    elif simpletype == Simpletype.DOUBLE_LIST or simpletype == Simpletype.FLOAT_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                float_value = float(value[i])
                value[i] = float_value
        return value
    elif simpletype == Simpletype.DOUBLE_SET or simpletype == Simpletype.FLOAT_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                float_value = float(inner_value)
                value_set.add(float_value)
        return value_set
    elif simpletype == Simpletype.STRING:
        return str(value)
    elif simpletype == Simpletype.STRING_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                value[i] = str(value[i])
        return value
    elif simpletype == Simpletype.STRING_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                value_set.add(str(inner_value))
        return value_set
    elif simpletype == Simpletype.BYTES:
        return bytes(value)
    elif simpletype == Simpletype.BYTES_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                value[i] = bytes(value[i])
        return value
    elif simpletype == Simpletype.BYTES_SET:
        value_set = set()
        for inner_value in value:
            if inner_value is None:
                value_set.add(None)
            else:
                value_set.add(bytes(inner_value))
        return value_set


def bytes_from_file(path):
    return open(path, 'rb').read()


def load_module_from_path(path):
    """
    Load a python module from a source file.
    @param path the path to the source file (string)
    @return the module loaded from the specified path
    """
    last_separator = path.rfind(os.sep)
    file_extension_start = path.rfind('.')
    module_name = path[last_separator + 1:file_extension_start]
    try:
        if EnvironmentHelper.is_python3():
            loaded_module = importlib.machinery.SourceFileLoader(module_name, path).load_module()
        else:
            loaded_module = imp.load_source(module_name, path)
    except ImportError as error:
        raise ImportError('Error while loading python module ' + module_name + '\nCause: ' + str(error))
    return loaded_module


def invoke_safely(exception_consumer, method, invokees):
    if method is None or invokees is None:
        return None
    if not isinstance(invokees, list):
        invokees = [invokees]
    error = None
    for i in invokees:
        try:
            if i is not None:
                method(i)
        except Exception as ex:
            if exception_consumer is not None:
                try:
                    exception_consumer("An exception occurred while safely invoking a method. Exception: " + str(ex),
                                       ex)
                except Exception:
                    pass  # ignore
                except BaseException as ex:
                    # Error will be handled by caller.
                    if error is None:
                        error = ex
        except BaseException as ex:
            # Error will be handled by caller.
            if error is None:
                error = ex
    return error
