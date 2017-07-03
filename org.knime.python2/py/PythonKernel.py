# -*- coding: utf-8 -*-

import sys
import math
import socket
import struct
import base64
import traceback
import numpy
import os
import pickle
import types
import collections
from datetime import datetime
from pandas import DataFrame, Index
from DBUtil import *

# suppress FutureWarnings
import warnings
warnings.filterwarnings(action='ignore', category=FutureWarning)


# check if we are running python 2 or python 3
_python3 = sys.version_info >= (3, 0)

if _python3:
    from io import StringIO
    import importlib
else:
    from StringIO import StringIO
    import imp

try:
    from pandas.tslib import Timestamp
    from pandas.tslib import NaT

    _tslib_available = True
except ImportError:
    Timestamp = None
    NaT = None
    _tslib_available = False

# load jedi for auto completion if available
try:
    import jedi

    _jedi_available = True
except ImportError:
    jedi = None
    _jedi_available = False

# global variables in the execution environment
_exec_env = {}
# TCP connection
_connection = None
_cleanup_object_names = []

# serialization library module
_serializer = None

# list of equivalent types
EQUIVALENT_TYPES = []

# fill EQUIVALENT_TYPES
if not _python3:
    EQUIVALENT_TYPES.append([unicode, str])
    EQUIVALENT_TYPES.append([int, long])

if _tslib_available:
    EQUIVALENT_TYPES.append([datetime, Timestamp])

# ******************************************************
# Remote debugging section
# ******************************************************

import debug_util
#debug_util.init_debug()
debug_util.debug_msg('Python Kernel enabled debugging!')


# ******************************************************
# Remote debugging section
# ******************************************************


# connect to the server on the localhost at the port given by the argument 1 and listen for commands
def run():
    global _connection
    global _serializer
    _connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _connection.connect(('localhost', int(sys.argv[1])))
    # load serialization library
    serializer_path = sys.argv[2]
    last_separator = serializer_path.rfind(os.sep)
    serializer_directory_path = serializer_path[0:last_separator + 1]
    sys.path.append(serializer_directory_path)
    _serializer = load_module_from_path(serializer_path)
    _serializer.init(Simpletype)
    # First send PID of this process (so it can reliably be killed later)
    write_integer(os.getpid())
    try:
        while 1:
            command = read_string()
            if command == 'execute':
                source_code = read_string()
                debug_util.debug_msg('executing: ' + source_code + '\n')
                output, error = execute(source_code)
                debug_util.debug_msg('executing done!')
                write_string(output)
                write_string(error)
            elif command == 'putFlowVariables':
                flow_variables = collections.OrderedDict()
                name = read_string()
                data_bytes = read_bytearray()
                data_frame = bytes_to_data_frame(data_bytes)
                fill_flow_variables_from_data_frame(flow_variables, data_frame)
                put_variable(name, flow_variables)
                write_dummy()
            elif command == 'getFlowVariables':
                name = read_string()
                current_variables = get_variable(name)
                data_frame = dict_to_data_frame(current_variables)
                data_bytes = data_frame_to_bytes(data_frame)
                write_bytearray(data_bytes)
            elif command == 'putTable':
                name = read_string()
                data_bytes = read_bytearray()
                data_frame = bytes_to_data_frame(data_bytes)
                put_variable(name, data_frame)
                write_dummy()
            elif command == 'appendToTable':
                name = read_string()
                data_bytes = read_bytearray()
                data_frame = bytes_to_data_frame(data_bytes)
                append_to_table(name, data_frame)
                write_dummy()
            elif command == 'getTableSize':
                name = read_string()
                data_frame = get_variable(name)
                write_integer(len(data_frame))
            elif command == 'getTable':
                debug_util.debug_msg('getTable\n')
                name = read_string()
                data_frame = get_variable(name)
                data_bytes = data_frame_to_bytes(data_frame)
                write_bytearray(data_bytes)
            elif command == 'getTableChunk':
                debug_util.debug_msg('getTableChunk\n')
                name = read_string()
                start = read_integer()
                end = read_integer()
                data_frame = get_variable(name)
                import pandas
                if type(data_frame) != pandas.core.frame.DataFrame:
                    raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame)) + "\nPlease make sure your output_table is a pandas.DataFrame.")
                data_frame_chunk = data_frame[start:end+1]
                data_bytes = data_frame_to_bytes(data_frame_chunk)
                write_bytearray(data_bytes)
            elif command == 'listVariables':
                variables = list_variables()
                data_frame = DataFrame(variables)
                data_bytes = data_frame_to_bytes(data_frame)
                write_bytearray(data_bytes)
            elif command == 'reset':
                reset()
                write_dummy()
            elif command == 'hasAutoComplete':
                if has_auto_complete():
                    value = 1
                else:
                    value = 0
                write_integer(value)
            elif command == 'autoComplete':
                source_code = read_string()
                line = read_integer()
                column = read_integer()
                suggestions = auto_complete(source_code, line, column)
                data_frame = DataFrame(suggestions)
                data_bytes = data_frame_to_bytes(data_frame)
                write_bytearray(data_bytes)
            elif command == 'getImage':
                name = read_string()
                image = get_variable(name)
                if _python3:
                    if type(image) is bytes:
                        data_bytes = image
                    else:
                        data_bytes = bytearray()
                else:
                    if type(image) is str:
                        data_bytes = image
                    else:
                        data_bytes = ''
                write_bytearray(data_bytes)
            elif command == 'getObject':
                name = read_string()
                data_object = get_variable(name)
                o_bytes = bytearray(pickle.dumps(data_object))
                o_type = type(data_object).__name__
                o_representation = object_to_string(data_object)
                data_frame = DataFrame([{'bytes': o_bytes, 'type': o_type, 'representation': o_representation}])
                data_bytes = data_frame_to_bytes(data_frame)
                write_bytearray(data_bytes)
            elif command == 'putObject':
                name = read_string()
                data_bytes = read_bytearray()
                data_object = pickle.loads(data_bytes)
                put_variable(name, data_object)
                write_dummy()
            elif command == 'addSerializer':
                s_id = read_string()
                s_type = read_string()
                s_path = read_string()
                _type_extension_manager.add_serializer(s_id, s_type, s_path)
                write_dummy()
            elif command == 'addDeserializer':
                d_id = read_string()
                d_path = read_string()
                _type_extension_manager.add_deserializer(d_id, d_path)
                write_dummy()
            elif command == 'shutdown':
                _cleanup()
                exit()
            elif command == 'putSql':
                name = read_string()
                data_bytes = read_bytearray()
                data_frame = bytes_to_data_frame(data_bytes)
                db_util = DBUtil(data_frame)
                _exec_env[name] = db_util
                _cleanup_object_names.append(name)
                write_dummy()
            elif command == 'getSql':
                name = read_string()
                db_util = get_variable(name)
                db_util._writer.commit()
                query = db_util.get_output_query()
                write_string(query)
    finally:
        _connection.close()


def bytes_from_file(path):
    return open(path, 'rb').read()


# Converts data_bytes into a pandas DataFrame using the configured serialization library.
# For extension types appropriate deserializers are requested from the _type_extension_manager.
# @param data_bytes    a bytearray containing a serialized KNIME table
def bytes_to_data_frame(data_bytes):
    column_names = _serializer.column_names_from_bytes(data_bytes)
    if len(column_names) == 0:
        return DataFrame()
    column_types = _serializer.column_types_from_bytes(data_bytes)
    column_serializers = _serializer.column_serializers_from_bytes(data_bytes)
    table = ToPandasTable(column_names, column_types, column_serializers)
    _serializer.bytes_into_table(table, data_bytes)
    return table.get_data_frame()


# Converts data_frame into a bytearray using the configured serialization library.
# For extension types appropriate serializers are requested from the _type_extension_manager.
# @param data_frame    a pandas DataFrame containing the table to serialize
def data_frame_to_bytes(data_frame):
    table = FromPandasTable(data_frame)
    data_bytes = _serializer.table_to_bytes(table)
    return data_bytes

# Fill the flow variable dict using a pandas DataFrame. The DataFrame is expected
# to contain only a single row. Each column name corresponds to a flow variable
# name and each cell to the respective flow variable's value.
# @param flow_variables    the flow variable dict to fill
# @param data_frame        the pandas DataFrame to fill from
def fill_flow_variables_from_data_frame(flow_variables, data_frame):
    for column in data_frame.columns:
        simpletype = simpletype_for_column(data_frame, column)[0]
        if simpletype == Simpletype.INTEGER:
            flow_variables[column] = int(data_frame[column][0])
        elif simpletype == Simpletype.DOUBLE:
            flow_variables[column] = float(data_frame[column][0])
        else:
            flow_variables[column] = str(data_frame[column][0])


# Convert a python dict to a pandas DataFrame in which each dict key represents
# a column and each dict value a cell in the respective column. Thus the resulting
# DataFrame will contain a single row.
# @param dictionary    a python dictionary
def dict_to_data_frame(dictionary):
    df = DataFrame()
    for key in dictionary:
        df[key] = [dictionary[key]]
    return df


def _cleanup():
    for name in _cleanup_object_names:
        obj = get_variable(name)
        if obj:
            try:
                obj._cleanup()
            except Exception:
                pass


# execute the given source code
def execute(source_code):
    output = StringIO()
    error = StringIO()
    if not debug_util.is_debug_enabled():
        sys.stdout = output
    # run execute with the provided source code
    try:
        exec(source_code, _exec_env, _exec_env)
    except Exception:
        traceback.print_exc(file=error)
    if not debug_util.is_debug_enabled():
        sys.stdout = sys.__stdout__
    return [output.getvalue(), error.getvalue()]


# put the given variable into the local environment under the given name
def put_variable(name, variable):
    _exec_env[name] = variable


# append the given data frame to an existing one, if it does not exist put the data frame into the local environment
def append_to_table(name, data_frame):
    if _exec_env[name] is None:
        _exec_env[name] = data_frame
    else:
        _exec_env[name] = _exec_env[name].append(data_frame)


# get the variable with the given name
def get_variable(name):
    if name in _exec_env:
        return _exec_env[name]
    else:
        raise NameError(name + ' is not defined.')


# list all currently loaded modules and defined classes, functions and variables
def list_variables():
    # create lists of modules, classes, functions and variables
    modules = []
    classes = []
    functions = []
    variables = []
    # iterate over dictionary to and put modules, classes, functions and variables in their respective lists
    for key, value in dict(_exec_env).items():
        # get name of the type
        var_type = type(value).__name__
        # class type changed from classobj to type in python 3
        class_type = 'classobj'
        if _python3:
            class_type = 'type'
        if var_type == 'module':
            modules.append({'name': key, 'type': var_type, 'value': ''})
        elif var_type == class_type:
            classes.append({'name': key, 'type': var_type, 'value': ''})
        elif var_type == 'function':
            functions.append({'name': key, 'type': var_type, 'value': ''})
        elif key != '__builtins__':
            value = object_to_string(value)
            variables.append({'name': key, 'type': var_type, 'value': value})
    # sort lists by name
    modules = sorted(modules, key=lambda k: k['name'])
    classes = sorted(classes, key=lambda k: k['name'])
    functions = sorted(functions, key=lambda k: k['name'])
    variables = sorted(variables, key=lambda k: k['name'])
    # create response list and add contents of the other lists in the order they should be displayed
    response = []
    response.extend(modules)
    response.extend(classes)
    response.extend(functions)
    response.extend(variables)
    return response


# reset the current environment
def reset():
    # reset environment by emptying variable definitions
    global _exec_env
    _exec_env = {}


# returns true if auto complete is available, false otherwise
def has_auto_complete():
    return _jedi_available


# returns a list of auto suggestions for the given code at the given cursor position
def auto_complete(source_code, line, column):
    response = []
    if has_auto_complete():
        # get possible completions by using Jedi and providing the source code, and the cursor position
        # note: the line number (argument 2) gets incremented by 1 since Jedi's line numbering starts at 1
        completions = jedi.Script(source_code, line + 1, column, None).completions()
        # extract interesting information
        for index, completion in enumerate(completions):
            response.append({'name': completion.name, 'type': completion.type, 'doc': completion.docstring()})
    return response


# get the type of a column (fails if multiple types are found)
def column_type(data_frame, column_name):
    col_type = None
    for cell in data_frame[column_name]:
        if not is_missing(cell):
            if col_type is not None:
                if not types_are_equivalent(type(cell), col_type):
                    raise ValueError('More than one type in column ' + str(column_name) + '. Found '
                                     + col_type.__name__ + ' and ' + type(cell).__name__)
            else:
                col_type = type(cell)
    return col_type


# get the type of a list column (fails if multiple types are found)
def list_column_type(data_frame, column_name):
    col_type = None
    for list_cell in data_frame[column_name]:
        if list_cell is not None:
            for cell in list_cell:
                if not is_missing(cell):
                    if col_type is not None:
                        if not types_are_equivalent(type(cell), col_type):
                            raise ValueError('More than one type in column ' + str(column_name) + '. Found '
                                             + col_type.__name__ + ' and ' + type(cell).__name__)
                    else:
                        col_type = type(cell)
    return col_type

# Returns the first object in a pandas DataFrame column that does not represent
# a missing type (None, NaN or NaT)
def first_valid_object(data_frame, column_name):
    for cell in data_frame[column_name]:
        if not is_missing(cell):
            return cell
    return None

# Returns the first object contained in a list in a pandas DataFrame column that 
# does not represent a missing type (None, NaN or NaT)
def first_valid_list_object(data_frame, column_name):
    for list_cell in data_frame[column_name]:
        if list_cell is not None:
            for cell in list_cell:
                if not is_missing(cell):
                    return cell
    return None


# checks if the two given types are equivalent based on the equivalence list and the equivalence of numpy types to
# python types
def types_are_equivalent(type_1, type_2):
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


# checks if the given type is a collection type
def is_collection(data_type):
    return data_type is list or data_type is set or data_type is dict or data_type is tuple


# checks if the given type is a numpy type
def is_numpy_type(data_type):
    return data_type.__module__ == numpy.__name__


# checks if the given value is None, NaN or NaT
def is_missing(value):
    return value is None or is_nat(value) or is_nan(value)


# checks if the given value is NaT
def is_nat(value):
    if _tslib_available:
        return value is NaT
    else:
        return False


# checks if the given value is NaN
def is_nan(value):
    try:
        return math.isnan(value)
    except BaseException:
        return False


def get_type_string(data_object):
    if hasattr(data_object, '__module__'):
        return data_object.__module__ + '.' + data_object.__class__.__name__
    else:
        return data_object.__class__.__name__


class TypeExtensionManager:
    def __init__(self):
        self._serializer_id_to_index = {}
        self._serializer_type_to_id = {}
        self._serializers = []
        self._deserializer_id_to_index = {}
        self._deserializers = []

    def get_deserializer_by_id(self, identifier):
        if identifier not in self._deserializer_id_to_index:
            return None
        return self.get_extension_by_index(self._deserializer_id_to_index[identifier], self._deserializers)

    def get_serializer_by_id(self, identifier):
        if identifier not in self._serializer_id_to_index:
            return None
        return self.get_extension_by_index(self._serializer_id_to_index[identifier], self._serializers)

    def get_serializer_by_type(self, type_string):
        if type_string not in self._serializer_type_to_id:
            return None
        return self.get_serializer_by_id(self._serializer_type_to_id[type_string])

    def get_serializer_id_by_type(self, type_string):
        if type_string not in self._serializer_type_to_id:
            return None
        return self._serializer_type_to_id[type_string]

    @staticmethod
    def get_extension_by_index(index, extensions):
        if index >= len(extensions):
            return None
        type_extension = extensions[index]
        if not isinstance(type_extension, types.ModuleType):
            path = type_extension
            last_separator = path.rfind(os.sep)
            file_extension_start = path.rfind('.')
            module_name = path[last_separator + 1:file_extension_start]
            try:
                if _python3:
                    type_extension = importlib.machinery.SourceFileLoader(module_name, path).load_module()
                else:
                    type_extension = imp.load_source(module_name, path)
            except ImportError as error:
                raise ImportError('Error while loading python type extension ' + module_name + '\nCause: ' + str(error))
            extensions[index] = type_extension
        return type_extension

    def add_serializer(self, identifier, type_string, path):
        index = len(self._serializers)
        self._serializers.append(path)
        self._serializer_id_to_index[identifier] = index
        self._serializer_type_to_id[type_string] = identifier

    def add_deserializer(self, identifier, path):
        index = len(self._deserializers)
        self._deserializers.append(path)
        self._deserializer_id_to_index[identifier] = index


def load_module_from_path(path):
    last_separator = path.rfind(os.sep)
    file_extension_start = path.rfind('.')
    module_name = path[last_separator + 1:file_extension_start]
    try:
        if _python3:
            loaded_module = importlib.machinery.SourceFileLoader(module_name, path).load_module()
        else:
            loaded_module = imp.load_source(module_name, path)
    except ImportError as error:
        raise ImportError('Error while loading python module ' + module_name + '\nCause: ' + str(error))
    return loaded_module

if _python3:
    def object_to_string(data_object):
        try:
            return str(data_object)
        except Exception:
            return ''
else:
    def object_to_string(data_object):
        try:
            return unicode(data_object)
        except UnicodeDecodeError:
            return '(base64 encoded)\n' + base64.b64encode(data_object)
        except Exception:
            return ''


_type_extension_manager = TypeExtensionManager()


def serialize_objects_to_bytes(data_frame, column_serializers):
    for column in column_serializers:
        serializer = _type_extension_manager.get_serializer_by_id(column_serializers[column])
        col_idx = data_frame.columns.get_loc(column)
        if data_frame[column].dtype != 'object':
            data_frame[column] = data_frame[column].astype('object')
        for i in range(len(data_frame)):
            if debug_util.is_debug_enabled():
                lastp = -1
                if (i * 100/len(data_frame)) % 5 == 0 and int(i * 100/len(data_frame)) != lastp:
                    debug_util.debug_msg(str(i * 100/len(data_frame)) + ' percent done (serialize)')
                    lastp = int(i * 100/len(data_frame))
            # Using bracket acessor is necessary here for ensuring that there are
            # no unwanted type conversions
            value = data_frame[column][i] 
            if value is not None:
                if isinstance(value, list):
                    new_list = []
                    for inner_value in value:
                        if inner_value is None:
                            new_list.append(None)
                        else:
                            new_list.append(serializer.serialize(inner_value))
                    data_frame.iat[i,col_idx] = new_list
                elif isinstance(value, set):
                    new_set = set()
                    for inner_value in value:
                        if inner_value is None:
                            new_set.add(None)
                        else:
                            new_set.add(serializer.serialize(inner_value))
                    data_frame.iat[i,col_idx] = new_set
                else:
                    data_frame.iat[i,col_idx] = serializer.serialize(value)


def deserialize_from_bytes(data_frame, column_serializers):
    #print('Data frame: ' + str(data_frame) + '\nserializers: ' + str(column_serializers) + '\n')
    for column in column_serializers:
        deserializer = _type_extension_manager.get_deserializer_by_id(column_serializers[column])
        for i in range(len(data_frame)):
            if debug_util.is_debug_enabled():
                lastp = -1
                if (i * 100/len(data_frame)) % 5 == 0 and int(i * 100/len(data_frame)) != lastp:
                    debug_util.debug_msg(str(i * 100/len(data_frame)) + ' percent done (deserialize)')
                    lastp = int(i * 100/len(data_frame))
            col_idx = data_frame.columns.get_loc(column)
            # Using bracket acessor is necessary here for ensuring that there are
            # no unwanted type conversions
            value = data_frame[column][i] 
            if isinstance(value, numpy.float64) and numpy.isnan(value):
                value = None
            if value:
                if isinstance(value, list):
                    new_list = []
                    for inner_value in value:
                        if isinstance(inner_value, numpy.float64) and numpy.isnan(inner_value):
                            inner_value = None
                        if inner_value:
                            new_list.append(deserializer.deserialize(inner_value))
                        else:
                            new_list.append(None)
                    data_frame.iat[i,col_idx] = new_list
                elif isinstance(value, set):
                    new_set = set()
                    for inner_value in value:
                        if isinstance(inner_value, numpy.float64) and numpy.isnan(inner_value):
                            inner_value = None
                        if inner_value:
                            new_set.add(deserializer.deserialize(inner_value))
                        else:
                            new_set.add(None)
                    data_frame.iat[i,col_idx] = new_set
                else:
                    data_frame.iat[i,col_idx] = deserializer.deserialize(value)
            else:
                data_frame.iat[i,col_idx] = None


# reads 4 bytes from the input stream and interprets them as size
def read_size():
    data = bytearray()
    while len(data) < 4:
        data.extend(_connection.recv(4))
    return struct.unpack('>L', data)[0]


# read the next data from the input stream
def read_data():
    size = read_size()
    data = bytearray()
    while len(data) < size:
        data.extend(_connection.recv(size))
    return data


# writes the given size as 4 byte integer to the output stream
def write_size(size):
    writer = _connection.makefile('wb', 4)
    writer.write(struct.pack('>L', size))
    writer.flush()
    #_connection.sendall(struct.pack('>L', size))


# writes the given data to the output stream
def write_data(data):
    write_size(len(data))
    _connection.sendall(data)


# writes an empty message
def write_dummy():
    write_size(0)


def read_integer():
    return struct.unpack('>L', read_data())[0]


def write_integer(integer):
    write_data(struct.pack('>L', integer))


def read_string():
    return read_data().decode('utf-8')


def write_string(string):
    write_data(bytearray(string, 'utf-8'))


def read_bytearray():
    return bytearray(read_data())


def write_bytearray(data_bytes):
    write_data(data_bytes)


class FromPandasTable:
    def __init__(self, data_frame):
        self._data_frame = data_frame.copy()
        self._data_frame.columns = self._data_frame.columns.astype(str)
        self._column_types = []
        self._column_serializers = {}
        for column in self._data_frame.columns:
            column_type, serializer_id = simpletype_for_column(self._data_frame, column)
            self._column_types.append(column_type)
            if serializer_id is not None:
                self._column_serializers[column] = serializer_id
        #debug_util.breakpoint()
        serialize_objects_to_bytes(self._data_frame, self._column_serializers)
        self.standardize_default_indices()
        self._row_indices = self._data_frame.index.astype(str)


    # Replace default numeric indices with the KNIME standard row indices.
    # This means that if an index value is equal to the numeric index of
    # a row (N) it is replaced by 'RowN'.
    def standardize_default_indices(self):
        row_indices = []
        for i in range(len(self._data_frame.index)):
            #debug_util.breakpoint()
            try:
                if int(self._data_frame.index[i]) == i:
                    row_indices.append(u'Row' + str(i))
                else:
                    row_indices.append(str(self._data_frame.index[i]))
            except:
                row_indices.append(str(self._data_frame.index[i]))
        self._data_frame.set_index(keys=Index(row_indices), drop=True, inplace=True)

    # example: table.get_type(0)
    def get_type(self, column_index):
        return self._column_types[column_index]

    # example: table.get_name(0)
    def get_name(self, column_index):
        return self._data_frame.columns.astype(str)[column_index]

    def get_names(self):
        return self._data_frame.columns.astype(str)

    # example: table.get_cell(0,0)
    def get_cell(self, column_index, row_index):
        value = self._data_frame.iat[row_index, column_index]
        if value is None:
            return None
        else:
            return value_to_simpletype_value(value,
                                             self._column_types[column_index])

    # example: table.get_rowkey(0)
    def get_rowkey(self, row_index):
        return self._row_indices[row_index]

    def get_rowkeys(self):
        return self._data_frame.index.astype(str)

    def get_number_columns(self):
        return len(self._data_frame.columns)

    def get_number_rows(self):
        return len(self._data_frame.index)

    def get_column_serializers(self):
        return self._column_serializers


class ToPandasTable:
    # example: ToPandasTable(('column1','column2'))
    def __init__(self, column_names, column_types, column_serializers):
        dtypes = {}
        for i in range(len(column_names)):
            name = column_names[i]
            if column_types[i] == Simpletype.BOOLEAN:
                col_type = numpy.bool
            elif column_types[i] == Simpletype.INTEGER:
                col_type = numpy.int32
            elif column_types[i] == Simpletype.LONG:
                col_type = numpy.int64
            elif column_types[i] == Simpletype.DOUBLE:
                col_type = numpy.float64
            else:
                col_type = numpy.str
            dtypes[name] = col_type
        self._dtypes = dtypes
        self._column_names = column_names
        self._data_frame = DataFrame(columns=column_names)
        self._column_serializers = column_serializers

    # example: table.add_row('row1',[1,2])
    def add_row(self, rowkey, values):
        row = DataFrame([values], index=[rowkey], columns=self._column_names)
        self._data_frame = self._data_frame.append(row)

    # example: table.add_column('column1', [1,2,3])
    def add_column(self, column_name, values):
        self._data_frame[column_name] = values

    # example: table.set_rowkeys(['row1','row2','row3'])
    def set_rowkeys(self, rowkeys):
        if len(self._data_frame.columns) == 0:
            self._data_frame = DataFrame(index=rowkeys)
        else:
            self._data_frame.index = rowkeys

    def get_data_frame(self):
        deserialize_from_bytes(self._data_frame, self._column_serializers)
        # Commented out since changing the type if the column contains missing values fails
        # if len(self._data_frame) > 0:
        #     for column in self._data_frame.columns:
        #         self._data_frame[column] = self._data_frame[column].astype(self._dtypes[column])
        return self._data_frame


class Simpletype():
    BOOLEAN = 1
    BOOLEAN_LIST = 2
    BOOLEAN_SET = 3
    INTEGER = 4
    INTEGER_LIST = 5
    INTEGER_SET = 6
    LONG = 7
    LONG_LIST = 8
    LONG_SET = 9
    DOUBLE = 10
    DOUBLE_LIST = 11
    DOUBLE_SET = 12
    STRING = 13
    STRING_LIST = 14
    STRING_SET = 15
    BYTES = 16
    BYTES_LIST = 17
    BYTES_SET = 18


def simpletype_for_column(data_frame, column_name):
    column_serializer = None
    if len(data_frame.index) == 0:
        # if table is empty we don't know the types so we make them strings
        simple_type = Simpletype.STRING
    else:
        if data_frame[column_name].dtype == 'bool':
            simple_type = Simpletype.BOOLEAN
        elif data_frame[column_name].dtype == 'int32' or data_frame[column_name].dtype == 'int64':
            minvalue = data_frame[column_name][data_frame[column_name].idxmin()]
            maxvalue = data_frame[column_name][data_frame[column_name].idxmax()]
            int32min = -2147483648
            int32max = 2147483647
            if minvalue >= int32min and maxvalue <= int32max:
                simple_type = Simpletype.INTEGER
            else:
                simple_type = Simpletype.LONG
        elif data_frame[column_name].dtype == 'double' or column_type(data_frame, column_name) == float:
            simple_type = Simpletype.DOUBLE
        else:
            col_type = column_type(data_frame, column_name)
            if col_type is None:
                # column with only missing values, make it string
                simple_type = Simpletype.STRING
            elif types_are_equivalent(col_type, bool):
                simple_type = Simpletype.BOOLEAN
            elif col_type is list or col_type is set:
                is_set = col_type is set
                list_col_type = list_column_type(data_frame, column_name)
                if list_col_type is None:
                    # column with only missing values, make it string
                    if is_set:
                        simple_type = Simpletype.STRING_SET
                    else:
                        simple_type = Simpletype.STRING_LIST
                elif types_are_equivalent(list_col_type, bool):
                    if is_set:
                        simple_type = Simpletype.BOOLEAN_SET
                    else:
                        simple_type = Simpletype.BOOLEAN_LIST
                elif types_are_equivalent(list_col_type, int):
                    minvalue = 0
                    maxvalue = 0
                    for list_cell in data_frame[column_name]:
                        if list_cell is not None:
                            for cell in list_cell:
                                if cell is not None:
                                    minvalue = min(minvalue, cell)
                                    maxvalue = max(maxvalue, cell)
                    int32min = -2147483648
                    int32max = 2147483647
                    if minvalue >= int32min and maxvalue <= int32max:
                        if is_set:
                            simple_type = Simpletype.INTEGER_SET
                        else:
                            simple_type = Simpletype.INTEGER_LIST
                    else:
                        if is_set:
                            simple_type = Simpletype.LONG_SET
                        else:
                            simple_type = Simpletype.LONG_LIST
                elif types_are_equivalent(list_col_type, float):
                    if is_set:
                        simple_type = Simpletype.DOUBLE_SET
                    else:
                        simple_type = Simpletype.DOUBLE_LIST
                elif types_are_equivalent(list_col_type, str):
                    if is_set:
                        simple_type = Simpletype.STRING_SET
                    else:
                        simple_type = Simpletype.STRING_LIST
                else:
                    type_string = get_type_string(first_valid_list_object(data_frame, column_name))
                    column_serializer = _type_extension_manager.get_serializer_id_by_type(type_string)
                    if column_serializer is None:
                        raise ValueError('Column ' + str(column_name) + ' has unsupported type ' + type_string)
                    if is_set:
                        simple_type = Simpletype.BYTES_SET
                    else:
                        simple_type = Simpletype.BYTES_LIST
            elif types_are_equivalent(col_type, str):
                simple_type = Simpletype.STRING
            elif types_are_equivalent(col_type, bytes) or types_are_equivalent(col_type, bytearray):
                simple_type = Simpletype.BYTES
            else:
                type_string = get_type_string(first_valid_object(data_frame, column_name))
                column_serializer = _type_extension_manager.get_serializer_id_by_type(type_string)
                if column_serializer is None:
                    raise ValueError('Column ' + str(column_name) + ' has unsupported type ' + type_string)
                simple_type = Simpletype.BYTES
    return simple_type, column_serializer


def value_to_simpletype_value(value, simpletype):
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
    elif simpletype == Simpletype.DOUBLE:
        float_value = float(value)
        return float_value
    elif simpletype == Simpletype.DOUBLE_LIST:
        for i in range(0, len(value)):
            if value[i] is not None:
                float_value = float(value[i])
                value[i] = float_value
        return value
    elif simpletype == Simpletype.DOUBLE_SET:
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

# Uncomment below and comment the run() call for profiling
# See https://docs.python.org/3/library/profile.html on how to interpet the result
#import cProfile
#profilepath = os.path.join(os.path.expanduser('~'), 'profileres.txt')
#cProfile.run('run()', filename=profilepath)
run()