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
#  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
from PythonToJavaMessage import *
from TypeExtensionManager import *
from Borg import Borg

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


#Logger class for parallel logging to sys.stdout and a sink, e.g. a StringIO
#object or a file
class Logger(object):
    def __init__(self, stdstream, sink):
        self.stdstream = stdstream
        self.sink = sink
        
    def write(self, message):
        self.stdstream.write(message)
        self.sink.write(message)
        
    def writelines(self, sequence):
        self.stdstream.writelines(sequence)
        self.sink.writelines(sequence)
        
    def flush(self):
        self.stdstream.flush()
        self.sink.flush()
        
# Wrapper class for data that should be serialized using the serialization library. 
# Manages the serialization of extension types to bytes before using the 
# registered serialization library for serializing primitive types.
class FromPandasTable:
    
    # Constructor.
    # Serializes objects having a type that is registered via the knimetopython
    # extension point to a bytes representation and adjusts the dataframe index
    # to reflect KNIME standard row indexing if necessary.
    # @param data_frame        a pandas DataFrame containing the table to serialize
    # @param kernel            the PythonKernel
    # @param start_row_number  the corresponding row number to the first row of the
    #                          dataframe. Differs from 0 as soon as a table chunk is
    #                          sent.
    def __init__(self, data_frame, kernel, start_row_number=0):
        self._data_frame = data_frame.copy()
        self._data_frame.columns = self._data_frame.columns.astype(str)
        self._column_types = []
        self._column_serializers = {}
        for column in self._data_frame.columns:
            column_type, serializer_id = kernel.simpletype_for_column(self._data_frame, column)
            self._column_types.append(column_type)
            if serializer_id is not None:
                self._column_serializers[column] = serializer_id
        kernel.serialize_objects_to_bytes(self._data_frame, self._column_serializers)
        self.standardize_default_indices(start_row_number)
        self._row_indices = self._data_frame.index.astype(str)


    # Replace default numeric indices with the KNIME standard row indices.
    # This means that if an index value is equal to the numeric index of
    # a row (N) it is replaced by 'RowN'.
    # @param start_row_number  the corresponding row number to the first row of the
    #                          dataframe. Differs from 0 as soon as a table chunk is
    #                          sent.
    def standardize_default_indices(self, start_row_number):
        row_indices = []
        for i in range(len(self._data_frame.index)):
            if type(self._data_frame.index[i]) == int and self._data_frame.index[i] == i + start_row_number:
                row_indices.append(u'Row' + str(i + start_row_number))
            else:
                row_indices.append(str(self._data_frame.index[i]))
        self._data_frame.set_index(keys=Index(row_indices), drop=True, inplace=True)

    # Get the type of the column at the provided index in the internal data_frame.
    # example: table.get_type(0)
    # @param column_index    numeric column index
    # @return a datatype
    def get_type(self, column_index):
        return self._column_types[column_index]

    # Get the name of the column at the provided index in the internal data_frame.
    # example: table.get_name(0)
    # @param column_index    numeric column index
    # @return a column name (string)
    def get_name(self, column_index):
        return self._data_frame.columns.astype(str)[column_index]

    # Get a list of all column names in the internal dataframe.
    # @return a list of column names (string)
    def get_names(self):
        return self._data_frame.columns.astype(str)

    # Get the cell content at the specified position.
    # example: table.get_cell(0,0)
    # @param column_index    the column index
    # @param row_index       the row index
    # @return the cell content
    def get_cell(self, column_index, row_index):
        #value = self._data_frame.iat[row_index, column_index]
        #if value is None:
        #    return None
        #else:
        #    return value_to_simpletype_value(value,
        #                                     self._column_types[column_index])
        return self._data_frame.iat[row_index, column_index]

    # example: table.get_rowkey(0)
    def get_rowkey(self, row_index):
        return self._row_indices[row_index]

    def get_rowkeys(self):
        return self._data_frame.index.astype(str)

    def get_number_columns(self):
        return len(self._data_frame.columns)

    def get_number_rows(self):
        return len(self._data_frame.index)

    # Get the column serializers.
    # @return a dict containing column names of the internal dataframe as keys
    #         and serializer_ids as values. A serializer_id is the id of the
    #         java extension point the serializer is registered at.
    def get_column_serializers(self):
        return self._column_serializers


# Wrapper class for data that should be deserialized using the serialization library. 
# Manages the deserialization of bytes to extension type objects after the 
# registered serialization library has been used for deserializing primitive types.
class ToPandasTable:
    
    # Constructor.
    # example: ToPandasTable(('column1','column2'))
    # @param column_names        list of names for the columns to deserialize
    # @param column_types        datatypes of the columns to deserialize
    # @param column_serializers  dict containing column names as keys and deserializer_ids
    #                            as values. A deserializer_id is the id of the
    #                            java extension point the deserializer is registered at.
    # @param kernel              the PythonKernel
    def __init__(self, column_names, column_types, column_serializers, kernel):
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
        self.kernel = kernel

    # Append a row to the internal dataframe.
    # example: table.add_row('row1',[1,2])
    # NOTE: use with caution -> slow, the preferred way is using add_column
    # or directly setting the internal data_frame
    def add_row(self, rowkey, values):
        row = DataFrame([values], index=[rowkey], columns=self._column_names)
        self._data_frame = self._data_frame.append(row)

    # Append a column to the internal dataframe.
    # example: table.add_column('column1', [1,2,3])
    def add_column(self, column_name, values):
        self._data_frame[column_name] = values

    # Set the index of the internal dataframe to rowkeys.
    # example: table.set_rowkeys(['row1','row2','row3'])
    def set_rowkeys(self, rowkeys):
        if len(self._data_frame.columns) == 0:
            self._data_frame = DataFrame(index=rowkeys)
        else:
            self._data_frame.index = rowkeys

    # Deserialize bytes in columns having an extension type and returns a 
    # pandas.DataFrame containing the deserialized KNIME table.
    def get_data_frame(self):
        self.kernel.deserialize_from_bytes(self._data_frame, self._column_serializers)
        return self._data_frame

# Enum containing ids for all simple tpyes, i.e. types that can be serialized
# directly using the serialization library.
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


class PythonKernel(Borg):
    
    def __init__(self):
        # global variables in the execution environment
        self._exec_env = {'request_from_java': self.write_message}
        # TCP connection
        self._connection = None
        self._cleanup_object_names = []

        # serialization library module
        self._serializer = None
        
        # Get the TypeExtensionManager instance
        self._type_extension_manager = TypeExtensionManager(self.write_message)
        
        # Define global command handlers
        self._command_handlers = [ExecuteCommandHandler(),PutFlowVariablesCommandHandler(),
                             GetFlowVariablesCommandHandler(),PutTableCommandHandler(),
                             AppendToTableCommandHandler(),GetTableSizeCommandHandler(),
                             GetTableCommandHandler(),GetTableChunkCommandHandler(),
                             ListVariablesCommandHandler(),ResetCommandHandler(),
                             HasAutoCompleteCommandHandler(),AutoCompleteCommandHandler(),
                             GetImageCommandHandler(),GetObjectCommandHandler(),
                             PutObjectCommandHandler(),AddSerializerCommandHandler(),
                             AddDeserializerCommandHandler(),ShutdownCommandHandler(),
                             PutSqlCommandHandler(),GetSqlCommandHandler(),
                             SetCustomModulePathsHandler()]
        
    def connect(self, parameters):
        self._connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connection.connect(parameters)
        
    def loadSerializer(self, path):
         self._serializer = self.load_module_from_path(path)
         self._serializer.init(Simpletype)
         
    #Register a new command handler processing a custom command
    def registerCommandHandler(self, cmdhandler):
        if not issubclass(type(cmdhandler), CommandHandler):
            raise TypeError("PythonKernel may only register command handlers inheriting form the CommandHandler class!")
        self._command_handlers.append(cmdhandler)
        
    # connect to the server on the localhost at the port given by the argument 1 and listen for commands
    def run(self):
        self.connect(('localhost', int(sys.argv[1])))
        # load serialization library
        serializer_path = sys.argv[2]
        last_separator = serializer_path.rfind(os.sep)
        serializer_directory_path = serializer_path[0:last_separator + 1]
        sys.path.append(serializer_directory_path)
        self.loadSerializer(serializer_path)
        
        # First send PID of this process (so it can reliably be killed later)
        self.write_integer(os.getpid())
        try:
            while 1:
               self.run_command(self.read_string())
        finally:
            self._cleanup()

    def run_command(self, command):
        handled = False
        for handler in self._command_handlers:
            if (handler.has_command(command)):
                handler.execute(self)
                handled = True
                break
        if not handled:
            raise LookupError('The command ' + command + ' was received but it cannot be handled by the Python Kernel.')

    def bytes_from_file(self, path):
        return open(path, 'rb').read()


    # Converts data_bytes into a pandas DataFrame using the configured serialization library.
    # For extension types appropriate deserializers are requested from the _type_extension_manager.
    # @param data_bytes    a bytearray containing a serialized KNIME table
    def bytes_to_data_frame(self, data_bytes):
        column_names = self._serializer.column_names_from_bytes(data_bytes)
        if len(column_names) == 0:
            return DataFrame()
        column_types = self._serializer.column_types_from_bytes(data_bytes)
        column_serializers = self._serializer.column_serializers_from_bytes(data_bytes)
        table = ToPandasTable(column_names, column_types, column_serializers, self)
        self._serializer.bytes_into_table(table, data_bytes)
        return table.get_data_frame()


    # Converts data_frame into a bytearray using the configured serialization library.
    # For extension types appropriate serializers are requested from the _type_extension_manager.
    # @param data_frame        a pandas DataFrame containing the table to serialize
    # @param start_row_number  the corresponding row number to the first row of the
    #                          dataframe. Differs from 0 as soon as a table chunk is
    #                          sent.
    def data_frame_to_bytes(self, data_frame, start_row_number=0):
        table = FromPandasTable(data_frame, self, start_row_number)
        #Uncomment to profile serialization time
        #import cProfile
        #profilepath = os.path.join(os.path.expanduser('~'), 'profileres.txt')
        #prof = cProfile.Profile()
        #data_bytes = prof.runcall(_serializer.table_to_bytes, table)
        #prof.dump_stats(profilepath)
        data_bytes = self._serializer.table_to_bytes(table)
        return data_bytes

    # Fill the flow variable dict using a pandas DataFrame. The DataFrame is expected
    # to contain only a single row. Each column name corresponds to a flow variable
    # name and each cell to the respective flow variable's value.
    # @param flow_variables    the flow variable dict to fill
    # @param data_frame        the pandas DataFrame to fill from
    def fill_flow_variables_from_data_frame(self, flow_variables, data_frame):
        for column in data_frame.columns:
            simpletype = self.simpletype_for_column(data_frame, column)[0]
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
    def flow_variables_dict_to_data_frame(self, dictionary):
        df = DataFrame()
        for key in dictionary:
            type = self.get_type_string(dictionary[key])
            if type.find('int') >= 0 or type.find('float') >= 0:
                df[key] = [dictionary[key]]
            else:
                df[key] = [str(dictionary[key])]
        return df

    # Is called on shutdown to clean up all variables that names are registered in 
    # _cleanup_object_names.
    def _cleanup(self):
        for name in self._cleanup_object_names:
            obj = self.get_variable(name)
            if obj:
                try:
                    obj._cleanup()
                except Exception:
                    pass

    # execute the given source code
    def execute(self, source_code):
        output = StringIO()
        error = StringIO()
        
        #log to stdout and output variable simultaneously
        backupStdOut = sys.stdout
        sys.stdout = Logger(sys.stdout, output)
        
        # run execute with the provided source code
        try:
            exec(source_code, self._exec_env, self._exec_env)
        except Exception:
            backupStdError = sys.stderr
            sys.stderr = error
            traceback.print_exc()
            sys.stderr = backupStdError
        
        sys.stdout = backupStdOut
        self.write_message(SuccessMessage())
        return [output.getvalue(), error.getvalue()]


    # put the given variable into the local environment under the given name
    def put_variable(self, name, variable):
        self._exec_env[name] = variable


    # append the given data frame to an existing one, if it does not exist put the data frame into the local environment
    def append_to_table(self, name, data_frame):
        if self._exec_env[name] is None:
            self._exec_env[name] = data_frame
        else:
            self._exec_env[name] = self._exec_env[name].append(data_frame)


    # get the variable with the given name
    def get_variable(self, name):
        if name in self._exec_env:
            return self._exec_env[name]
        else:
            raise NameError(name + ' is not defined.')


    # list all currently loaded modules and defined classes, functions and variables
    def list_variables(self):
        # create lists of modules, classes, functions and variables
        modules = []
        classes = []
        functions = []
        variables = []
        # iterate over dictionary to and put modules, classes, functions and variables in their respective lists
        for key, value in dict(self._exec_env).items():
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
                value = self.object_to_string(value)
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
    def reset(self):
        # reset environment by emptying variable definitions
        self._exec_env = {}


    # returns true if auto complete is available, false otherwise
    def has_auto_complete(self):
        return _jedi_available


    # returns a list of auto suggestions for the given code at the given cursor position
    def auto_complete(self, source_code, line, column):
        response = []
        if self.has_auto_complete():
            # get possible completions by using Jedi and providing the source code, and the cursor position
            # note: the line number (argument 2) gets incremented by 1 since Jedi's line numbering starts at 1
            completions = jedi.Script(source_code, line + 1, column, None).completions()
            # extract interesting information
            for index, completion in enumerate(completions):
                response.append({'name': completion.name, 'type': completion.type, 'doc': completion.docstring()})
        return response


    # get the type of a column (fails if multiple types are found)
    def column_type(self, data_frame, column_name):
        col_type = None
        for cell in data_frame[column_name]:
            if not self.is_missing(cell):
                if col_type is not None:
                    if not self.types_are_equivalent(type(cell), col_type):
                        raise ValueError('More than one type in column ' + str(column_name) + '. Found '
                                         + col_type.__name__ + ' and ' + type(cell).__name__)
                else:
                    col_type = type(cell)
        return col_type


    # get the type of a list column (fails if multiple types are found)
    def list_column_type(self, data_frame, column_name):
        col_type = None
        for list_cell in data_frame[column_name]:
            if list_cell is not None:
                for cell in list_cell:
                    if not self.is_missing(cell):
                        if col_type is not None:
                            if not self.types_are_equivalent(type(cell), col_type):
                                raise ValueError('More than one type in column ' + str(column_name) + '. Found '
                                                 + col_type.__name__ + ' and ' + type(cell).__name__)
                        else:
                            col_type = type(cell)
        return col_type

    # Returns the first object in a pandas DataFrame column that does not represent
    # a missing type (None, NaN or NaT)
    def first_valid_object(self, data_frame, column_name):
        for cell in data_frame[column_name]:
            if not self.is_missing(cell):
                return cell
        return None

    # Returns the first object contained in a list in a pandas DataFrame column that 
    # does not represent a missing type (None, NaN or NaT)
    def first_valid_list_object(self, data_frame, column_name):
        for list_cell in data_frame[column_name]:
            if list_cell is not None:
                for cell in list_cell:
                    if not self.is_missing(cell):
                        return cell
        return None


    # checks if the two given types are equivalent based on the equivalence list and the equivalence of numpy types to
    # python types
    def types_are_equivalent(self, type_1, type_2):
        for pair in EQUIVALENT_TYPES:
            if type_1 is pair[0] and type_2 is pair[1]:
                return True
            if type_1 is pair[1] and type_2 is pair[0]:
                return True
        if self.is_collection(type_1) or self.is_collection(type_2):
            return type_1 is type_2
        if self.is_numpy_type(type_1) or self.is_numpy_type(type_2):
            return numpy.issubdtype(type_1, type_2) and numpy.issubdtype(type_2, type_1)
        else:
            return type_1 is type_2


    # checks if the given type is a collection type
    def is_collection(self, data_type):
        return data_type is list or data_type is set or data_type is dict or data_type is tuple


    # checks if the given type is a numpy type
    def is_numpy_type(self, data_type):
        return data_type.__module__ == numpy.__name__


    # checks if the given value is None, NaN or NaT
    def is_missing(self, value):
        return value is None or self.is_nat(value) or self.is_nan(value)


    # checks if the given value is NaT
    def is_nat(self, value):
        if _tslib_available:
            return value is NaT
        else:
            return False


    # checks if the given value is NaN
    def is_nan(self, value):
        try:
            return math.isnan(value)
        except BaseException:
            return False

    # gets the name of an object's type.
    # NOTE: the name of an object's type is not the fully qualified one returned by the type()
    # function but just the last pice of it (e.g. 'time' instead of 'datetime.time')
    def get_type_string(self, data_object):
        if hasattr(data_object, '__module__'):
            return data_object.__module__ + '.' + data_object.__class__.__name__
        else:
            return data_object.__class__.__name__

    # Load a python module from a source file.
    # @param path    the path to the source file (string)
    # @return the module loaded from the specified path
    def load_module_from_path(self, path):
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
    
    #Convert data_object to a string representation
    def object_to_string(self, data_object):
        if _python3:
            try:
                return str(data_object)
            except Exception:
                return ''
        else:
            try:
                return unicode(data_object)
            except UnicodeDecodeError:
                return '(base64 encoded)\n' + base64.b64encode(data_object)
            except Exception:
                return ''

            


    # Serialize all cells in the provided data frame to a bytes representation (inplace).
    # @param data_frame          a pandas.DataFrame containing columns to serialize
    # @param column_serializers  dict containing column names present in data_frame as keys and serializer_ids as values.
    #                            A serializer_id should be the id of the java extension point on which the serializer is
    #                            registered. Each column identified by the dict keys is serialized using the serializer
    #                            provided by the TypeExtensionManager for the given serializer_id.
    def serialize_objects_to_bytes(self, data_frame, column_serializers):
        for column in column_serializers:
            serializer = self._type_extension_manager.get_serializer_by_id(column_serializers[column])
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
                value = data_frame[column][data_frame.index[i]] 
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


    # Deserialize all cells in the provided data frame from a bytes representation (inplace).
    # @param data_frame          a pandas.DataFrame containing columns to deserialize
    # @param column_serializers  dict containing column names present in data_frame as keys and deserializer_ids as values.
    #                            A deserializer_id should be the id of the java extension point on which the deserializer is
    #                            registered. Each column identified by the dict keys is deserialized using the deserializer
    #                            provided by the TypeExtensionManager for the given deserializer_id.
    def deserialize_from_bytes(self, data_frame, column_serializers):
        #print('Data frame: ' + str(data_frame) + '\nserializers: ' + str(column_serializers) + '\n')
        for column in column_serializers:
            deserializer = self._type_extension_manager.get_deserializer_by_id(column_serializers[column])
            for i in range(len(data_frame)):
                if debug_util.is_debug_enabled():
                    lastp = -1
                    if (i * 100/len(data_frame)) % 5 == 0 and int(i * 100/len(data_frame)) != lastp:
                        debug_util.debug_msg(str(i * 100/len(data_frame)) + ' percent done (deserialize)')
                        lastp = int(i * 100/len(data_frame))
                col_idx = data_frame.columns.get_loc(column)
                # Using bracket acessor is necessary here for ensuring that there are
                # no unwanted type conversions
                value = data_frame[column][data_frame.index[i]] 
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
    def read_size(self):
        data = bytearray()
        while len(data) < 4:
            data.extend(self._connection.recv(4))
        return struct.unpack('>L', data)[0]


    # read the next data from the input stream
    def read_data(self):
        size = self.read_size()
        data = bytearray()
        while len(data) < size:
            data.extend(self._connection.recv(size))
        return data


    # writes the given size as 4 byte integer to the output stream
    def write_size(self, size):
        writer = self._connection.makefile('wb', 4)
        writer.write(struct.pack('>L', size))
        writer.flush()
        #_connection.sendall(struct.pack('>L', size))


    # writes the given data to the output stream
    def write_data(self, data):
        self.write_size(len(data))
        self._connection.sendall(data)


    # writes an empty message
    def write_dummy(self):
        self.write_size(0)


    def read_integer(self):
        return struct.unpack('>L', self.read_data())[0]


    def write_integer(self, integer):
        self.write_data(struct.pack('>L', integer))


    def read_string(self):
        return self.read_data().decode('utf-8')


    def write_string(self, string):
        self.write_data(bytearray(string, 'utf-8'))


    def read_bytearray(self):
        return bytearray(self.read_data())


    def write_bytearray(self, data_bytes):
        self.write_data(data_bytes)


    def read_response(self, msg):
        command_or_reponse = self.read_string()
        while not (command_or_reponse == msg._cmd + "_response"):
            self.run_command(command_or_reponse)
            command_or_reponse = self.read_string()
        return msg.process_response(self.read_data().decode('utf-8'))


    # Write a PythonToJavaMessage object
    def write_message(self, msg):
        if not issubclass(type(msg), PythonToJavaMessage):
            raise TypeError("write_message was called with an object of a type not inheriting PythonToJavaMessage!")
        self.write_data(msg.to_string().encode('utf-8'))
        if msg.is_data_request():
            return self.read_response(msg)


    # Get the {@link Simpletype} of a column in the passed dataframe and the serializer_id
    # if available (only interesting for extension types that are transferred as bytes).
    # @param data_frame    the dataframe containing the columns to evaluate
    # @param column_name   the name of the column in data_frame to evaluate
    # @return tuple containing the {@link SimpleType} and the serializer_id (or None) of the column
    def simpletype_for_column(self, data_frame, column_name):
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
            elif data_frame[column_name].dtype == 'double' or self.column_type(data_frame, column_name) == float:
                simple_type = Simpletype.DOUBLE
            else:
                col_type = self.column_type(data_frame, column_name)
                if col_type is None:
                    # column with only missing values, make it string
                    simple_type = Simpletype.STRING
                elif self.types_are_equivalent(col_type, bool):
                    simple_type = Simpletype.BOOLEAN
                elif col_type is list or col_type is set:
                    is_set = col_type is set
                    list_col_type = self.list_column_type(data_frame, column_name)
                    if list_col_type is None:
                        # column with only missing values, make it string
                        if is_set:
                            simple_type = Simpletype.STRING_SET
                        else:
                            simple_type = Simpletype.STRING_LIST
                    elif self.types_are_equivalent(list_col_type, bool):
                        if is_set:
                            simple_type = Simpletype.BOOLEAN_SET
                        else:
                            simple_type = Simpletype.BOOLEAN_LIST
                    elif self.types_are_equivalent(list_col_type, int):
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
                    elif self.types_are_equivalent(list_col_type, float):
                        if is_set:
                            simple_type = Simpletype.DOUBLE_SET
                        else:
                            simple_type = Simpletype.DOUBLE_LIST
                    elif self.types_are_equivalent(list_col_type, str):
                        if is_set:
                            simple_type = Simpletype.STRING_SET
                        else:
                            simple_type = Simpletype.STRING_LIST
                    else:
                        type_string = self.get_type_string(self.first_valid_list_object(data_frame, column_name))
                        column_serializer = self._type_extension_manager.get_serializer_id_by_type(type_string)
                        if column_serializer is None:
                            raise ValueError('Column ' + str(column_name) + ' has unsupported type ' + type_string)
                        if is_set:
                            simple_type = Simpletype.BYTES_SET
                        else:
                            simple_type = Simpletype.BYTES_LIST
                elif self.types_are_equivalent(col_type, str):
                    simple_type = Simpletype.STRING
                elif self.types_are_equivalent(col_type, bytes) or self.types_are_equivalent(col_type, bytearray):
                    simple_type = Simpletype.BYTES
                else:
                    type_string = self.get_type_string(self.first_valid_object(data_frame, column_name))
                    column_serializer = self._type_extension_manager.get_serializer_id_by_type(type_string)
                    if column_serializer is None:
                        raise ValueError('Column ' + str(column_name) + ' has columntype "' 
                                         + str(data_frame[column_name].dtype) 
                                         + '" although the first nonnull element has type "' 
                                         + type_string + '". Mixed types in the column cannot be ruled out.'
                                         + ' You may convert the column type manually using the pandas.Series.astype method.')
                    simple_type = Simpletype.BYTES
        return simple_type, column_serializer

    # Convert a value to a given {@link Simpletype}.
    # @param value        a value
    # @param simpletype   the {@link Simpletype} to convert to
    def value_to_simpletype_value(self, value, simpletype):
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

class CommandHandler(object):
    def __init__(self, command):
        self._command = command
        
    def has_command(self, command):
        return self._command == command
        
    def execute(self, kernel):
        raise NotImplementedError("Abstract class CommandHandler does not provide an Implementation for execute().")
    
class ExecuteCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'execute'
        
    def execute(self, kernel):
        source_code = kernel.read_string()
        debug_util.debug_msg('executing: ' + source_code + '\n')
        output, error = kernel.execute(source_code)
        debug_util.debug_msg('executing done!')
        kernel.write_string(output)
        kernel.write_string(error)
        
class PutFlowVariablesCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'putFlowVariables'
        
    def execute(self, kernel):
        flow_variables = collections.OrderedDict()
        name = kernel.read_string()
        data_bytes = kernel.read_bytearray()
        data_frame = kernel.bytes_to_data_frame(data_bytes)
        kernel.fill_flow_variables_from_data_frame(flow_variables, data_frame)
        kernel.put_variable(name, flow_variables)
        kernel.write_dummy()
        
class GetFlowVariablesCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getFlowVariables'
        
    def execute(self, kernel):
        name = kernel.read_string()
        current_variables = kernel.get_variable(name)
        data_frame = kernel.flow_variables_dict_to_data_frame(current_variables)
        data_bytes = kernel.data_frame_to_bytes(data_frame)
        kernel.write_bytearray(data_bytes)
        
class PutTableCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'putTable'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_bytes = kernel.read_bytearray()
        data_frame = kernel.bytes_to_data_frame(data_bytes)
        kernel.put_variable(name, data_frame)
        kernel.write_message(SuccessMessage())
        
class AppendToTableCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'appendToTable'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_bytes = kernel.read_bytearray()
        data_frame = kernel.bytes_to_data_frame(data_bytes)
        kernel.append_to_table(name, data_frame)
        kernel.write_message(SuccessMessage())
        
class GetTableSizeCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getTableSize'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_frame = kernel.get_variable(name)
        kernel.write_integer(len(data_frame))
        
class GetTableCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getTable'
        
    def execute(self, kernel):
        debug_util.debug_msg('getTable\n')
        name = kernel.read_string()
        data_frame = kernel.get_variable(name)
        data_bytes = kernel.data_frame_to_bytes(data_frame)
        kernel.write_message(SuccessMessage())
        kernel.write_bytearray(data_bytes)
        
class GetTableChunkCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getTableChunk'
        
    def execute(self, kernel):
        debug_util.debug_msg('getTableChunk\n')
        name = kernel.read_string()
        start = kernel.read_integer()
        end = kernel.read_integer()
        data_frame = kernel.get_variable(name)
        import pandas
        if type(data_frame) != pandas.core.frame.DataFrame:
            raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame)) + "\nPlease make sure your output_table is a pandas.DataFrame.")
        data_frame_chunk = data_frame[start:end+1]
        data_bytes = kernel.data_frame_to_bytes(data_frame_chunk, start)
        kernel.write_message(SuccessMessage())
        kernel.write_bytearray(data_bytes)
        
class ListVariablesCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'listVariables'
        
    def execute(self, kernel):
        variables = kernel.list_variables()
        data_frame = DataFrame(variables)
        data_bytes = kernel.data_frame_to_bytes(data_frame)
        kernel.write_bytearray(data_bytes)
        
class ResetCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'reset'
        
    def execute(self, kernel):
        kernel.reset()
        kernel.write_dummy()
        
class HasAutoCompleteCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'hasAutoComplete'
        
    def execute(self, kernel):
        if kernel.has_auto_complete():
            value = 1
        else:
            value = 0
        kernel.write_integer(value)
        
class AutoCompleteCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'autoComplete'
        
    def execute(self, kernel):
        source_code = kernel.read_string()
        line = kernel.read_integer()
        column = kernel.read_integer()
        suggestions = kernel.auto_complete(source_code, line, column)
        data_frame = DataFrame(suggestions)
        data_bytes = kernel.data_frame_to_bytes(data_frame)
        kernel.write_bytearray(data_bytes)
        
class GetImageCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getImage'
        
    def execute(self, kernel):
        name = kernel.read_string()
        image = kernel.get_variable(name)
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
        kernel.write_bytearray(data_bytes)
        
class GetObjectCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getObject'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_object = kernel.get_variable(name)
        o_bytes = bytearray(pickle.dumps(data_object))
        o_type = type(data_object).__name__
        o_representation = kernel.object_to_string(data_object)
        data_frame = DataFrame([{'bytes': o_bytes, 'type': o_type, 'representation': o_representation}])
        data_bytes = kernel.data_frame_to_bytes(data_frame)
        kernel.write_bytearray(data_bytes)
        
class PutObjectCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'putObject'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_bytes = kernel.read_bytearray()
        data_object = pickle.loads(data_bytes)
        kernel.put_variable(name, data_object)
        kernel.write_dummy()
        
class AddSerializerCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'addSerializer'
        
    def execute(self, kernel):
        s_id = kernel.read_string()
        s_type = kernel.read_string()
        s_path = kernel.read_string()
        kernel._type_extension_manager.add_serializer(s_id, s_type, s_path)
        kernel.write_dummy()
        
class AddDeserializerCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'addDeserializer'
        
    def execute(self, kernel):
        d_id = kernel.read_string()
        d_path = kernel.read_string()
        kernel._type_extension_manager.add_deserializer(d_id, d_path)
        kernel.write_dummy()
        
class ShutdownCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'shutdown'
        
    def execute(self, kernel):
        kernel._cleanup()
        exit()
        
class PutSqlCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'putSql'
        
    def execute(self, kernel):
        name = kernel.read_string()
        data_bytes = kernel.read_bytearray()
        data_frame = kernel.bytes_to_data_frame(data_bytes)
        db_util = DBUtil(data_frame)
        kernel._exec_env[name] = db_util
        kernel._cleanup_object_names.append(name)
        kernel.write_dummy()
        
class GetSqlCommandHandler(CommandHandler):
    def __init__(self):
        self._command = 'getSql'
        
    def execute(self, kernel):
        name = kernel.read_string()
        db_util = kernel.get_variable(name)
        db_util._writer.commit()
        query = db_util.get_output_query()
        kernel.write_string(query)

# Reads the custom model directories (registred via the org.knime.python.modules
# extension point) as comma separated list and adds them to the pythonpath
class SetCustomModulePathsHandler(CommandHandler):
    def __init__(self):
        self._command = 'setCustomModulePaths'
        
    def execute(self, kernel):
        path = kernel.read_string()
        sys.path.append(path)
        kernel.write_dummy()

if __name__=="__main__":
    # Uncomment below and comment the run() call for profiling
    # See https://docs.python.org/3/library/profile.html on how to interpet the result
    #import cProfile
    #profilepath = os.path.join(os.path.expanduser('~'), 'profileres.txt')
    #cProfile.run('kernel = PythonKernel(); kernel.run()', filename=profilepath)
    kernel = PythonKernel()
    kernel.run()
