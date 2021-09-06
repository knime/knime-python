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

import atexit
import base64
import os
import shutil
import tempfile

import pandas

import PythonUtils

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

_types_ = None
_eval_types_ = None
_bytes_types_ = None

_temp_dir = None


# Initialize the enum of known type ids
# @param types     the enum of known type ids
def init(types):
    global _types_, _eval_types_, _bytes_types_
    _types_ = types
    _eval_types_ = {_types_.BOOLEAN_LIST, _types_.BOOLEAN_SET, _types_.INTEGER_LIST,
                    _types_.INTEGER_SET, _types_.LONG_LIST, _types_.LONG_SET,
                    _types_.DOUBLE_LIST, _types_.DOUBLE_SET, _types_.STRING_LIST,
                    _types_.STRING_SET, _types_.BYTES_LIST, _types_.BYTES_SET}
    _bytes_types_ = {_types_.BYTES, _types_.BYTES_LIST, _types_.BYTES_SET}


# Get the column names of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_names_from_bytes(data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    try:
        data_frame = pandas.read_csv(in_file, index_col=0, nrows=0, skiprows=2)
    except ValueError:
        data_frame = pandas.DataFrame()
    finally:
        in_file.close()
    return data_frame.columns.tolist()


# Get the column types of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_types_from_bytes(data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    try:
        types = in_file.readline().strip()[2:].split(',')
        if types == ['']:
            types = []
        column_types = []
        for i in range(len(types)):
            col_type_id = int(types[i])
            column_types.append(col_type_id)
    finally:
        in_file.close()
    return column_types


# Get the serializer ids (meaning the java extension point id of the serializer)
# of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_serializers_from_bytes(data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    try:
        types = in_file.readline().strip()[2:].split(',')
        serializers_line = in_file.readline().strip()[2:].split(',')
        serializers = {}
        for part in serializers_line:
            if part != '':
                key_value = part.split('=')
                serializers[key_value[0]] = key_value[1]
    finally:
        in_file.close()
    return serializers


# Read the CSV serialized data into a pandas.DataFrame.
# Delete the temporary CSV file afterwards.
# @param table        a {@link ToPandasTable} wrapping the data frame and 
#                     managing the deserialization of extension types
# @param data_bytes   the serialized path to the temporary CSV file
def bytes_into_table(table, data_bytes):
    path = data_bytes.decode('utf-8')
    try:
        with open(path, 'rb') as in_file:
            types = in_file.readline().decode('utf-8').strip()[2:].split(',')
            if types == ['']:
                types = []
            serializers_line = in_file.readline().decode('utf-8').strip()[2:].split(',')
            try:
                # Includes the "column header" of the row key (= index) column. Reading the column headers here and
                # explicitly passing them to the actual reading of the table fixes problems with empty tables (i.e.,
                # both tables without columns and tables without rows).
                names = pandas.read_csv(in_file, index_col=False, nrows=0).columns.tolist()
            except ValueError:
                names = []
            in_file.seek(0)
            try:
                data_frame = pandas.read_csv(in_file, header=None, names=names, index_col=0, skiprows=3,
                                             na_values=['MissingCell'],
                                             keep_default_na=False)
            except ValueError:
                data_frame = pandas.DataFrame()
            # Remove header of index column. We only work on "real" columns from here on.
            if len(names) > 0:
                names = names[1:]
            for i in range(len(types)):
                col_type_id = int(types[i])
                if col_type_id in _eval_types_:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        if str(data_frame[names[i]][index]) != 'nan':
                            data_frame.at[index, names[i]] = eval(data_frame[names[i]][index])
                        else:
                            data_frame.at[index, names[i]] = None
                if col_type_id == _types_.BYTES:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        if str(data_frame[names[i]][index]) != 'nan':
                            data_frame.at[index, names[i]] = base64.b64decode(data_frame[names[i]][index])
                        else:
                            data_frame.at[index, names[i]] = None
                elif col_type_id == _types_.BYTES_LIST:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        base64_list = data_frame[names[i]][index]
                        if base64_list is not None:
                            bytes_list = []
                            for k in range(len(base64_list)):
                                bytes_value = base64_list[k]
                                if bytes_value:
                                    bytes_list.append(base64.b64decode(bytes_value))
                                else:
                                    bytes_list.append(None)
                            data_frame.at[index, names[i]] = bytes_list
                elif col_type_id == _types_.BYTES_SET:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        base64_set = data_frame[names[i]][index]
                        if base64_set is not None:
                            bytes_set = set()
                            for value in base64_set:
                                if value:
                                    bytes_set.add(base64.b64decode(value))
                                else:
                                    bytes_set.add(None)
                            data_frame.at[index, names[i]] = bytes_set
                elif col_type_id == _types_.DOUBLE:
                    data_frame.iloc[:, i] = data_frame.iloc[:, i].astype('float', copy=False)
                # Check if column contains only missing values. If so make sure dtype is object.
                elif col_type_id == _types_.STRING and data_frame.iloc[:, i].dtype != 'object' and data_frame.iloc[:,
                                                                                                   i].isnull().all():
                    data_frame.iloc[:, i] = [None] * len(data_frame)
            table._data_frame = data_frame
    finally:
        PythonUtils.invoke_safely(None, os.remove, [path])


# Serialize a pandas.DataFrame into a temporary CSV file.
# Return the path to the created file as bytearray.
# @param table    a {@link FromPandasTable} wrapping the data frame and 
#                 managing the serialization of extension types 
def table_to_bytes(table):
    global _temp_dir
    if _temp_dir is None or not os.path.exists(_temp_dir):
        _temp_dir = tempfile.mkdtemp(prefix='knime-python-')
        # Delete temporary directory upon Python shutdown.
        atexit.register(close)
    fd, path = tempfile.mkstemp(suffix='.csv', prefix='python-to-java-', dir=_temp_dir, text=True)
    try:
        os.close(fd)
        with open(path, 'wb') as out_file:
            types_line = '#'
            needs_copy = False
            types = []
            for i in range(table.get_number_columns()):
                col_type_id = table.get_type(i)
                types.append(col_type_id)
                if col_type_id in _bytes_types_:
                    needs_copy = True
                types_line += ',' + str(col_type_id)
            serializers_line = '#'
            column_serializers = table.get_column_serializers()
            for serializer_id in column_serializers:
                serializers_line += ',' + serializer_id + '=' + column_serializers[serializer_id]
            out_file.write((types_line + '\n').encode('utf-8'))
            out_file.write((serializers_line + '\n').encode('utf-8'))
            data_frame = table._data_frame
            if needs_copy:
                data_frame = data_frame.copy()
            names = data_frame.columns.tolist()
            out_buffer = StringIO()
            for i in range(len(types)):
                col_type_id = int(types[i])
                if col_type_id == _types_.BYTES:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        if data_frame[names[i]][index] is not None:
                            data_frame.at[index, names[i]] = base64.b64encode(data_frame[names[i]][index])
                elif col_type_id == _types_.BYTES_LIST:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        bytes_list = data_frame[names[i]][index]
                        if bytes_list is not None:
                            base64_list = []
                            for k in range(len(bytes_list)):
                                bytes_value = bytes_list[k]
                                if bytes_value:
                                    base64_list.append(base64.b64encode(bytes_value))
                                else:
                                    base64_list.append(None)
                            data_frame.at[index, names[i]] = base64_list
                elif col_type_id == _types_.BYTES_SET:
                    for j in range(len(data_frame)):
                        index = data_frame.index[j]
                        bytes_set = data_frame[names[i]][index]
                        if bytes_set is not None:
                            base64_set = set()
                            for value in bytes_set:
                                if value:
                                    base64_set.add(base64.b64encode(value))
                                else:
                                    base64_set.add(None)
                            data_frame.at[index, names[i]] = base64_set
            data_frame.to_csv(out_buffer, na_rep='MissingCell', float_format="%.17g")
            out_file.write(out_buffer.getvalue().encode('utf-8'))
        return bytearray(path, 'utf-8')
    except BaseException:
        PythonUtils.invoke_safely(None, os.remove, [path])
        raise


def close():
    global _temp_dir
    # Remove entire temporary directory.
    PythonUtils.invoke_safely(None, lambda p: shutil.rmtree(p, ignore_errors=True), [_temp_dir])
