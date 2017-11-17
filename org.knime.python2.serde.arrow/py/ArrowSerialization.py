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

import pandas
import tempfile
import os
import base64
import pyarrow
import json
import sys
import struct
import numpy as np
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import debug_util

_python3 = sys.version_info >= (3, 0)


_types_ = None
_pandas_native_types_ = None
_bytes_types_ = None

read_data_frame = None
read_types = []
read_serializers = {}
path_to_mmap = None


# Initialize the enum of known type ids
# @param types     the enum of known type ids
def init(types):
    global _types_, _pandas_native_types_, _bytes_types_
    _types_ = types
    _pandas_native_types_ = {_types_.INTEGER, _types_.LONG, _types_.DOUBLE,
                             _types_.STRING, _types_.BYTES, _types_.BOOLEAN}
    _bytes_types_ = {_types_.BYTES, _types_.BYTES_LIST, _types_.BYTES_SET}


# Get the column names of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_names_from_bytes(data_bytes):
    global read_data_frame, read_types
    path = data_bytes.decode('utf-8')
    if read_data_frame is None:
        deserialize_data_frame(path)
    return read_data_frame.columns.tolist()


# Get the column types of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_types_from_bytes(data_bytes):
    global read_data_frame, read_types
    path = data_bytes.decode('utf-8')
    if read_data_frame is None:
        deserialize_data_frame(path)
    return read_types


# Get the serializer ids (meaning the java extension point id of the serializer)
# of the table to create from the serialized data.
# @param data_bytes    the serialized path to the temporary CSV file
def column_serializers_from_bytes(data_bytes):
    global read_data_frame, read_types, read_serializers
    path = data_bytes.decode('utf-8')
    if read_data_frame is None:
        deserialize_data_frame(path)
    return read_serializers

# Read the CSV serialized data into a pandas.DataFrame.
# Delete the temporary CSV file afterwards.
# @param table        a {@link ToPandasTable} wrapping the data frame and 
#                     managing the deserialization of extension types
# @param data_bytes   the serialized path to the temporary CSV file
def bytes_into_table(table, data_bytes):
    global read_data_frame, read_types, read_serializers
    path = data_bytes.decode('utf-8')
    if read_data_frame is None:
        deserialize_data_frame(path)
    table._data_frame = read_data_frame
    read_data_frame = None
    read_types = []
    read_serializers = {}
                
# Generator function for collection columns of type Integer, Long, Double.
# @param arrowcolumn    the pyarrow.Column to extract the values from
# @param isset          are the column values sets or lists
# @param entry_len      the length of a single primitive inside every collection (e.g. 4 for Integer collections)
# @param format_char    the format char to pass to struct.unpack for the primitive type inside every collection (e.g. 'i' for Integer collections)
# @return collection type values
def collection_generator(arrowcolumn, isset, entry_len, format_char):
    for i in range(len(arrowcolumn.data.chunk(0))):
        if type(arrowcolumn.data.chunk(0)[i]) == pyarrow.lib.NAType:
            yield None
        else:
            py_obj = arrowcolumn.data.chunk(0)[i].as_py()
            #debug_util.breakpoint()
            #buffer: length(values) int32, values [int32], missing [bit]
            #get length(values)
            n_vals = struct.unpack(">i", py_obj[:4])[0]
            #get values
            if not isset:
                res = list(struct.unpack(">%d%s" % (n_vals, format_char), py_obj[4:4 + entry_len * n_vals]))
                #get missings and set corresponding value to None if required
                missings = np.fromstring(py_obj[4 + entry_len * n_vals:], dtype=np.uint8)
                for i in range(n_vals):
                    if missings[i // 8] & (1 << (i % 8)) == 0:
                        res[i] = None
                yield res
            else:
                if _python3:
                    hasMissing = (np.uint8(py_obj[4 + entry_len * n_vals]) == 0)
                else:
                    hasMissing = (py_obj[4 + entry_len * n_vals] == b'\x00')
                res = set(struct.unpack(">%d%s" % (n_vals, format_char), py_obj[4:4 + entry_len * n_vals]))
                #debug_util.breakpoint()
                if hasMissing:
                    res.add(None)
                yield res
                
# Generator function for collection columns of type String.
# @param arrowcolumn    the pyarrow.Column to extract the values from
# @param isset          are the column values sets or lists
# @return collection type values
def string_collection_generator(arrowcolumn, isset):
    for i in range(len(arrowcolumn.data.chunk(0))):
        if type(arrowcolumn.data.chunk(0)[i]) == pyarrow.lib.NAType:
            yield None
        else:
            py_obj = arrowcolumn.data.chunk(0)[i].as_py()
            #debug_util.breakpoint()
            #buffer: length(values) int32, values [int32], missing [bit]
            #get length(values)
            n_vals = struct.unpack(">i", py_obj[:4])[0]
            val_offset = 4 + 4 * (n_vals + 1)
            offsets = struct.unpack(">%di" % (n_vals + 1), py_obj[4:val_offset])
            #get values
            if not isset:
                res = [py_obj[val_offset + offsets[i]:val_offset + offsets[i + 1]].decode("utf-8") for i in range(n_vals)]
                #get missings and set corresponding value to None if required
                missings = np.fromstring(py_obj[val_offset + offsets[-1]:], dtype=np.uint8)
                for i in range(n_vals):
                    if missings[i // 8] & (1 << (i % 8)) == 0:
                        res[i] = None
                yield res
            else:
                if _python3:
                    hasMissing = (np.uint8(py_obj[val_offset + offsets[-1]]) == 0)
                else:
                    hasMissing = (py_obj[val_offset + offsets[-1]] == b'\x00')
                res = set(py_obj[val_offset + offsets[i]:val_offset + offsets[i + 1]].decode("utf-8") for i in range(n_vals))
                #debug_util.breakpoint()
                if hasMissing:
                    res.add(None)
                yield res
                
# Generator function for collection columns of type String.
# @param arrowcolumn    the pyarrow.Column to extract the values from
# @param isset          are the column values sets or lists
# @return collection type values
def bytes_collection_generator(arrowcolumn, isset):
    for i in range(len(arrowcolumn.data.chunk(0))):
        if type(arrowcolumn.data.chunk(0)[i]) == pyarrow.lib.NAType:
            yield None
        else:
            py_obj = arrowcolumn.data.chunk(0)[i].as_py()
            #debug_util.breakpoint()
            #buffer: length(values) int32, values [int32], missing [bit]
            #get length(values)
            n_vals = struct.unpack(">i", py_obj[:4])[0]
            val_offset = 4 + 4 * (n_vals + 1)
            offsets = struct.unpack(">%di" % (n_vals + 1), py_obj[4:val_offset])
            #get values
            if not isset:
                res = [py_obj[val_offset + offsets[i]:val_offset + offsets[i + 1]] for i in range(n_vals)]
                #get missings and set corresponding value to None if required
                missings = np.fromstring(py_obj[val_offset + offsets[-1]:], dtype=np.uint8)
                for i in range(n_vals):
                    if missings[i // 8] & (1 << (i % 8)) == 0:
                        res[i] = None
                yield res
            else:
                if _python3:
                    hasMissing = (np.uint8(py_obj[val_offset + offsets[-1]]) == 0)
                else:
                    hasMissing = (py_obj[val_offset + offsets[-1]] == b'\x00')
                res = set(py_obj[val_offset + offsets[i]:val_offset + offsets[i + 1]] for i in range(n_vals))
                #debug_util.breakpoint()
                if hasMissing:
                    res.add(None)
                yield res
                
# Generator function for Boolean collection columns.
# @param arrowcolumn    the pyarrow.Column to extract the values from
# @param isset          are the column values sets or lists
# @return collection type values
def boolean_collection_generator(arrowcolumn, isset):
    for i in range(len(arrowcolumn.data.chunk(0))):
        if type(arrowcolumn.data.chunk(0)[i]) == pyarrow.lib.NAType:
            yield None
        else:
            py_obj = arrowcolumn.data.chunk(0)[i].as_py()
            #buffer: length(values) int32, values [int32], missing [bit]
            #get length(values)
            n_vals = struct.unpack(">i", py_obj[:4])[0]
            val_ln = n_vals // 8 + (0 if n_vals % 8 == 0 else 1)
            missing_start = 4 + val_ln
            if _python3:
                rbytes = py_obj[4:missing_start]
            else:
                rbytes = np.fromstring(py_obj[4:missing_start], dtype='i1')
            #get values
            if not isset:
                res = [((rbytes[j // 8] & (1 << (j % 8))) > 0) for j in range(n_vals)] 
                #get missings and set corresponding value to None if required
                missings = np.fromstring(py_obj[missing_start:], dtype=np.uint8)
                for i in range(n_vals):
                    if missings[i // 8] & (1 << (i % 8)) == 0:
                        res[i] = None
                yield res
            else:
                if _python3:
                    hasMissing = (np.uint8(py_obj[missing_start]) == 0)
                else:
                    hasMissing = (py_obj[missing_start] == b'\x00')
                res = set(((rbytes[j // 8] & (1 << (j % 8))) > 0) for j in range(n_vals))
                if hasMissing:
                    res.add(None)
                yield res

# Deserialize the data contained in the specified file as pandas.DataFrame.
# The data frame is written to the global read_data_frame to avoid multiple
# deserialization attempts.
# @param path the path to the file containing the serialized byte data
def deserialize_data_frame(path):
    global read_data_frame, read_types, read_serializers, _pandas_native_types_, path_to_mmap
    path_to_mmap = path
    with pyarrow.OSFile(path, 'rb') as f:
        stream_reader = pyarrow.RecordBatchStreamReader(f)
        arrowtable = stream_reader.read_all()
        #metadata 
        pandas_metadata = json.loads(arrowtable.schema.metadata[b'pandas'].decode('utf-8'))
        names = []
        for col in pandas_metadata['columns']:
            names.append(col['name'])
            read_types.append(col['metadata']['type_id'])
            ser_id = col['metadata']['serializer_id']
            if ser_id != '':
                read_serializers[col['name']] = ser_id
                
        #debug_util.breakpoint()
        #data 
        read_data_frame = pandas.DataFrame()       
        for arrowcolumn in arrowtable.itercolumns():
            typeidx = names.index(arrowcolumn.name)
            coltype = read_types[typeidx]
            if coltype in _pandas_native_types_:
                dfcol = arrowcolumn.to_pandas()
            else:
                if coltype == _types_.INTEGER_LIST or coltype == _types_.INTEGER_SET:
                    dfcol = pandas.Series(collection_generator(arrowcolumn, coltype == _types_.INTEGER_SET, 4, 'i')) 
                elif coltype == _types_.LONG_LIST or coltype == _types_.LONG_SET:
                    dfcol = pandas.Series(collection_generator(arrowcolumn, coltype == _types_.LONG_SET, 8, 'q')) 
                elif coltype == _types_.DOUBLE_LIST or coltype == _types_.DOUBLE_SET:
                    dfcol = pandas.Series(collection_generator(arrowcolumn, coltype == _types_.DOUBLE_SET, 8, 'd')) 
                elif coltype == _types_.BOOLEAN_LIST or coltype == _types_.BOOLEAN_SET:
                    dfcol = pandas.Series(boolean_collection_generator(arrowcolumn, coltype == _types_.BOOLEAN_SET)) 
                elif coltype == _types_.STRING_LIST or coltype == _types_.STRING_SET:
                    dfcol = pandas.Series(string_collection_generator(arrowcolumn, coltype == _types_.STRING_SET)) 
                elif coltype == _types_.BYTES_LIST or coltype == _types_.BYTES_SET:
                    dfcol = pandas.Series(bytes_collection_generator(arrowcolumn, coltype == _types_.BYTES_SET)) 
                else:
                    raise KeyError('Type with id ' + str(coltype) + ' cannot be deserialized!')
            #Note: we only have one index column (the KNIME RowKeys)
            if arrowcolumn.name in pandas_metadata['index_columns']:
                indexcol = dfcol
            else:
                read_data_frame[arrowcolumn.name] = dfcol
                
        if not 'indexcol' in locals():  
            raise NameError('Variable indexcol has not been set properly, exiting!')
        
        if len(read_data_frame.columns) > 0:
            read_data_frame.set_index(keys=indexcol, inplace=True)
        else:
            read_data_frame = pandas.DataFrame(index=indexcol)

        #debug_util.breakpoint()
        #print('test')
    os.remove(path)
        

# Convert a simpletype to the corresponding pyarrow.DataType
# @param type a SimpleType
def to_pyarrow_type(type):
    if type == _types_.BOOLEAN:
        return pyarrow.bool_()
    elif type == _types_.INTEGER:
        return pyarrow.int32()
    elif type == _types_.LONG:
        return pyarrow.int64()
    elif type == _types_.DOUBLE:
        return pyarrow.float64()
    elif type == _types_.STRING:
        return pyarrow.string()
    else:
        return pyarrow.binary()

# Generator converting values in a list type column to a binary representation 
# having the format (length(values), values, missing_mask). Works on Integer,
# Long and Double lists.
# @param column      the column to convert (a pandas.Series)
# @param numpy_type  the numpy_type of the primitive collection entries
def binary_from_list_generator(column, numpy_type):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])
            missings = np.zeros(shape=((n_vals // 8) + (1 if n_vals % 8 != 0 else 0)), dtype=np.uint8)
            for idx in range(n_vals):
                if not column[j][idx] == None:
                    missings[idx // 8] += (1 << (idx % 8))
                else:
                    column[j][idx] = 0
            yield np.int32(n_vals).tobytes() + np.array(column[j], dtype=numpy_type).tobytes() + missings.tobytes()
            
# Generator converting values in a list type column to a binary representation 
# having the format (length(values), offsets, values, missing_mask). Works on 
# String lists.
# @param column      the column to convert (a pandas.Series)
def binary_from_string_list_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            #debug_util.breakpoint()
            n_vals = len(column[j])
            
            missings = np.zeros(shape=((n_vals // 8) + (1 if n_vals % 8 != 0 else 0)), dtype=np.uint8)
            for idx in range(n_vals):
                if not column[j][idx] == None:
                    missings[idx // 8] += (1 << (idx % 8))
                else:
                    column[j][idx] = ''
            
            offsets = np.zeros(n_vals + 1, dtype=np.int32)
            for i in range(n_vals):
                offsets[i + 1] = offsets[i] + len(column[j][i])
            # Get all strings in the current list as one bytelist of utf-8 encoded bytes
            if _python3:
                values = bytes(bts for elem in column[j] for bts in elem.encode("utf-8"))
                yield np.int32(n_vals).tobytes() + offsets.tobytes() + values + missings.tobytes()
            else:
                values = b''.join(bts for elem in column[j] for bts in elem.encode("utf-8"))
                yield np.int32(n_vals).tobytes() + offsets.tobytes() + values.encode("utf-8") + missings.tobytes()
            
            
            
# Generator converting values in a set type column to a binary representation 
# having the format (length(values), offsets, values, missing_mask). Works on 
# String sets.
# @param column      the column to convert (a pandas.Series)
def binary_from_string_set_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])

            hasMissing = np.uint8(1)
            if None in column[j]:
                hasMissing = np.uint8(0)
                column[j].discard(None)
                n_vals -= 1
                
            offsets = np.zeros(n_vals + 1, dtype=np.int32)
            for i, elem in enumerate(column[j]):
                offsets[i + 1] = offsets[i] + len(elem)
                
            # Get all strings in the current list as one bytelist of utf-8 encoded bytes
            if _python3:
                values = bytes(bts for elem in column[j] for bts in elem.encode("utf-8"))
                yield np.int32(n_vals).tobytes() + offsets.tobytes() + values + hasMissing.tobytes()
            else:
                values = b''.join(bts for elem in column[j] for bts in elem.encode("utf-8"))
                yield np.int32(n_vals).tobytes() + offsets.tobytes() + values.encode("utf-8") + hasMissing.tobytes()
            
# Generator converting values in a list type column to a binary representation 
# having the format (length(values), offsets, values, missing_mask). Works on 
# Bytes lists.
# @param column      the column to convert (a pandas.Series)
def binary_from_bytes_list_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])
           
            
            missings = np.zeros(shape=((n_vals // 8) + (1 if n_vals % 8 != 0 else 0)), dtype=np.uint8)
            for idx in range(n_vals):
                if not column[j][idx] == None:
                    missings[idx // 8] += (1 << (idx % 8))
                else:
                    column[j][idx] = b''
                          
            offsets = np.zeros(n_vals + 1, dtype=np.int32)
            for i in range(n_vals):
                offsets[i + 1] = offsets[i] + len(column[j][i])
            # Get all strings in the current list as one bytelist of utf-8 encoded bytes
            if _python3:
                values = bytes(bts for elem in column[j] for bts in elem)
            else:
                values = b''.join(bts for elem in column[j] for bts in elem)
            
            yield np.int32(n_vals).tobytes() + offsets.tobytes() + values + missings.tobytes()
            
# Generator converting values in a set type column to a binary representation 
# having the format (length(values), offsets, values, missing_mask). Works on 
# Bytes sets.
# @param column      the column to convert (a pandas.Series)
def binary_from_bytes_set_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])

            hasMissing = np.uint8(1)
            if None in column[j]:
                hasMissing = np.uint8(0)
                column[j].discard(None)
                n_vals -= 1
                
            offsets = np.zeros(n_vals + 1, dtype=np.int32)
            for i, elem in enumerate(column[j]):
                offsets[i + 1] = offsets[i] + len(elem)

            # Get all strings in the current list as one bytelist of utf-8 encoded bytes
            if _python3:
                values = bytes(bts for elem in column[j] for bts in elem)
            else:
                values = b''.join(bts for elem in column[j] for bts in elem)
            
            yield np.int32(n_vals).tobytes() + offsets.tobytes() + values + hasMissing.tobytes()

#Converts a list of booleans to a bit-representation            
def bool_to_bits_generator(cell):
    cur_byte = np.uint8(0)
    for i in range(len(cell)):
        if cell[i]:
            cur_byte |= (1 << (i%8))
        if i % 8 == 7:
            yield cur_byte
            cur_byte = np.uint8(0)
    if not (len(cell) % 8 == 0):
        yield cur_byte
            
# Generator converting values in a list type column to a binary representation 
# having the format (length(values), values, missing_mask). Works on Boolean
# lists.
# @param column      the column to convert (a pandas.Series)
def binary_from_boolean_list_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])
            val_len = (n_vals // 8) + (1 if n_vals % 8 != 0 else 0)
            missings = np.zeros(shape=(val_len), dtype=np.uint8)
            for idx in range(n_vals):
                if not column[j][idx] == None:
                    missings[idx // 8] += (1 << (idx % 8))
                else:
                    column[j][idx] = False
            
            if _python3:
                yield np.int32(n_vals).tobytes() + bytes(bool_to_bits_generator(column[j])) + missings.tobytes()
            else:
                ar = np.zeros(val_len, dtype=np.uint8)
                for i, el in enumerate(bool_to_bits_generator(column[j])): ar[i] = el
                yield np.int32(n_vals).tobytes() + ar.tobytes() + missings.tobytes()
            
# Generator converting values in a set type column to a binary representation 
# having the format (length(values), values, missing_mask)
# @param column      the column to convert (a pandas.Series)
# @param numpy_type  the numpy_type of the primitive collection entries
def binary_from_set_generator(column, numpy_type):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])
            hasMissing = np.uint8(1)
            if None in column[j]:
                hasMissing = np.uint8(0)
                column[j].discard(None)
                n_vals -= 1
            np_set = np.empty(len(column[j]), dtype=numpy_type)
            for i, elem in enumerate(column[j]): np_set[i] = elem
            yield np.int32(n_vals).tobytes() + np_set.tobytes() + hasMissing.tobytes()
            
# Generator converting values in a set type column to a binary representation 
# having the format (length(values), values, missing_mask). Works on Boolean
# sets.
# @param column      the column to convert (a pandas.Series)
def binary_from_boolean_set_generator(column):
    for j in range(len(column)):
        if column[j] == None:
            yield None
        else: 
            n_vals = len(column[j])
            val_len = (n_vals // 8) + (1 if n_vals % 8 != 0 else 0)
            hasMissing = np.uint8(1)
            if None in column[j]:
                hasMissing = np.uint8(0)
                column[j].discard(None)
                n_vals -= 1
            if _python3:
                yield np.int32(n_vals).tobytes() + bytes(bool_to_bits_generator(list(column[j]))) + hasMissing.tobytes()
            else:
                ar = np.zeros(val_len, dtype=np.uint8)
                for i, el in enumerate(bool_to_bits_generator(list(column[j]))): ar[i] = el
                yield np.int32(n_vals).tobytes() + ar.tobytes() + hasMissing.tobytes()

# Get the first element of the specified column that is not None.
# @param column a pandas.Series
def get_first_not_None(column):
    for elem in column:
        if not elem == None:
            return elem
    return None

# Serialize a pandas.DataFrame into a file
# Return the path to the created file as bytearray.
# @param table    a {@link FromPandasTable} wrapping the data frame and 
#                 managing the serialization of extension types 
def table_to_bytes(table):
    path = tempfile.mkstemp(suffix='.dat', prefix='arrow-memory-mapped', text=False)[1]
    try:
        
        #debug_util.breakpoint()
        #Python2 workaround for strings -> convert all to unicode
        if not _python3:
            for i in range(len(table._data_frame.columns)):
                if type(get_first_not_None(table._data_frame.iloc[:,i])) == str and table._column_types[i] == _types_.STRING:
                    for j in range(len(table._data_frame)):
                        table._data_frame.iloc[j,i] = unicode(table._data_frame.iloc[j,i])
            indexls = []
            for j in range(len(table._data_frame)):
                indexls.append(unicode(table._data_frame.index[j]))
            if indexls:
                table._data_frame.set_index(keys=pandas.Series(indexls), inplace=True)
        
        mp = pyarrow.MemoryPool(2**64)
        col_arrays = []
        col_names = []
        all_names = []
        missing_names = []

        col_arrays.append(pyarrow.Array.from_pandas(table._data_frame.index, type=to_pyarrow_type(_types_.STRING), memory_pool=mp))
        col_names.append("__index_level_0__")
        all_names.append("__index_level_0__")
        # Serialize the dataframe into a list of pyarrow.Array column by column
        for i in range(len(table._data_frame.columns)):
            #missing column ? -> save name and don't send any buffer for column
            if(table._data_frame.iloc[:,i].isnull().all()):
                missing_names.append(table.get_name(i))
                all_names.append(table.get_name(i))
                continue
            #Convert collection types to binary
            if table.get_type(i) == _types_.INTEGER_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_list_generator(table._data_frame.iloc[:,i], '<i4')))
            elif table.get_type(i) == _types_.LONG_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_list_generator(table._data_frame.iloc[:,i], '<i8')))
            elif table.get_type(i) == _types_.DOUBLE_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_list_generator(table._data_frame.iloc[:,i], '<f8')))
            elif table.get_type(i) == _types_.BOOLEAN_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_boolean_list_generator(table._data_frame.iloc[:,i])))
            elif table.get_type(i) == _types_.STRING_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_string_list_generator(table._data_frame.iloc[:,i])))
            elif table.get_type(i) == _types_.BYTES_LIST:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_bytes_list_generator(table._data_frame.iloc[:,i])))
            elif table.get_type(i) == _types_.INTEGER_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_set_generator(table._data_frame.iloc[:,i], '<i4')))
            elif table.get_type(i) == _types_.LONG_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_set_generator(table._data_frame.iloc[:,i], '<i8')))
            elif table.get_type(i) == _types_.DOUBLE_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_set_generator(table._data_frame.iloc[:,i], '<f8')))
            elif table.get_type(i) == _types_.BOOLEAN_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_boolean_set_generator(table._data_frame.iloc[:,i])))
            elif table.get_type(i) == _types_.STRING_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_string_set_generator(table._data_frame.iloc[:,i])))
            elif table.get_type(i) == _types_.BYTES_SET:
                col_arrays.append(pyarrow.Array.from_pandas(binary_from_bytes_set_generator(table._data_frame.iloc[:,i])))
            #Workaround until numpy typecasts are implemented in pyarrow 
            elif table.get_type(i) == _types_.INTEGER and table._data_frame.iloc[:,i].dtype == np.int64:
                col_arrays.append(pyarrow.Array.from_pandas(np.array(table._data_frame.iloc[:,i], dtype=np.int32), memory_pool=mp))
            #Workaround until fixed in pyarrow ... it is assumed that the first non-None object is bytearray if any
            elif table.get_type(i) == _types_.BYTES and type(get_first_not_None(table._data_frame.iloc[:,i])) == bytearray:
                col_arrays.append(pyarrow.Array.from_pandas(map(lambda x: x if x is None else bytes(x), table._data_frame.iloc[:,i]), memory_pool=mp))
            #create pyarrow.Array
            else:
                pa_type = to_pyarrow_type(table.get_type(i))
                #pyarrow.binary() type is not allowed as argument for type atm
                if pa_type == pyarrow.binary():
                    col_arrays.append(pyarrow.BinaryArray.from_pandas(table._data_frame.iloc[:,i], memory_pool=mp))
                else:
                    col_arrays.append(pyarrow.Array.from_pandas(table._data_frame.iloc[:,i], type=pa_type, memory_pool=mp))
            col_names.append(table.get_name(i))
            all_names.append(table.get_name(i))
        
        #Construct metadata
        custom_metadata = {"index_columns": [all_names[0]], 
                           "columns": [{"name": all_names[0], "metadata": {"serializer_id": "", "type_id": _types_.STRING}}], 
                           "missing_columns": missing_names, 
                           "num_rows": len(table._data_frame)}
        
        real_col_names = list(table._data_frame.columns)
        for name in all_names[1:]:
            col_idx = real_col_names.index(name)
            if table.get_type(col_idx) in [_types_.BYTES, _types_.BYTES_LIST, _types_.BYTES_SET]:
                custom_metadata['columns'].append({"name": name, "metadata": {"serializer_id": table.get_column_serializers().get(name, ""), "type_id": table.get_type(col_idx)}})
            else:
                custom_metadata['columns'].append({"name": name, "metadata": {"serializer_id": "", "type_id": table.get_type(col_idx)}})
        
        metadata = {b'ArrowSerializationLibrary': json.dumps(custom_metadata).encode('utf-8')}
        
        # Empty record batches are not supported, therefore add a dummy array if dataframe is empty
        if not col_arrays:
            col_arrays.append(pyarrow.array([], type=pyarrow.int32()))
            col_names.append('dummy')
        
        batch = pyarrow.RecordBatch.from_arrays(col_arrays, col_names)
          
        schema = batch.schema.remove_metadata()
        schema = schema.add_metadata(metadata)
        
        #Write data to file and return filepath
        with pyarrow.OSFile(path, 'wb') as f:
            stream_writer = pyarrow.RecordBatchStreamWriter(f, schema)
            stream_writer.write_batch(batch)
            stream_writer.close()
        return bytearray(path, 'utf-8')
    except Exception as error:
        os.remove(path)
        raise error
        