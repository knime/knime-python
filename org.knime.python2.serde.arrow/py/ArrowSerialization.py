# ------------------------------------------------------------------------
#  Copyright by KNIME GmbH, Konstanz, Germany
#  Website: http://www.knime.org; Email: contact@knime.org
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
        
def deserialize_data_frame(path):
    global read_data_frame, read_types, read_serializers, _pandas_native_types_
    with pyarrow.OSFile(path, 'rb') as f:
        stream_reader = pyarrow.StreamReader(f)
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
        #data 
        read_data_frame = pandas.DataFrame()       
        for arrowcolumn in arrowtable.itercolumns():
            typeidx = names.index(arrowcolumn.name)
            coltype = read_types[typeidx]
            if coltype in _pandas_native_types_:
                dfcol = arrowcolumn.to_pandas()
            else:
                raise Error()
            #Note: we only have one index column (the KNIME RowKeys)
            if arrowcolumn.name in pandas_metadata['index_columns']:
                indexcol = dfcol
            else:
                read_data_frame[arrowcolumn.name] = dfcol
                
        if not 'indexcol' in locals():  
            raise NameError('Variable indexcol has not been set properly, exiting!')
        
        read_data_frame.set_index(keys=indexcol, inplace=True)      
        #import debug_util
        #debug_util.breakpoint()
        #print('test')
        

# Serialize a pandas.DataFrame into a memory mapped file
# Return the path to the created memory mapped file as bytearray.
# @param table    a {@link FromPandasTable} wrapping the data frame and 
#                 managing the serialization of extension types 
def table_to_bytes(table):
    path = os.path.join(tempfile.gettempdir(), 'memory_mapped.dat')
    #for i in range(len(table._data_frame.columns)):
    #    if type(table._data_frame.iat[0,i]) == list:
    #        if type(table._data_frame.iat[0,i][0]) == str:
    #            for j in range(len(table._data_frame)):
    #                table._data_frame.iat[j,i] = str(table._data_frame.iat[j,i]).encode("utf-8")
    
    #Python2 workaround for strings -> convert all to unicode
    if not _python3:
        for i in range(len(table._data_frame.columns)):
            if type(table._data_frame.iloc[0,i]) == str and table._column_types[i] == _types_.STRING:
                for j in range(len(table._data_frame)):
                    table._data_frame.iloc[j,i] = unicode(table._data_frame.iloc[j,i])
        indexls = []
        for j in range(len(table._data_frame)):
            indexls.append(unicode(table._data_frame.index[j]))
        table._data_frame.set_index(keys=pandas.Series(indexls), inplace=True)
    #import debug_util
    #debug_util.breakpoint()
    
    batch = pyarrow.RecordBatch.from_pandas(table._data_frame)
    metadata = batch.schema.metadata
    pandas_metadata = json.loads(metadata[b'pandas'].decode('utf-8'))
    for column in pandas_metadata['columns']:
        try:
            col_idx = table.get_names().get_loc(column['name'])
            if table.get_type(col_idx) == _types_.BYTES:
                column['metadata'] = {"serializer_id": table.get_column_serializers()[column['name']], "type_id": table.get_type(col_idx)}
            else:
                column['metadata'] = {"serializer_id": '', "type_id": table.get_type(col_idx)}
        except:
            #Only index columns -> always string
            column['metadata'] = {"serializer_id": '', "type_id": _types_.STRING}
        
    metadata[b'pandas'] = json.dumps(pandas_metadata).encode('utf-8')
    schema = batch.schema.remove_metadata()
    schema = schema.add_metadata(metadata)
            
    with pyarrow.OSFile(path, 'wb') as f:
        stream_writer = pyarrow.StreamWriter(f, schema)
        stream_writer.write_batch(batch)
        stream_writer.close()
    return bytearray(path, 'utf-8')
