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

import numpy
from pandas import DataFrame
from pandas import Index

from PythonUtils import Simpletype


# Wrapper class for data that should be serialized using the serialization library.
# Manages the serialization of extension types to bytes before using the
# registered serialization library for serializing primitive types.
class FromPandasTable:
    # Constructor.
    # Serializes objects having a type that is registered via the knimetopython
    # extension point to a bytes representation and adjusts the dataframe index
    # to reflect KNIME standard row indexing if necessary.
    # @param data_frame        a pandas DataFrame containing the table to serialize
    # @param serializer        the serializer
    # @param start_row_number  the corresponding row number to the first row of the
    #                          dataframe. Differs from 0 as soon as a table chunk is
    #                          sent.
    def __init__(self, data_frame, serializer, start_row_number=0):
        self._data_frame = data_frame.copy()
        self._data_frame.columns = self._data_frame.columns.astype(str)
        self._column_types = []
        self._column_serializers = {}
        for i, column in enumerate(self._data_frame.columns):
            column_type, serializer_id = serializer.simpletype_for_column(self._data_frame, column)
            self._column_types.append(column_type)
            if serializer_id is not None:
                self._column_serializers[column] = serializer_id
        serializer.serialize_objects_to_bytes(self._data_frame, self._column_serializers)
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
        # value = self._data_frame.iat[row_index, column_index]
        # if value is None:
        #    return None
        # else:
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
    # @param serializer          the serializer
    def __init__(self, column_names, column_types, column_serializers, serializer):
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
        self._serializer = serializer

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
        self._serializer.deserialize_from_bytes(self._data_frame, self._column_serializers)
        return self._data_frame
