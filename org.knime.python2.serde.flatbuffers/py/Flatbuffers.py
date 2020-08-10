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

import numpy as np
from pandas import DataFrame

import flatbuffers
from knimetable import BooleanCollectionCell
from knimetable import BooleanCollectionColumn
from knimetable import BooleanColumn
from knimetable import ByteCell
from knimetable import ByteCollectionCell
from knimetable import ByteCollectionColumn
from knimetable import ByteColumn
from knimetable import Column
from knimetable import DoubleCollectionCell
from knimetable import DoubleCollectionColumn
from knimetable import DoubleColumn
from knimetable import IntCollectionColumn
from knimetable import IntColumn
from knimetable import IntegerCollectionCell
from knimetable import KnimeTable
from knimetable import LongCollectionCell
from knimetable import LongCollectionColumn
from knimetable import LongColumn
from knimetable import StringCollectionCell
from knimetable import StringCollectionColumn
from knimetable import StringColumn

_types_ = None


# Get the column types of the table to create from the serialized data.
# @param data_bytes    the data_bytes
def column_types_from_bytes(data_bytes):
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colTypes = []
    for j in range(0, table.ColumnsLength()):
        colTypes.append(table.Columns(j).Type())
    return colTypes


# Get the column names of the table to create from the serialized data.
# @param data_bytes    the data_bytes
def column_names_from_bytes(data_bytes):
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, table.ColNamesLength()):
        colNames.append(table.ColNames(j).decode('utf-8'))
    return colNames


# Get the serializer ids (meaning the java extension point id of the serializer)
# of the table to create from the serialized data.
# @param data_bytes    the data_bytes
def column_serializers_from_bytes(data_bytes):
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = column_names_from_bytes(data_bytes)

    serializers = {}
    for j in range(0, table.ColumnsLength()):
        if table.Columns(j).Type() == _types_.BYTES:
            serializers[colNames[j]] = table.Columns(j).ByteColumn().Serializer().decode('utf-8')
        elif table.Columns(j).Type() == _types_.BYTES_LIST:
            serializers[colNames[j]] = table.Columns(j).ByteListColumn().Serializer().decode('utf-8')
        elif table.Columns(j).Type() == _types_.BYTES_SET:
            serializers[colNames[j]] = table.Columns(j).ByteSetColumn().Serializer().decode('utf-8')

    return serializers


# Deserialize the data_bytes into a pandas.DataFrame.
# @param table        a {@link ToPandasTable} wrapping the data frame and 
#                     managing the deserialization of extension types
# @param data_bytes   the data_bytes
def bytes_into_table(table, data_bytes):
    knimeTable = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    rowIds = []

    for idx in range(0, knimeTable.RowIDsLength()):
        rowIds.append(knimeTable.RowIDs(idx).decode('utf-8'))

    colNames = []

    for j in range(0, knimeTable.ColNamesLength()):
        colNames.append(knimeTable.ColNames(j).decode('utf-8'))

    df = DataFrame(columns=colNames, index=rowIds)

    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)

        if col.Type() == _types_.INTEGER:
            colVec = col.IntColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.INTEGER_LIST:
            colVec = col.IntListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.INTEGER_SET:
            colVec = col.IntSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

        elif col.Type() == _types_.BOOLEAN:
            colVec = col.BooleanColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.BOOLEAN_LIST:
            colVec = col.BooleanListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.BOOLEAN_SET:
            colVec = col.BooleanSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

        elif col.Type() == _types_.LONG:
            colVec = col.LongColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.LONG_LIST:
            colVec = col.LongListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.LONG_SET:
            colVec = col.LongSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

        elif col.Type() == _types_.DOUBLE or col.Type() == _types_.FLOAT:
            colVec = col.DoubleColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.DOUBLE_LIST or col.Type() == _types_.FLOAT_LIST:
            colVec = col.DoubleListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.DOUBLE_SET or col.Type() == _types_.FLOAT_SET:
            colVec = col.DoubleSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

        elif col.Type() == _types_.STRING:
            colVec = col.StringColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.STRING_LIST:
            colVec = col.StringListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.STRING_SET:
            colVec = col.StringSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

        elif col.Type() == _types_.BYTES:
            colVec = col.ByteColumn()
            colVec.AddValuesAsColumn(df, j)

        elif col.Type() == _types_.BYTES_LIST:
            colVec = col.ByteListColumn()
            colVec.AddValuesAsColumn(df, j, True)

        elif col.Type() == _types_.BYTES_SET:
            colVec = col.ByteSetColumn()
            colVec.AddValuesAsColumn(df, j, False)

    table._data_frame = df


# Serialize a pands.DataFrame into bytes.
# @param table        a {@link FromPandasTable} wrapping the data frame and 
#                     managing the serialization of extension types
# @param data_bytes   the data_bytes
# @return the bytes
def table_to_bytes(table):
    try:

        builder = flatbuffers.Builder(1024)

        # Row IDs
        idx_col = [elem.encode('utf-8') for elem in table._row_indices]
        rowIdLength = list(map(len, idx_col))
        offListOff = builder.CreateByteVector(np.array(rowIdLength, dtype='i4').tobytes())
        strOff = builder.CreateByteVector(b''.join(idx_col))

        KnimeTable.KnimeTableStartRowIDsVector(builder, 2)
        builder.PrependInt32(offListOff)
        builder.PrependInt32(strOff)
        rowIdVecOffset = builder.EndVector(2)

        # Column Names
        colNameOffsets = []

        for colName in table.get_names():
            nameOffset = builder.CreateString(str(colName))
            colNameOffsets.append(nameOffset)

        KnimeTable.KnimeTableStartColNamesVector(builder, len(colNameOffsets))
        for colNameOffset in reversed(colNameOffsets):
            builder.PrependUOffsetTRelative(colNameOffset)
        colNameVecOffset = builder.EndVector(len(colNameOffsets))

        colOffsetList = []

        serializers = table._column_serializers

        for colIdx in range(0, table.get_number_columns()):
            col = table._data_frame.iloc[:, colIdx]
            if table.get_type(colIdx) == _types_.INTEGER:
                valVec = builder.CreateByteVector(np.array(col.values, dtype='i4').tobytes())

                IntColumn.IntColumnStart(builder)
                IntColumn.IntColumnAddValues(builder, valVec)
                colOffset = IntColumn.IntColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddIntColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.INTEGER_LIST:
                cellOffsets = []

                for valIdx in range(0, len(col)):
                    cellVec = 0
                    if col[valIdx] == None:
                        IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, 1)
                        builder.PrependInt32(-2147483648)
                        cellVec = builder.EndVector(1)
                    else:
                        cellMissing = []
                        IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, len(col[valIdx]))
                        for cellIdx in reversed(range(0, len(col[valIdx]))):
                            if col[valIdx][cellIdx] == None:
                                builder.PrependInt32(-2147483648)
                                cellMissing.append(True)
                            else:
                                builder.PrependInt32(col[valIdx][cellIdx])
                                cellMissing.append(False)
                        cellVec = builder.EndVector(len(col[valIdx]))

                        # the missing vector is already in reversed order
                        IntegerCollectionCell.IntegerCollectionCellStartMissingVector(builder, len(col[valIdx]))
                        for cellIdx in range(0, len(col[valIdx])):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(col[valIdx]))

                    IntegerCollectionCell.IntegerCollectionCellStart(builder)
                    IntegerCollectionCell.IntegerCollectionCellAddValue(builder, cellVec)
                    if col[valIdx] != None:
                        IntegerCollectionCell.IntegerCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))

                IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                IntCollectionColumn.IntCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                IntCollectionColumn.IntCollectionColumnStart(builder)
                IntCollectionColumn.IntCollectionColumnAddValues(builder, valVec)
                IntCollectionColumn.IntCollectionColumnAddMissing(builder, missVec)
                colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddIntListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.INTEGER_SET:
                cellOffsets = []

                for valIdx in range(0, len(col)):
                    addMissingValue = False;
                    if col[valIdx] == None:
                        IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, 1)
                        builder.PrependInt32(-2147483648)
                        cellVec = builder.EndVector(1)
                    else:
                        IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, len(col[valIdx]))
                        numElems = 0
                        for elem in col[valIdx]:
                            if elem == None:
                                addMissingValue = True
                            else:
                                builder.PrependInt32(elem)
                                numElems += 1
                        cellVec = builder.EndVector(numElems)

                    IntegerCollectionCell.IntegerCollectionCellStart(builder)
                    IntegerCollectionCell.IntegerCollectionCellAddValue(builder, cellVec)
                    IntegerCollectionCell.IntegerCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))

                IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                IntCollectionColumn.IntCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                IntCollectionColumn.IntCollectionColumnStart(builder)
                IntCollectionColumn.IntCollectionColumnAddValues(builder, valVec)
                IntCollectionColumn.IntCollectionColumnAddMissing(builder, missVec)
                colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddIntSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))


            elif table.get_type(colIdx) == _types_.BOOLEAN:
                missingVec = builder.CreateByteVector(col.isnull().values.tobytes())
                valVec = builder.CreateByteVector(col.fillna(False).astype('bool').values.tobytes())

                BooleanColumn.BooleanColumnStart(builder)
                BooleanColumn.BooleanColumnAddValues(builder, valVec)
                BooleanColumn.BooleanColumnAddMissing(builder, missingVec)
                colOffset = BooleanColumn.BooleanColumnEnd(builder)

                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddBooleanColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.BOOLEAN_LIST:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    if col[valIdx] == None:
                        BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, 1)
                        builder.PrependBool(True)
                        cellVec = builder.EndVector(1)
                    else:
                        BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                        cellMissing = []
                        for cellIdx in reversed(range(0, len(col[valIdx]))):
                            if col[valIdx][cellIdx] == None:
                                builder.PrependBool(False)
                                cellMissing.append(True)
                            else:
                                builder.PrependBool(col[valIdx][cellIdx])
                                cellMissing.append(False)
                        cellVec = builder.EndVector(len(col[valIdx]))
                        # the missing vector is already in reversed order
                        BooleanCollectionCell.BooleanCollectionCellStartMissingVector(builder, len(col[valIdx]))
                        for cellIdx in range(0, len(col[valIdx])):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(col[valIdx]))

                    BooleanCollectionCell.BooleanCollectionCellStart(builder)
                    BooleanCollectionCell.BooleanCollectionCellAddValue(builder, cellVec)
                    if col[valIdx] != None:
                        BooleanCollectionCell.BooleanCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))

                    #     valVec = builder.EndVector(len(cellOffsets))

                BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                BooleanCollectionColumn.BooleanCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                BooleanCollectionColumn.BooleanCollectionColumnStart(builder)
                BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder, valVec)
                BooleanCollectionColumn.BooleanCollectionColumnAddMissing(builder, missVec)
                colOffset = BooleanCollectionColumn.BooleanCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddBooleanListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.BOOLEAN_SET:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    addMissingValue = False

                    if col[valIdx] == None:
                        BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, 1)
                        builder.PrependBool(False)
                        cellVec = builder.EndVector(1)
                    else:
                        BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                        numElems = 0
                        for elem in col[valIdx]:
                            if elem == None:
                                addMissingValue = True
                            else:
                                builder.PrependBool(elem)
                                numElems += 1
                        cellVec = builder.EndVector(numElems)

                    BooleanCollectionCell.BooleanCollectionCellStart(builder)
                    BooleanCollectionCell.BooleanCollectionCellAddValue(builder, cellVec)
                    BooleanCollectionCell.BooleanCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))

                BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                BooleanCollectionColumn.BooleanCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                BooleanCollectionColumn.BooleanCollectionColumnStart(builder)
                BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder, valVec)
                BooleanCollectionColumn.BooleanCollectionColumnAddMissing(builder, missVec)
                colOffset = BooleanCollectionColumn.BooleanCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddBooleanSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))


            elif table.get_type(colIdx) == _types_.LONG:
                valVec = builder.CreateByteVector(np.array(col.values, dtype='i8').tobytes())

                LongColumn.LongColumnStart(builder)
                LongColumn.LongColumnAddValues(builder, valVec)
                colOffset = LongColumn.LongColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddLongColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.LONG_LIST:
                cellOffsets = []
                for valIdx in range(0, len(col)):

                    if col[valIdx] == None:
                        LongCollectionCell.LongCollectionCellStartValueVector(builder, 1)
                        builder.PrependInt64(-9223372036854775808)
                        cellVec = builder.EndVector(1)
                    else:
                        LongCollectionCell.LongCollectionCellStartValueVector(builder, len(col[valIdx]))
                        cellMissing = []
                        for cellIdx in reversed(range(0, len(col[valIdx]))):
                            if col[valIdx][cellIdx] == None:
                                builder.PrependInt64(-9223372036854775808)
                                cellMissing.append(True)
                            else:
                                builder.PrependInt64(col[valIdx][cellIdx])
                                cellMissing.append(False)
                        cellVec = builder.EndVector(len(col[valIdx]))

                        # the missing vector is already in reversed order
                        LongCollectionCell.LongCollectionCellStartMissingVector(builder, len(col[valIdx]))
                        for cellIdx in range(0, len(col[valIdx])):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(col[valIdx]))

                    LongCollectionCell.LongCollectionCellStart(builder)
                    LongCollectionCell.LongCollectionCellAddValue(builder, cellVec)
                    if col[valIdx] != None:
                        LongCollectionCell.LongCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))

                LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                LongCollectionColumn.LongCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                LongCollectionColumn.LongCollectionColumnStart(builder)
                LongCollectionColumn.LongCollectionColumnAddValues(builder, valVec)
                LongCollectionColumn.LongCollectionColumnAddMissing(builder, missVec)
                colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddLongListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.LONG_SET:
                cellOffsets = []
                for valIdx in range(0, len(col)):

                    if col[valIdx] == None:
                        LongCollectionCell.LongCollectionCellStartValueVector(builder, 1)
                        builder.PrependInt64(-9223372036854775808)
                        cellVec = builder.EndVector(1)
                    else:
                        addMissingValue = False
                        LongCollectionCell.LongCollectionCellStartValueVector(builder, len(col[valIdx]))
                        numElems = 0
                        for elem in col[valIdx]:
                            if elem == None:
                                addMissingValue = True
                            else:
                                builder.PrependInt64(elem)
                                numElems += 1
                        cellVec = builder.EndVector(numElems)

                    LongCollectionCell.LongCollectionCellStart(builder)
                    LongCollectionCell.LongCollectionCellAddValue(builder, cellVec)
                    LongCollectionCell.LongCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))

                LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                LongCollectionColumn.LongCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(cellOffsets))

                LongCollectionColumn.LongCollectionColumnStart(builder)
                LongCollectionColumn.LongCollectionColumnAddValues(builder, valVec)
                LongCollectionColumn.LongCollectionColumnAddMissing(builder, missVec)
                colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder)

                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddLongSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.DOUBLE or table.get_type(colIdx) == _types_.FLOAT:
                bytes = np.array(col.values, dtype='f8', copy=False).tobytes()

                valVec = builder.CreateByteVector(bytes)

                DoubleColumn.DoubleColumnStart(builder)
                DoubleColumn.DoubleColumnAddValues(builder, valVec)
                colOffset = DoubleColumn.DoubleColumnEnd(builder)

                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddDoubleColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.DOUBLE_LIST or table.get_type(colIdx) == _types_.FLOAT_LIST:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    if col[valIdx] == None:
                        DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, 1)
                        builder.PrependFloat64(float('NaN'))
                        cellVec = builder.EndVector(1)
                    else:
                        DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, len(col[valIdx]))
                        cellMissing = []
                        for cellIdx in reversed(range(0, len(col[valIdx]))):
                            if col[valIdx][cellIdx] == None:
                                builder.PrependFloat64(float('NaN'))
                                cellMissing.append(True)
                            else:
                                builder.PrependFloat64(col[valIdx][cellIdx])
                                cellMissing.append(False)
                        cellVec = builder.EndVector(len(col[valIdx]))

                        # the missing vector is already in reversed order
                        DoubleCollectionCell.DoubleCollectionCellStartMissingVector(builder, len(col[valIdx]))
                        for cellIdx in range(0, len(col[valIdx])):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(col[valIdx]))

                    DoubleCollectionCell.DoubleCollectionCellStart(builder)
                    DoubleCollectionCell.DoubleCollectionCellAddValue(builder, cellVec)
                    if col[valIdx] != None:
                        DoubleCollectionCell.DoubleCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))

                DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                DoubleCollectionColumn.DoubleCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(col))

                DoubleCollectionColumn.DoubleCollectionColumnStart(builder)
                DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder, valVec)
                DoubleCollectionColumn.DoubleCollectionColumnAddMissing(builder, missVec)
                colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddDoubleListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.DOUBLE_SET or table.get_type(colIdx) == _types_.FLOAT_SET:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    addMissingValue = False
                    if col[valIdx] == None:
                        DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, 1)
                        builder.PrependFloat64(float('NaN'))
                        cellVec = builder.EndVector(1)
                    else:
                        DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, len(col[valIdx]))
                        numElems = 0;
                        for elem in col[valIdx]:
                            if elem == None:
                                addMissingValue = True
                            else:
                                builder.PrependFloat64(elem)
                                numElems += 1
                        cellVec = builder.EndVector(numElems)

                    DoubleCollectionCell.DoubleCollectionCellStart(builder)
                    DoubleCollectionCell.DoubleCollectionCellAddValue(builder, cellVec)
                    DoubleCollectionCell.DoubleCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))

                DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                DoubleCollectionColumn.DoubleCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(col))

                DoubleCollectionColumn.DoubleCollectionColumnStart(builder)
                DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder, valVec)
                DoubleCollectionColumn.DoubleCollectionColumnAddMissing(builder, missVec)
                colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddDoubleSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.STRING:
                missingVec = builder.CreateByteVector(col.isnull().values.tobytes())
                col.fillna(value='', inplace=True)

                col = [elem.encode('utf-8') for elem in col]
                strLengths = list(map(len, col))
                offStrLengths = builder.CreateByteVector(np.array(strLengths, dtype='i4').tobytes())
                offStrBlob = builder.CreateByteVector(b''.join(col))

                StringColumn.StringColumnStartValuesVector(builder, 2)
                builder.PrependInt32(offStrLengths)
                builder.PrependInt32(offStrBlob)
                valVec = builder.EndVector(2)

                StringColumn.StringColumnStart(builder)
                StringColumn.StringColumnAddValues(builder, valVec)
                StringColumn.StringColumnAddMissing(builder, missingVec)
                colOffset = StringColumn.StringColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddStringColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.STRING_LIST:
                cellOffsets = []
                for valIdx in range(0, len(col)):

                    strOffsets = []
                    if col[valIdx] == None:
                        strOffsets.append(builder.CreateString("Missing Value"))
                    else:
                        cellMissing = []
                        for strIdx in range(0, len(col[valIdx])):
                            if col[valIdx][strIdx] == None:
                                strOffsets.append(builder.CreateString("Missing Value"))
                                cellMissing.append(True);
                            else:
                                strOffsets.append(builder.CreateString(col[valIdx][strIdx]))
                                cellMissing.append(False)

                                # the missing vector is *not* already in reversed order
                        StringCollectionCell.StringCollectionCellStartMissingVector(builder, len(col[valIdx]))
                        for cellIdx in reversed(range(0, len(col[valIdx]))):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(col[valIdx]))

                    StringCollectionCell.StringCollectionCellStartValueVector(builder, len(strOffsets))
                    for cellIdx in reversed(range(0, len(strOffsets))):
                        builder.PrependUOffsetTRelative(strOffsets[cellIdx])

                    cellVec = builder.EndVector(len(strOffsets))
                    StringCollectionCell.StringCollectionCellStart(builder)
                    StringCollectionCell.StringCollectionCellAddValue(builder, cellVec)
                    if col[valIdx] != None:
                        StringCollectionCell.StringCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))

                StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                StringCollectionColumn.StringCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missingVec = builder.EndVector(len(col))

                StringCollectionColumn.StringCollectionColumnStart(builder)
                StringCollectionColumn.StringCollectionColumnAddValues(builder, valVec)
                StringCollectionColumn.StringCollectionColumnAddMissing(builder, missingVec)
                colOffset = StringCollectionColumn.StringCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddStringListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.STRING_SET:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    strOffsets = []
                    if col[valIdx] == None:
                        strOffsets.append(builder.CreateString("Missing Value"))
                    else:
                        addMissingValue = False
                        for elem in col[valIdx]:
                            if elem == None:
                                addMissingValue = True
                            else:
                                strOffsets.append(builder.CreateString(elem))

                    StringCollectionCell.StringCollectionCellStartValueVector(builder, len(strOffsets))
                    for cellIdx in reversed(range(0, len(strOffsets))):
                        builder.PrependUOffsetTRelative(strOffsets[cellIdx])
                    cellVec = builder.EndVector(len(strOffsets))

                    StringCollectionCell.StringCollectionCellStart(builder)
                    StringCollectionCell.StringCollectionCellAddValue(builder, cellVec)
                    StringCollectionCell.StringCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))

                StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                StringCollectionColumn.StringCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missingVec = builder.EndVector(len(col))

                StringCollectionColumn.StringCollectionColumnStart(builder)
                StringCollectionColumn.StringCollectionColumnAddValues(builder, valVec)
                StringCollectionColumn.StringCollectionColumnAddMissing(builder, missingVec)
                colOffset = StringCollectionColumn.StringCollectionColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddStringSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.BYTES:

                bytesOffsets = []
                missingVec = builder.CreateByteVector(col.isnull().values.tobytes())
                for idx in range(0, len(col)):
                    if col[idx] == None:
                        bytesOffsets.append(get_empty_ByteCell(builder))
                    else:
                        bytesOffsets.append(get_ByteCell(builder, col[idx]))

                ByteColumn.ByteColumnStartValuesVector(builder, len(bytesOffsets))
                for valIdx in reversed(range(0, len(bytesOffsets))):
                    builder.PrependUOffsetTRelative(bytesOffsets[valIdx])
                valVec = builder.EndVector(len(bytesOffsets))

                serializerstr = ''
                try:
                    serializerstr = serializers[table.get_names()[colIdx]]
                except:
                    pass
                if serializerstr != '':
                    serializer = builder.CreateString(serializerstr)

                ByteColumn.ByteColumnStart(builder)
                ByteColumn.ByteColumnAddValues(builder, valVec)
                ByteColumn.ByteColumnAddMissing(builder, missingVec)
                if serializerstr != '':
                    ByteColumn.ByteColumnAddSerializer(builder, serializer)
                colOffset = ByteColumn.ByteColumnEnd(builder)
                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddByteColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))
            elif table.get_type(colIdx) == _types_.BYTES_LIST:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    collOffsets = []
                    if col[valIdx] == None:
                        collOffsets.append(get_empty_ByteCell(builder))
                    else:
                        cellMissing = []
                        for cellIdx in range(0, len(col[valIdx])):
                            cell = col[valIdx][cellIdx]
                            if cell == None:
                                collOffsets.append(get_empty_ByteCell(builder))
                                cellMissing.append(True)
                            else:
                                collOffsets.append(get_ByteCell(builder, bytearray(cell)))
                                cellMissing.append(False)

                        ByteCollectionCell.ByteCollectionCellStartMissingVector(builder, len(cellMissing))
                        for cellIdx in reversed(range(0, len(cellMissing))):
                            builder.PrependBool(cellMissing[cellIdx])
                        cellMissingVec = builder.EndVector(len(cellMissing))

                        ByteCollectionCell.ByteCollectionCellStartValueVector(builder, len(collOffsets))
                        for cellIdx in reversed(range(0, len(collOffsets))):
                            builder.PrependUOffsetTRelative(collOffsets[cellIdx])
                        cellVec = builder.EndVector(len(collOffsets))

                    ByteCollectionCell.ByteCollectionCellStart(builder)
                    if col[valIdx] != None:
                        ByteCollectionCell.ByteCollectionCellAddValue(builder, cellVec)
                        ByteCollectionCell.ByteCollectionCellAddMissing(builder, cellMissingVec)
                    cellOffsets.append(ByteCollectionCell.ByteCollectionCellEnd(builder))

                ByteCollectionColumn.ByteCollectionColumnStartValuesVector(builder, len(cellOffsets))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                ByteCollectionColumn.ByteCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(col))

                serializerstr = ''
                try:
                    serializerstr = serializers[table.get_names()[colIdx]]
                except:
                    pass
                if serializerstr != '':
                    serializer = builder.CreateString(serializerstr)

                ByteCollectionColumn.ByteCollectionColumnStart(builder)
                ByteCollectionColumn.ByteCollectionColumnAddValues(builder, valVec)
                ByteCollectionColumn.ByteCollectionColumnAddMissing(builder, missVec)
                if serializerstr != '':
                    ByteCollectionColumn.ByteCollectionColumnAddSerializer(builder, serializer)
                colOffset = ByteCollectionColumn.ByteCollectionColumnEnd(builder)

                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddByteListColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

            elif table.get_type(colIdx) == _types_.BYTES_SET:
                cellOffsets = []
                for valIdx in range(0, len(col)):
                    collOffsets = []
                    if col[valIdx] == None:
                        collOffsets.append(get_empty_ByteCell(builder))
                    else:
                        addMissingValue = False
                        for cell in col[valIdx]:
                            if cell == None:
                                addMissingValue = True
                            else:
                                collOffsets.append(get_ByteCell(builder, bytearray(cell)))

                    ByteCollectionCell.ByteCollectionCellStartValueVector(builder, len(collOffsets))
                    for collIdx in reversed(range(0, len(collOffsets))):
                        builder.PrependUOffsetTRelative(collOffsets[collIdx])
                    cellVec = builder.EndVector(len(collOffsets))

                    ByteCollectionCell.ByteCollectionCellStart(builder)
                    ByteCollectionCell.ByteCollectionCellAddValue(builder, cellVec)
                    ByteCollectionCell.ByteCollectionCellAddKeepDummy(builder, addMissingValue)
                    cellOffsets.append(ByteCollectionCell.ByteCollectionCellEnd(builder))

                ByteCollectionColumn.ByteCollectionColumnStartValuesVector(builder, len(cellOffsets))
                for valIdx in reversed(range(0, len(cellOffsets))):
                    builder.PrependUOffsetTRelative(cellOffsets[valIdx])
                valVec = builder.EndVector(len(cellOffsets))

                ByteCollectionColumn.ByteCollectionColumnStartMissingVector(builder, len(col))
                for missIdx in reversed(range(0, len(col))):
                    builder.PrependBool(col[missIdx] == None)
                missVec = builder.EndVector(len(col))

                serializerstr = ''
                try:
                    serializerstr = serializers[table.get_names()[colIdx]]
                except:
                    pass
                if serializerstr != '':
                    serializer = builder.CreateString(serializerstr)

                ByteCollectionColumn.ByteCollectionColumnStart(builder)
                ByteCollectionColumn.ByteCollectionColumnAddValues(builder, valVec)
                ByteCollectionColumn.ByteCollectionColumnAddMissing(builder, missVec)
                if serializerstr != '':
                    ByteCollectionColumn.ByteCollectionColumnAddSerializer(builder, serializer)
                colOffset = ByteCollectionColumn.ByteCollectionColumnEnd(builder)

                Column.ColumnStart(builder)
                Column.ColumnAddType(builder, table.get_type(colIdx))
                Column.ColumnAddByteSetColumn(builder, colOffset)
                colOffsetList.append(Column.ColumnEnd(builder))

        KnimeTable.KnimeTableStartColumnsVector(builder, len(colOffsetList))
        for colOffset in reversed(colOffsetList):
            builder.PrependUOffsetTRelative(colOffset)
        colVecOffset = builder.EndVector(len(colOffsetList))

        KnimeTable.KnimeTableStart(builder)
        KnimeTable.KnimeTableAddRowIDs(builder, rowIdVecOffset)
        KnimeTable.KnimeTableAddColNames(builder, colNameVecOffset)
        KnimeTable.KnimeTableAddColumns(builder, colVecOffset)
        knimeTable = KnimeTable.KnimeTableEnd(builder)
        builder.Finish(knimeTable)

        return builder.Output()
    except flatbuffers.builder.BuilderSizeError:
        raise BufferError("The requested buffersize during serialization exceeds the maximum buffer size."
                          + " Please consider decreasing the 'Rows per chunk' parameter in the 'Options' tab of the configuration dialog.")
    except:
        raise


# Create a {@link ByteCell} in the flatbuffers builder from a bytearray.
# @param builder    the flatbuffers builder
# @param cell       a bytearray
def get_ByteCell(builder, cell):
    bytesVec = builder.CreateByteVector(cell)
    ByteCell.ByteCellStart(builder)
    ByteCell.ByteCellAddValue(builder, bytesVec)
    return ByteCell.ByteCellEnd(builder)


# Create an empty {@link ByteCell} in the flatbuffers builder.
# @param builder    the flatbuffers builder
def get_empty_ByteCell(builder):
    ByteCell.ByteCellStartValueVector(builder, 1)
    ByteCell.ByteCellAddValue(builder, 0)
    bytesVec = builder.EndVector(1)
    ByteCell.ByteCellStart(builder)
    ByteCell.ByteCellAddValue(builder, bytesVec)
    return ByteCell.ByteCellEnd(builder)


# Initialize the enum of known type ids
# @param types     the enum of known type ids
def init(types):
    global _types_
    _types_ = types
