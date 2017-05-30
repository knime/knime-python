import flatbuffers
import os
import imp
import importlib
import inspect
import sys
import struct
from knimetable import KnimeTable
from knimetable import IntColumn
from knimetable import Column
from knimetable import StringColumn
from knimetable import DoubleColumn
from knimetable import LongColumn
from knimetable import BooleanColumn
from knimetable import BooleanCollectionColumn
from knimetable import BooleanCollectionCell
from knimetable import IntCollectionColumn
from knimetable import IntegerCollectionCell
from knimetable import LongCollectionColumn
from knimetable import LongCollectionCell
from knimetable import DoubleCollectionColumn
from knimetable import DoubleCollectionCell
from knimetable import StringCollectionColumn
from knimetable import StringCollectionCell
from knimetable import ByteCell
from knimetable import ByteColumn
from knimetable import ByteCollectionCell
from knimetable import ByteCollectionColumn
   
_types_ = None

def column_types_from_bytes(data_bytes):    
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colTypes= []
    for j in range(0, table.ColumnsLength()):
        colTypes.append(table.Columns(j).Type())       
    return colTypes


def column_names_from_bytes(data_bytes):
    
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, table.ColNamesLength()):
        colNames.append(table.ColNames(j).decode('utf-8'))       
    return colNames

def column_serializers_from_bytes(data_bytes):
    
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)
    
    colNames = column_names_from_bytes(data_bytes)
    
    serializers = {}
    for j in range(0, table.ColumnsLength()):
        if table.Columns(j).Type() == _types_.BYTES.value:
            serializers[colNames[j]] = table.Columns(j).ByteColumn().Serializer().decode('utf-8') 
        elif table.Columns(j).Type() == _types_.BYTES_LIST.value:
            serializers[colNames[j]] = table.Columns(j).ByteListColumn().Serializer().decode('utf-8') 
        elif table.Columns(j).Type() == _types_.BYTES_SET.value :
            serializers[colNames[j]] = table.Columns(j).ByteSetColumn().Serializer().decode('utf-8')
    
    return serializers

def bytes_into_table(table, data_bytes):
  #  print("Starting bytes_into_table()")
    
    
    knimeTable = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)


    rowIds = []
        
    for idx in range(0, knimeTable.RowIDsLength()):
        rowIds.append(knimeTable.RowIDs(idx).decode('utf-8'))        
    
    colNames = []
        
    for j in range(0, knimeTable.ColNamesLength()):
        colNames.append(knimeTable.ColNames(j).decode('utf-8'))
            
  #  print("Flatbuffers->Python: Column Length() ", knimeTable.ColumnsLength())
  
    
    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)
        
     #   print("Flatbuffers->Python: Column[",j,"] col.Type() ", col.Type())
        if col.Type() == _types_.INTEGER.value:
            colVec = col.IntColumn()
            table.add_column(colNames[j], get_single_column(colVec))
            
        elif col.Type() == _types_.INTEGER_LIST.value:
            colVec = col.IntListColumn()                       
            table.add_column(colNames[j], get_list_column(colVec))
            
        elif col.Type() == _types_.INTEGER_SET.value:
            colVec = col.IntSetColumn()                 
            table.add_column(colNames[j], get_set_column(colVec))

        elif col.Type() == _types_.BOOLEAN.value:
            colVec = col.BooleanColumn()
            table.add_column(colNames[j], get_single_column(colVec))
            
        elif col.Type() == _types_.BOOLEAN_LIST.value:
            colVec = col.BooleanListColumn()            
            table.add_column(colNames[j], get_list_column(colVec))
            
        elif col.Type() == _types_.BOOLEAN_SET.value:
            colVec = col.BooleanSetColumn()                
            table.add_column(colNames[j], get_set_column(colVec))
         
        elif col.Type() == _types_.LONG.value:
            colVec = col.LongColumn()
            table.add_column(colNames[j], get_single_column(colVec))
            
        elif col.Type() == _types_.LONG_LIST.value:
            colVec = col.LongListColumn()
            table.add_column(colNames[j], get_list_column(colVec))
            
        elif col.Type() == _types_.LONG_SET.value:
            colVec = col.LongSetColumn()
            table.add_column(colNames[j], get_set_column(colVec))
         
        elif col.Type() == _types_.DOUBLE.value:
            colVec = col.DoubleColumn()
            table.add_column(colNames[j], get_single_column(colVec))
            
        elif col.Type() == _types_.DOUBLE_LIST.value:
            colVec = col.DoubleListColumn()
            table.add_column(colNames[j], get_list_column(colVec))
            
        elif col.Type() == _types_.DOUBLE_SET.value:
            colVec = col.DoubleSetColumn()
            table.add_column(colNames[j], get_set_column(colVec))
                       
        elif col.Type() == _types_.STRING.value:
            colVec = col.StringColumn()
            colVals = []        
            for idx in range(0, colVec.ValuesLength()):
         #       print("Flatbuffers -> Python: (String) Value[", idx, "]", colVec.Values(idx))
         #       print("Flatbuffers -> Python: (String) Missing[", idx, "]",  colVec.Missing(idx))
                if colVec.Missing(idx):
                    colVals.append(None)
                else:
                    colVals.append(colVec.Values(idx).decode('utf-8'))
            
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.STRING_LIST.value:
            colVec = col.StringListColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                
                if colVec.Missing(idx):
                    colVals.append(None)
                else:
                    cell = colVec.Values(idx)
                    cellVals = []
                    for cellIdx in range(0, cell.ValueLength()):
                        if cell.Missing(cellIdx):
                            cellVals.append(None)
                        else:
                            cellVals.append(cell.Value(cellIdx).decode('utf-8'))
                    colVals.append(cellVals)

            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.STRING_SET.value:
            colVec = col.StringSetColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                
                if colVec.Missing(idx):
                    colVals.append(None)
                else:
                    cell = colVec.Values(idx)
                    cellVals = set()
                    for cellIdx in range(0, cell.ValueLength()):
                        cellVals.add(cell.Value(cellIdx).decode('utf-8'))
                    if cell.KeepDummy():
                        cellVals.add(None)
                    colVals.append(cellVals)
                 
            table.add_column(colNames[j], colVals)

            
        elif col.Type() == _types_.BYTES.value:
          #  print("Flatbuffers -> Python: (BYTES Column) Start")
            colVec = col.ByteColumn()
            colVals = []
          #  print("Flatbuffers -> Python: (BYTES Column) ValuesLength():", colVec.ValuesLength())        
            for idx in range(0, colVec.ValuesLength()):
                cell = colVec.Values(idx)   
                byteVals = []           
                
                 #   print("Flatbuffers -> Python: (BYTES Column) Cell.Type():", type(cell.Value(cellIdx)))
                for byteIdx in range(0,cell.ValueLength()):
                    val = cell.Value(byteIdx)
                 #   if val < 0:
                 #       val += 256
                    byteVals.append(val)
                 
                if colVec.Missing(idx):
                    colVals.append(None)
               #     print("Flatbuffers -> Python: (BYTES Column) col Missing", colVec.Missing(idx)) 
                else:
                    colVals.append(bytearray(byteVals))
              #  print("Flatbuffers -> Python: (BYTES Column) Cell.Type():", type(byteVals))
              #  print("Flatbuffers -> Python: (BYTES Column) Cell.Type():", byteVals)
                 
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.BYTES_LIST.value:
            colVec = col.ByteListColumn()
            colVals = []
            # Rows in the column
            for idx in range(0,colVec.ValuesLength()):
                if colVec.Missing(idx):
                    colVals.append(None)
                else:   
                    cell = colVec.Values(idx)
                    cellVals = []
                    # units in the List
                    for cellIdx in range(0, cell.ValueLength()):
                        byteVals = []
                        if cell.Missing(cellIdx):
                            cellVals.append(None)
                        else:           
                            for byteIdx in range(0,cell.Value(cellIdx).ValueLength()):
                                byteVals.append(cell.Value(cellIdx).Value(byteIdx))
                            cellVals.append(bytes(struct.pack("b"*len(byteVals),*byteVals))) 
               #     print("Flatbuffers -> Python: (BYTES LIST Column) Cell.Type():", type(byteVals))
               #     print("Flatbuffers -> Python: (BYTES LIST Column) Cell", byteVals)      

                    colVals.append(cellVals)
             
            table.add_column(colNames[j], colVals)
                           
        elif col.Type() == _types_.BYTES_SET.value:
            colVec = col.ByteSetColumn()
            colVals = []
            # Rows in the column
            for idx in range(0,colVec.ValuesLength()):
                
                if colVec.Missing(idx):
                    colVals.append(None)
                else:
                    cell = colVec.Values(idx)
                    cellVals = set()
                    # units in the Set
                    for cellIdx in range(0, cell.ValueLength()):
                        byteVals = []           
                        for byteIdx in range(0,cell.Value(cellIdx).ValueLength()):
                            byteVals.append(cell.Value(cellIdx).Value(byteIdx))
                        cellVals.add(bytes(struct.pack("b"*len(byteVals),*byteVals)))
                        if cell.KeepDummy():
                            cellVals.add(None)
                    
                #    print("Flatbuffers -> Python: (BYTES SET Column) Cell.Type():", type(byteVals))
                #    print("Flatbuffers -> Python: (BYTES SET Column) Cell", byteVals)
                                  
                    colVals.append(cellVals)
            
            table.add_column(colNames[j], colVals)

    table.set_rowkeys(rowIds)

def get_single_column(colVec):
    colVals = []
    for idx in range(0,colVec.ValuesLength()):
        if colVec.Missing(idx):
            colVals.append(None)
        else:
            colVals.append(colVec.Values(idx))
    return colVals

def get_list_column(colVec):
    colVals = []
    for idx in range(0,colVec.ValuesLength()):        
        if colVec.Missing(idx):
            colVals.append(None)
        else:
            cell = colVec.Values(idx)
            cellVals = []
            for cellIdx in range(0, cell.ValueLength()):
                if cell.Missing(cellIdx):
                    cellVals.append(None)
                else:
                    cellVals.append(cell.Value(cellIdx))

            colVals.append(cellVals)
    return colVals

def get_set_column(colVec):
    colVals = []
    for idx in range(0,colVec.ValuesLength()):              
        if colVec.Missing(idx):
            colVals.append(None)
        else:
            cell = colVec.Values(idx)
            cellVals = set()
            for cellIdx in range(0, cell.ValueLength()):
                cellVals.add(cell.Value(cellIdx))
  
            if cell.KeepDummy():
                cellVals.add(None)
                        
            colVals.append(cellVals)
        
    return colVals

def table_to_bytes(table):
   
    #print("Python->Flatbuffers: Starting table_to_bytes()")
    builder = flatbuffers.Builder(1024)
    
    #Row IDs
    rowIdOffsets = [] 
    
    for idx in range(0, table.get_number_rows()):
        rowIdOffset = builder.CreateString(str(table.get_rowkey(idx)))
        rowIdOffsets.append(rowIdOffset)
 #       print("Python->Flatbuffers: (RowID)", table.get_rowkey(idx))
        
    KnimeTable.KnimeTableStartRowIDsVector(builder, len(rowIdOffsets))
    for idOffset in reversed(rowIdOffsets):
        builder.PrependUOffsetTRelative(idOffset)
    rowIdVecOffset = builder.EndVector(len(rowIdOffsets))
       
    #Column Names
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

    
   # print("data_frame", table._data_frame)
    for colIdx in range(0,table.get_number_columns()):
        if table.get_type(colIdx) == _types_.INTEGER:  
            col = table_column(table, colIdx)
            IntColumn.IntColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                if col[valIdx] == None:
                    builder.PrependInt32(-2147483648)
                else:
                    builder.PrependInt32(int(col[valIdx]))
 #               print("Python->Flatbuffers: (Int)", col[valIdx])
            valVec = builder.EndVector(len(col))
            
            IntColumn.IntColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))
                      
            IntColumn.IntColumnStart(builder)                             
            IntColumn.IntColumnAddValues(builder, valVec)
            IntColumn.IntColumnAddMissing(builder, missVec)
            colOffset = IntColumn.IntColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.INTEGER_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
                     
            for valIdx in range(0,len(col)):
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
                IntegerCollectionCell.IntegerCollectionCellAddValue(builder,cellVec)
                IntegerCollectionCell.IntegerCollectionCellAddMissing(builder, cellMissingVec)
                cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))
                        
            IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))
            
            IntCollectionColumn.IntCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
            
            IntCollectionColumn.IntCollectionColumnStart(builder)    
            IntCollectionColumn.IntCollectionColumnAddValues(builder,valVec)
            IntCollectionColumn.IntCollectionColumnAddMissing(builder, missVec)                                  
            colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.INTEGER_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            
            for valIdx in range(0,len(col)):
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
#                    print("Python->Flatbuffers: (Integer Set)", col[valIdx])                   
                
                IntegerCollectionCell.IntegerCollectionCellStart(builder)
                IntegerCollectionCell.IntegerCollectionCellAddValue(builder,cellVec)
                IntegerCollectionCell.IntegerCollectionCellAddKeepDummy(builder, addMissingValue)
                cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))
                        
            IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))
            
            IntCollectionColumn.IntCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
         
            IntCollectionColumn.IntCollectionColumnStart(builder)    
            IntCollectionColumn.IntCollectionColumnAddValues(builder,valVec)  
            IntCollectionColumn.IntCollectionColumnAddMissing(builder,missVec)                                
            colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))

            
        elif table.get_type(colIdx) == _types_.BOOLEAN:  
            col = table_column(table, colIdx)
            BooleanColumn.BooleanColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                if col[valIdx] == None:
                    builder.PrependBool(False)
                else:
                    builder.PrependBool(bool(col[valIdx]))
 #               print("Python->Flatbuffers: (Boolean)", col[valIdx])
            valVec = builder.EndVector(len(col))
            
            BooleanColumn.BooleanColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))   
                
            BooleanColumn.BooleanColumnStart(builder)                             
            BooleanColumn.BooleanColumnAddValues(builder, valVec)
            BooleanColumn.BooleanColumnAddMissing(builder, missVec)
            colOffset = BooleanColumn.BooleanColumnEnd(builder)
                       
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BOOLEAN_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               # print("Python->Flatbuffers: (Boolean List) col [", valIdx,"]", col[valIdx]) 
                if col[valIdx] == None:
                    BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, 1)
                    builder.PrependBool(True)
                    cellVec = builder.EndVector(1)
                else: 
                    BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                    cellMissing = []
                 #   print("Python->Flatbuffers: (Boolean List) cell", col[valIdx])                 
                    for cellIdx in reversed(range(0, len(col[valIdx]))):
                        if col[valIdx][cellIdx] == None:
                            builder.PrependBool(False)
                            cellMissing.append(True)
                        else:
                            builder.PrependBool(col[valIdx][cellIdx])
                            cellMissing.append(False)
                    cellVec = builder.EndVector(len(col[valIdx]))
                #    print("Python->Flatbuffers: (Boolean List) missing", cellMissing) 
                    # the missing vector is already in reversed order
                    BooleanCollectionCell.BooleanCollectionCellStartMissingVector(builder, len(col[valIdx]))
                    for cellIdx in range(0, len(col[valIdx])):
                        builder.PrependBool(cellMissing[cellIdx])
                    cellMissingVec = builder.EndVector(len(col[valIdx]))
                    
                BooleanCollectionCell.BooleanCollectionCellStart(builder)
                BooleanCollectionCell.BooleanCollectionCellAddValue(builder,cellVec)
                BooleanCollectionCell.BooleanCollectionCellAddMissing(builder,cellMissingVec)
                cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))
                        
       #     valVec = builder.EndVector(len(cellOffsets))
                        
                
            BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets)) 
            
            BooleanCollectionColumn.BooleanCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
              
            BooleanCollectionColumn.BooleanCollectionColumnStart(builder)    
            BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder,valVec)
            BooleanCollectionColumn.BooleanCollectionColumnAddMissing(builder,missVec)                                  
            colOffset = BooleanCollectionColumn.BooleanCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BOOLEAN_SET:  
            col = table_column(table, colIdx)
#            print("Python->Flatbuffers: (Boolean Set Start)")       
#            print("Python->Flatbuffers: (Boolean Set Column Length):", len(col))
            cellOffsets = []
            for valIdx in range(0,len(col)):
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
                BooleanCollectionCell.BooleanCollectionCellAddValue(builder,cellVec)
                BooleanCollectionCell.BooleanCollectionCellAddKeepDummy(builder, addMissingValue)
                cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))
                                       
            BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            BooleanCollectionColumn.BooleanCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
          
            BooleanCollectionColumn.BooleanCollectionColumnStart(builder)    
            BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder,valVec)   
            BooleanCollectionColumn.BooleanCollectionColumnAddMissing(builder,missVec)                               
            colOffset = BooleanCollectionColumn.BooleanCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))

            
        elif table.get_type(colIdx) == _types_.LONG:  
            col = table_column(table, colIdx)
            LongColumn.LongColumnStartValuesVector(builder, len(col))
           
        
            if sys.version_info > (3,0):
                for valIdx in reversed(range(0,len(col))):
                    if col[valIdx] == None:
                        builder.PrependInt64(-9223372036854775808)
                    else:
                        builder.PrependInt64(col[valIdx])
            else:
                for valIdx in reversed(range(0,len(col))):
                    if col[valIdx] == None:
                        builder.PrependInt64(-9223372036854775808)
                    else:
                        builder.PrependInt64((col[valIdx]))
      #          print("Python->Flatbuffers: (Long)", col[valIdx])
            valVec = builder.EndVector(len(col))  
            
            LongColumn.LongColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))   
        
            LongColumn.LongColumnStart(builder)                             
            LongColumn.LongColumnAddValues(builder, valVec)
            LongColumn.LongColumnAddMissing(builder, missVec)
            colOffset = LongColumn.LongColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.LONG_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
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
                LongCollectionCell.LongCollectionCellAddValue(builder,cellVec)
                LongCollectionCell.LongCollectionCellAddMissing(builder, cellMissingVec)
                cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))
                        
            LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))
            
            LongCollectionColumn.LongCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
    
            
            LongCollectionColumn.LongCollectionColumnStart(builder)    
            LongCollectionColumn.LongCollectionColumnAddValues(builder,valVec) 
            LongCollectionColumn.LongCollectionColumnAddMissing(builder,missVec)                                 
            colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.LONG_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
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
                LongCollectionCell.LongCollectionCellAddValue(builder,cellVec)
                LongCollectionCell.LongCollectionCellAddKeepDummy(builder, addMissingValue)
                cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))
                        
            LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            LongCollectionColumn.LongCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(cellOffsets))   
              
            LongCollectionColumn.LongCollectionColumnStart(builder)    
            LongCollectionColumn.LongCollectionColumnAddValues(builder,valVec) 
            LongCollectionColumn.LongCollectionColumnAddMissing(builder,missVec)                                                             
            colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder) 
                      
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
        
        elif table.get_type(colIdx) == _types_.DOUBLE:
            col = table_column(table, colIdx) 
            DoubleColumn.DoubleColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,table.get_number_rows())):
                if col[valIdx] == None:
                    builder.PrependFloat64(float('NaN'))
                else:
                    builder.PrependFloat64(col[valIdx])
             #   print("Python->Flatbuffers: (Double) (col)[", valIdx, "]", col[valIdx])   
                
            valVec = builder.EndVector(len(col))
                
            DoubleColumn.DoubleColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missingVec = builder.EndVector(len(col))                      
                   
            DoubleColumn.DoubleColumnStart(builder)                     
            DoubleColumn.DoubleColumnAddValues(builder, valVec)
            DoubleColumn.DoubleColumnAddMissing(builder, missingVec)
            colOffset = DoubleColumn.DoubleColumnEnd(builder)
            
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.DOUBLE_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               # print("Python->Flatbuffers: (Double List)", col[valIdx])               
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
                            #print("Python->Flatbuffers: (Double List Cell) [", cellIdx, "]", col[valIdx][cellIdx])
                        else:
                            builder.PrependFloat64(col[valIdx][cellIdx])
                            cellMissing.append(False)    
                            #print("Python->Flatbuffers: (Double List Cell) [", cellIdx, "]", col[valIdx][cellIdx])                 
                    cellVec = builder.EndVector(len(col[valIdx]))
                    
                #    print("Python->Flatbuffers: (Double List Cell):", col[valIdx])
                    # the missing vector is already in reversed order
                    DoubleCollectionCell.DoubleCollectionCellStartMissingVector(builder, len(col[valIdx]))
                    for cellIdx in range(0, len(col[valIdx])):
                        builder.PrependBool(cellMissing[cellIdx])
                    cellMissingVec = builder.EndVector(len(col[valIdx]))
                                   
                DoubleCollectionCell.DoubleCollectionCellStart(builder)
                DoubleCollectionCell.DoubleCollectionCellAddValue(builder,cellVec)
                DoubleCollectionCell.DoubleCollectionCellAddMissing(builder, cellMissingVec)
                cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))
                        
            DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))   
            
            DoubleCollectionColumn.DoubleCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))                      
            
            DoubleCollectionColumn.DoubleCollectionColumnStart(builder)    
            DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder,valVec) 
            DoubleCollectionColumn.DoubleCollectionColumnAddMissing(builder, missVec)                                 
            colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.DOUBLE_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
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
                DoubleCollectionCell.DoubleCollectionCellAddValue(builder,cellVec)
                DoubleCollectionCell.DoubleCollectionCellAddKeepDummy(builder, addMissingValue)
                cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))
                        
            DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))
                
            DoubleCollectionColumn.DoubleCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))                      
            
            DoubleCollectionColumn.DoubleCollectionColumnStart(builder)    
            DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder,valVec) 
            DoubleCollectionColumn.DoubleCollectionColumnAddMissing(builder, missVec)                                                                
            colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
                
        elif table.get_type(colIdx) == _types_.STRING:
            #print("Python->Flatbuffers: (String) colIdx", colIdx, "colName", table.get_names()[colIdx])
          
            col = table_column(table, colIdx) 
          
            strOffsets = []
            for strIdx in range(0, len(col)):
                if col[strIdx] == None:
                    strOffsets.append(builder.CreateString("Missing Value"))
                else:
                    strOffsets.append(builder.CreateString(col[strIdx]))
            
            StringColumn.StringColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(strOffsets))):
                builder.PrependUOffsetTRelative(strOffsets[valIdx])
            valVec = builder.EndVector(len(col))
            
            StringColumn.StringColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missVec = builder.EndVector(len(col))                      
            
            StringColumn.StringColumnStart(builder)        
            StringColumn.StringColumnAddValues(builder, valVec)
            StringColumn.StringColumnAddMissing(builder, missVec)
            colOffset = StringColumn.StringColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.STRING_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                strOffsets = []
                if col[valIdx] == None:
                    strOffsets.append(builder.CreateString("Missing Value"))
                
#                print("Python->Flatbuffers: (String List Cell):", col[valIdx])
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
                StringCollectionCell.StringCollectionCellAddValue(builder,cellVec)
                StringCollectionCell.StringCollectionCellAddMissing(builder, cellMissingVec)
                cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))
                        
            StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))  
            
            StringCollectionColumn.StringCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missingVec = builder.EndVector(len(col))                      
            
            StringCollectionColumn.StringCollectionColumnStart(builder)    
            StringCollectionColumn.StringCollectionColumnAddValues(builder,valVec)
            StringCollectionColumn.StringCollectionColumnAddMissing(builder,missingVec)                                  
            colOffset = StringCollectionColumn.StringCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.STRING_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
#                print("Python->Flatbuffers: (String Set Cell):", col[valIdx])
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
                StringCollectionCell.StringCollectionCellAddValue(builder,cellVec)
                StringCollectionCell.StringCollectionCellAddKeepDummy(builder, addMissingValue)
                cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))
                        
            StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            StringCollectionColumn.StringCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(col[missIdx] == None)
            missingVec = builder.EndVector(len(col))                      
            
            StringCollectionColumn.StringCollectionColumnStart(builder)    
            StringCollectionColumn.StringCollectionColumnAddValues(builder,valVec)  
            StringCollectionColumn.StringCollectionColumnAddMissing(builder, missingVec)                                
            colOffset = StringCollectionColumn.StringCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BYTES:
            #print("Python->Flatbuffers: Starting BYTES")
            col = table_column(table, colIdx) 
          
            bytesOffsets = []
            missing = []
            for idx in range(0, len(col)):
                
                if col[idx] == None:
                    cell = bytes(0)
                    bytesOffsets.append(get_empty_ByteCell(builder))
                    missing.append(True)
                else:
                    cell = bytearray(col[idx])
                    bytesOffsets.append(get_ByteCell(builder, cell))
                    missing.append(False)
                    #print("Python->Flatbuffers: (Bytes) cell:", cell)
                    #print("Python->Flatbuffers: (Bytes) len(cell):", len(cell))
                
                            
            ByteColumn.ByteColumnStartValuesVector(builder, len(bytesOffsets))
            for valIdx in reversed(range(0,len(bytesOffsets))):
                builder.PrependUOffsetTRelative(bytesOffsets[valIdx])
            valVec = builder.EndVector(len(bytesOffsets))
            
            ByteColumn.ByteColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
                builder.PrependBool(missing[missIdx])
            missVec = builder.EndVector(len(col))                        
            
            serializerstr = ''
            try:
                serializerstr = serializers[table.get_names()[colIdx]]
            except:
                pass
            if serializerstr != '':
                serializer = builder.CreateString(serializerstr)
            
            ByteColumn.ByteColumnStart(builder)        
            ByteColumn.ByteColumnAddValues(builder, valVec)
            ByteColumn.ByteColumnAddMissing(builder, missVec)
            if serializerstr != '':
                ByteColumn.ByteColumnAddSerializer(builder, serializer)
            colOffset = ByteColumn.ByteColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddByteColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            #print("Python->Flatbuffers: Ending BYTES")
            
        elif table.get_type(colIdx) == _types_.BYTES_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
                collOffsets = []
                if col[valIdx] == None:                                                              
                    collOffsets.append(get_empty_ByteCell(builder))                 
                else:                 
                    #print("Python->Flatbuffers: (BYTES List Cell): col[valIdx]", col[valIdx])
                    cellMissing = []
                    for cellIdx in range(0, len(col[valIdx])):
                        cell = col[valIdx][cellIdx]
                        #print("Python->Flatbuffers: (BYTES List Cell): cell [", cellIdx, "]", cell)
                        if cell == None:
                            collOffsets.append(get_empty_ByteCell(builder))
                            cellMissing.append(True)
                        else:
                            collOffsets.append(get_ByteCell(builder, bytearray(cell)))
                            cellMissing.append(False)
                    
                    #print("Python->Flatbuffers: (BYTES List): cellMissing", cellMissing)
                            
                    ByteCollectionCell.ByteCollectionCellStartMissingVector(builder, len(cellMissing))
                    for cellIdx in reversed(range(0, len(cellMissing))):
                         builder.PrependBool(cellMissing[cellIdx])                                    
                    cellMissingVec = builder.EndVector(len(cellMissing))       
                        
                                 
                    ByteCollectionCell.ByteCollectionCellStartValueVector(builder, len(collOffsets))
                    for cellIdx in reversed(range(0, len(collOffsets))):
                        builder.PrependUOffsetTRelative(collOffsets[cellIdx])                                    
                    cellVec = builder.EndVector(len(collOffsets))                   
                
                ByteCollectionCell.ByteCollectionCellStart(builder)
                ByteCollectionCell.ByteCollectionCellAddValue(builder, cellVec)
                ByteCollectionCell.ByteCollectionCellAddMissing(builder, cellMissingVec)
                cellOffsets.append(ByteCollectionCell.ByteCollectionCellEnd(builder))
                        
            ByteCollectionColumn.ByteCollectionColumnStartValuesVector(builder, len(cellOffsets))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets)) 
            
            ByteCollectionColumn.ByteCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
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
            ByteCollectionColumn.ByteCollectionColumnAddValues(builder,valVec)
            ByteCollectionColumn.ByteCollectionColumnAddMissing(builder,missVec)
            if serializerstr != '':   
                ByteCollectionColumn.ByteCollectionColumnAddSerializer(builder,serializer)                                         
            colOffset = ByteCollectionColumn.ByteCollectionColumnEnd(builder)   
                    
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddByteListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BYTES_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
                collOffsets = []
                if col[valIdx] == None:
                    collOffsets.append(get_empty_ByteCell(builder))                   
                else:                 
             #       print("Python->Flatbuffers: (BYTES List Cell): col[valIdx]", col[valIdx])
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
                ByteCollectionCell.ByteCollectionCellAddValue(builder,cellVec)
                ByteCollectionCell.ByteCollectionCellAddKeepDummy(builder,addMissingValue)
                cellOffsets.append(ByteCollectionCell.ByteCollectionCellEnd(builder))
                        
            ByteCollectionColumn.ByteCollectionColumnStartValuesVector(builder, len(cellOffsets))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets)) 
            
            ByteCollectionColumn.ByteCollectionColumnStartMissingVector(builder, len(col))
            for missIdx in reversed(range(0,len(col))):
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
            ByteCollectionColumn.ByteCollectionColumnAddValues(builder,valVec)
            ByteCollectionColumn.ByteCollectionColumnAddMissing(builder,missVec) 
            if serializerstr != '':  
                ByteCollectionColumn.ByteCollectionColumnAddSerializer(builder,serializer)                                         
            colOffset = ByteCollectionColumn.ByteCollectionColumnEnd(builder)   
                    
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
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
            
  #  print("Python->Flatbuffers Finished KnimeTable")
    
    return builder.Output()

def get_ByteCell(builder, cell):
    ByteCell.ByteCellStartValueVector(builder, len(cell))
    for byteIdx in reversed(range(0, len(cell))):
        builder.PrependUint8(cell[byteIdx])                           
    bytesVec = builder.EndVector(len(cell))
                
    ByteCell.ByteCellStart(builder)
    ByteCell.ByteCellAddValue(builder, bytesVec)
    return ByteCell.ByteCellEnd(builder)
     

def get_empty_ByteCell(builder):
    ByteCell.ByteCellStartValueVector(builder, 1)
    ByteCell.ByteCellAddValue(builder, 0)
    bytesVec = builder.EndVector(1)
    ByteCell.ByteCellStart(builder)
    ByteCell.ByteCellAddValue(builder, bytesVec)
    return ByteCell.ByteCellEnd(builder)
    

def table_column(table, col_idx):
    col = []
    for row_idx in range(0, table.get_number_rows()):
        col.append(table.get_cell(col_idx, row_idx))
    
    
    return col

def init(types):
    global _types_
    _types_ = types
      
