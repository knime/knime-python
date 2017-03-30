import flatbuffers
import os
import imp
import importlib
import inspect
import sys
from knimetable import KnimeTable
from knimetable import IntColumn
from knimetable import Column
from knimetable import StringColumn
from knimetable import DoubleColumn
from knimetable import LongColumn
from knimetable import BooleanColumn
from knimetable import BooleanCollectionColumn
from knimetable import BooleanCollectionCell
   
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
        colNames.append(table.ColNames(j))       
    return colNames

def column_serializers_from_bytes(data_bytes):
    return {}

def bytes_into_table(table, data_bytes):
    knimeTable = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
        
    for j in range(0, knimeTable.ColNamesLength()):
        colNames.append(knimeTable.ColNames(j))
            
    print("Flatbuffers->Python: Column Length() ", knimeTable.ColumnsLength())
    
    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)
        
        print("Flatbuffers->Python: Column[",j,"] col.Type() ", col.Type())
        if col.Type() == _types_.INTEGER.value:
            colVec = col.IntColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colNames[j], colVals)
                      
        elif col.Type() == _types_.BOOLEAN.value:
            colVec = col.BooleanColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
                #print("Flatbuffers -> Python: (Boolean)", colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.BOOLEAN_LIST.value:
            colVec = col.BooleanListColumn()
            print("Flatbuffers -> Python: (Boolean List Column) Start")
            colVals = []
            print("Flatbuffers -> Python: (Boolean List Column) ValuesLength():", colVec.ValuesLength())
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx))
  #                  print("Flatbuffers -> Python: (Boolean List Element)", cell.Value(cellIdx))
                    sys.stdout.flush()
                    
                    
                colVals.append(cellVals)
                print("Flatbuffers -> Python: (Boolean List)[",idx,"]", cellVals)
                sys.stdout.flush()
                
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.LONG.value:
            colVec = col.LongColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
                print("Flatbuffers -> Python: (Long)", colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.DOUBLE.value:
            colVec = col.DoubleColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.STRING.value:
            colVec = col.StringColumn()
            colVals = []        
            for i in range(0, colVec.ValuesLength()):
                colVals.append(colVec.Values(i).decode('utf-8'))
            table.add_column(colNames[j], colVals)
            
    rowIds = []
        
    for idx in range(0, knimeTable.RowIDsLength()):
        rowIds.append(knimeTable.RowIDs(idx))
        
    table.set_rowkeys(rowIds)

          

def table_to_bytes(table):
   
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
    
    
    for colIdx in range(0,table.get_number_columns()):
        if table.get_type(colIdx) == _types_.INTEGER:  
            col = table_column(table, colIdx)
            IntColumn.IntColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                builder.PrependInt32(int(col[valIdx]))
 #               print("Python->Flatbuffers: (Int)", col[valIdx])
            valVec = builder.EndVector(len(col))          
            IntColumn.IntColumnStart(builder)                             
            IntColumn.IntColumnAddValues(builder, valVec)
            colOffset = IntColumn.IntColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BOOLEAN:  
            col = table_column(table, colIdx)
            BooleanColumn.BooleanColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                builder.PrependBool(bool(col[valIdx]))
 #               print("Python->Flatbuffers: (Boolean)", col[valIdx])
            valVec = builder.EndVector(len(col))          
            BooleanColumn.BooleanColumnStart(builder)                             
            BooleanColumn.BooleanColumnAddValues(builder, valVec)
            colOffset = BooleanColumn.BooleanColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BOOLEAN_LIST:  
            col = table_column(table, colIdx)
            print("Python->Flatbuffers: (Boolean List Start)")       
            print("Python->Flatbuffers: (Boolean List Column Length):", len(col))
            cellOffsets = []
            for valIdx in range(0,len(col)):
                print("Python->Flatbuffers: (Boolean List Element)[",valIdx,"]", col[valIdx])
               
                BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(col[valIdx]))):
                    print("Python->Flatbuffers: (Boolean List Element)[",valIdx,",",cellIdx,"]", col[valIdx][cellIdx])
                    builder.PrependBool(col[valIdx][cellIdx])
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                BooleanCollectionCell.BooleanCollectionCellStart(builder)
                BooleanCollectionCell.BooleanCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))
                        
       #     valVec = builder.EndVector(len(cellOffsets))
                        
                
            BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            BooleanCollectionColumn.BooleanCollectionColumnStart(builder)    
            BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder,valVec)                                  
            colOffset = BooleanColumn.BooleanColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.LONG:  
            col = table_column(table, colIdx)
            LongColumn.LongColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                builder.PrependInt64(long(col[valIdx]))
      #          print("Python->Flatbuffers: (Long)", col[valIdx])
            valVec = builder.EndVector(len(col))          
            LongColumn.LongColumnStart(builder)                             
            LongColumn.LongColumnAddValues(builder, valVec)
            colOffset = LongColumn.LongColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
        
        elif table.get_type(colIdx) == _types_.DOUBLE:
            col = table_column(table, colIdx) 
            DoubleColumn.DoubleColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,table.get_number_rows())):
                builder.PrependFloat64(col[valIdx])
            valVec = builder.EndVector(len(col))
            DoubleColumn.DoubleColumnStart(builder)           
           
            DoubleColumn.DoubleColumnAddValues(builder, valVec)
            colOffset = DoubleColumn.DoubleColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.STRING:
          
            col = table_column(table, colIdx) 
          
            strOffsets = []
            for strIdx in range(0, len(col)):
                strOffsets.append(builder.CreateString(col[strIdx]))
            
            StringColumn.StringColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(strOffsets))):
                builder.PrependUOffsetTRelative(strOffsets[valIdx])
            valVec = builder.EndVector(len(col))
            StringColumn.StringColumnStart(builder)        
            StringColumn.StringColumnAddValues(builder, valVec)
            colOffset = StringColumn.StringColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringColumn(builder, colOffset)
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
            
    print("Python->Flatbuffers Finished KnimeTable")
    
    return builder.Output()

def table_column(table, col_idx):
    col = []
    for row_idx in range(0, table.get_number_rows()):
        col.append(table.get_cell(col_idx, row_idx))
    
    
    return col

def init(types):
    global _types_
    _types_ = types
      
