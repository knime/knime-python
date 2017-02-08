import flatbuffers
import os
import imp
import importlib
import inspect
from knimetable import KnimeTable
from knimetable import IntColumn
from knimetable import Column
from knimetable import StringColumn
from knimetable import DoubleColumn
   

_types_ = None


def column_names_from_bytes(data_bytes):
    
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, table.ColNamesLength()):
        colNames.append(table.ColNames(j))
    
    return colNames

def bytes_into_table(table, data_bytes):
    knimeTable = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, knimeTable.ColNamesLength()):
        colNames.append(knimeTable.ColNames(j))
            
    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)
        
        if col.Type() == _types_.INTEGER.value:
            colVec = col.IntColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
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
            table.add_column(colNames[j],colVals)
          

def table_to_bytes(table):
   
    builder = flatbuffers.Builder(1024)
    
    colNameOffsets = []
   
    for colName in table.get_names():
        nameOffset = builder.CreateString(str(colName))
        colNameOffsets.append(nameOffset)
   
    KnimeTable.KnimeTableStartHeadersVector(builder, len(colNameOffsets))
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
            valVec = builder.EndVector(len(col))          
            IntColumn.IntColumnStart(builder)                             
            IntColumn.IntColumnAddValues(builder, valVec)
            colOffset = IntColumn.IntColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntColumn(builder, colOffset)
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
    KnimeTable.KnimeTableAddColNames(builder, colNameVecOffset)
    KnimeTable.KnimeTableAddColumns(builder, colVecOffset)
    knimeTable = KnimeTable.KnimeTableEnd(builder)
    builder.Finish(knimeTable)
            
    return builder.Output()

def table_column(table, col_idx):
    col = []
    for row_idx in range(0, table.get_number_rows()):
        col.append(table.get_cell(col_idx, row_idx))
        
    return col



def init(types):
    global _types_
    _types_ = types
      
