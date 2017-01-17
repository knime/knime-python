import flatbuffers
import os
import imp
import importlib
import inspect
from knimetable import KnimeTable
from knimetable import Header
from knimetable import IntColumn
from knimetable import Column
from knimetable import StringColumn
from knimetable import DoubleColumn
   

_types_ = None


def column_names_from_bytes(data_bytes):
    
    table = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, table.HeadersLength()):
        colNames.append(table.Headers(j).Name().decode('utf-8'))
    
    return colNames

def bytes_into_table(table, data_bytes):
    knimeTable = KnimeTable.KnimeTable.GetRootAsKnimeTable(data_bytes, 0)

    colNames = []
    for j in range(0, knimeTable.HeadersLength()):
        colNames.append(knimeTable.Headers(j).Name().decode('utf-8'))
        

    
    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)
        
        if col.Type() == _types_.INTEGER.value:
            colVec = col.IntColumns()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colVec.Name().decode('utf-8'), colVals)
            
        elif col.Type() == _types_.DOUBLE.value:
            colVec = col.DoubleColumns()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colVec.Name().decode('utf-8'), colVals)
            
        elif col.Type() == _types_.STRING.value:
            colVec = col.StringColumns()
            colVals = []        
            for i in range(0, colVec.ValuesLength()):
                colVals.append(colVec.Values(i).decode('utf-8'))
            table.add_column(colVec.Name().decode('utf-8'),colVals)
          

def table_to_bytes(table):
   
    builder = flatbuffers.Builder(1024)
    
    colNameList = []
   
    for colName in table.get_names():
        nameOffset = builder.CreateString(str(colName))
        Header.HeaderStart(builder)
        Header.HeaderAddName(builder, nameOffset)
        colNameList.append(Header.HeaderEnd(builder))
   
    KnimeTable.KnimeTableStartHeadersVector(builder, len(colNameList))
    for colNameOffset in reversed(colNameList):
        builder.PrependUOffsetTRelative(colNameOffset) 
    colNameVecOffset = builder.EndVector(len(colNameList))
    
    colOffsetList = []
    
    for colIdx in range(0,table.get_number_columns()):
        nameOffset = builder.CreateString((table.get_name(colIdx)))
        if table.get_type(colIdx) == _types_.INTEGER:  
            col = table_column(table, colIdx)
            IntColumn.IntColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(col))):
                builder.PrependInt32(int(col[valIdx]))
            valVec = builder.EndVector(len(col))          
            IntColumn.IntColumnStart(builder)           
            IntColumn.IntColumnAddName(builder, nameOffset)          
            IntColumn.IntColumnAddValues(builder, valVec)
            colOffset = IntColumn.IntColumnEnd(builder)
            
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntColumns(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
        
        elif table.get_type(colIdx) == _types_.DOUBLE:
            col = table_column(table, colIdx) 
            DoubleColumn.DoubleColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,table.get_number_rows())):
                builder.PrependFloat64(col[valIdx])
            valVec = builder.EndVector(len(col))
            DoubleColumn.DoubleColumnStart(builder)           
            DoubleColumn.DoubleColumnAddName(builder, nameOffset)
            DoubleColumn.DoubleColumnAddValues(builder, valVec)
            colOffset = DoubleColumn.DoubleColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleColumns(builder, colOffset)
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
            StringColumn.StringColumnAddName(builder, nameOffset)          
            StringColumn.StringColumnAddValues(builder, valVec)
            colOffset = StringColumn.StringColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringColumns(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
             
    
    KnimeTable.KnimeTableStartColumnsVector(builder, len(colOffsetList))
    for colOffset in reversed(colOffsetList):
        builder.PrependUOffsetTRelative(colOffset)    
    colVecOffset = builder.EndVector(len(colOffsetList))
          
    KnimeTable.KnimeTableStart(builder)
    KnimeTable.KnimeTableAddHeaders(builder, colNameVecOffset)
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
      
