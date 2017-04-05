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
from knimetable import IntCollectionColumn
from knimetable import IntegerCollectionCell
from knimetable import LongCollectionColumn
from knimetable import LongCollectionCell
from knimetable import DoubleCollectionColumn
from knimetable import DoubleCollectionCell
from knimetable import StringCollectionColumn
from knimetable import StringCollectionCell
   
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
            
  #  print("Flatbuffers->Python: Column Length() ", knimeTable.ColumnsLength())
    
    for j in range(0, knimeTable.ColumnsLength()):
        col = knimeTable.Columns(j)
        
     #   print("Flatbuffers->Python: Column[",j,"] col.Type() ", col.Type())
        if col.Type() == _types_.INTEGER.value:
            colVec = col.IntColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.INTEGER_LIST.value:
            colVec = col.IntListColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx))

                colVals.append(cellVals)
             
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.INTEGER_SET.value:
            colVec = col.IntSetColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = set()
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.add(cell.Value(cellIdx))
  
                colVals.append(cellVals)
                 
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
      #      print("Flatbuffers -> Python: (Boolean List Column) Start")
            colVals = []
     #       print("Flatbuffers -> Python: (Boolean List Column) ValuesLength():", colVec.ValuesLength())
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx))
  #                  print("Flatbuffers -> Python: (Boolean List Element)", cell.Value(cellIdx))

                colVals.append(cellVals)
      #          print("Flatbuffers -> Python: (Boolean List)[",idx,"]", cellVals)
             
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.BOOLEAN_SET.value:
            colVec = col.BooleanSetColumn()
 #           print("Flatbuffers -> Python: (Boolean Set Column) Start")
            colVals = []
 #           print("Flatbuffers -> Python: (Boolean Set Column) ValuesLength():", colVec.ValuesLength())
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = set()
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.add(cell.Value(cellIdx))
 #                   print("Flatbuffers -> Python: (Boolean Set Element)", cell.Value(cellIdx))
  
                colVals.append(cellVals)
 #               print("Flatbuffers -> Python: (Boolean Set)[",idx,"]", cellVals)
                 
            table.add_column(colNames[j], colVals)

            
        elif col.Type() == _types_.LONG.value:
            colVec = col.LongColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
 #               print("Flatbuffers -> Python: (Long)", colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.LONG_LIST.value:
            colVec = col.LongListColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx))

                colVals.append(cellVals)
             
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.LONG_SET.value:
            colVec = col.LongSetColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = set()
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.add(cell.Value(cellIdx))
  
                colVals.append(cellVals)
                 
            table.add_column(colNames[j], colVals)

            
        elif col.Type() == _types_.DOUBLE.value:
            colVec = col.DoubleColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                colVals.append(colVec.Values(idx))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.DOUBLE_LIST.value:
            colVec = col.DoubleListColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx))

                colVals.append(cellVals)
             
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.DOUBLE_SET.value:
            colVec = col.DoubleSetColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = set()
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.add(cell.Value(cellIdx))
  
                colVals.append(cellVals)
                 
            table.add_column(colNames[j], colVals)

            
        elif col.Type() == _types_.STRING.value:
            colVec = col.StringColumn()
            colVals = []        
            for i in range(0, colVec.ValuesLength()):
                colVals.append(colVec.Values(i).decode('utf-8'))
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.STRING_LIST.value:
            colVec = col.StringListColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = []
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.append(cell.Value(cellIdx).decode('utf-8'))

                colVals.append(cellVals)
             
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.STRING_SET.value:
            colVec = col.StringSetColumn()
            colVals = []
            for idx in range(0,colVec.ValuesLength()):
                cell = colVec.Values(idx)
                cellVals = set()
                for cellIdx in range(0, cell.ValueLength()):
                    cellVals.add(cell.Value(cellIdx).decode('utf-8'))
  
                colVals.append(cellVals)
                 
            table.add_column(colNames[j], colVals)
            
        elif col.Type() == _types_.BYTES.value:
            colVec = col.ByteColumn()
            colVals = []        
            for i in range(0, colVec.ValuesLength()):
                cellBytes = []
                cell = colVec.Values(i)              
                for j in range(0, cell.ValueLength()):
                    cellBytes.append(cell.Value(j))
                 
                colVals.append(cellBytes)   
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
    
   # print("data_frame", table._data_frame)
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
            
        elif table.get_type(colIdx) == _types_.INTEGER_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(col[valIdx]))):
                    builder.PrependInt32(col[valIdx][cellIdx])
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                IntegerCollectionCell.IntegerCollectionCellStart(builder)
                IntegerCollectionCell.IntegerCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))
                        
            IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            IntCollectionColumn.IntCollectionColumnStart(builder)    
            IntCollectionColumn.IntCollectionColumnAddValues(builder,valVec)                                  
            colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.INTEGER_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                IntegerCollectionCell.IntegerCollectionCellStartValueVector(builder, len(col[valIdx]))
                for elem in col[valIdx]:
                    builder.PrependInt32(elem)
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                IntegerCollectionCell.IntegerCollectionCellStart(builder)
                IntegerCollectionCell.IntegerCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(IntegerCollectionCell.IntegerCollectionCellEnd(builder))
                        
            IntCollectionColumn.IntCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            IntCollectionColumn.IntCollectionColumnStart(builder)    
            IntCollectionColumn.IntCollectionColumnAddValues(builder,valVec)                                  
            colOffset = IntCollectionColumn.IntCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddIntSetColumn(builder, colOffset)
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
     #       print("Python->Flatbuffers: (Boolean List Start)")       
     #       print("Python->Flatbuffers: (Boolean List Column Length):", len(col))
            cellOffsets = []
            for valIdx in range(0,len(col)):
    #            print("Python->Flatbuffers: (Boolean List Element)[",valIdx,"]", col[valIdx])
               
                BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(col[valIdx]))):
       #             print("Python->Flatbuffers: (Boolean List Element)[",valIdx,",",cellIdx,"]", col[valIdx][cellIdx])
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
#                print("Python->Flatbuffers: (Boolean Set Element)[",valIdx,"]", col[valIdx])
               
                BooleanCollectionCell.BooleanCollectionCellStartValueVector(builder, len(col[valIdx]))
                for elem in col[valIdx]:
#                    print("Python->Flatbuffers: (Boolean Set Element)[",valIdx,"]", elem)
                    builder.PrependBool(elem)
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                BooleanCollectionCell.BooleanCollectionCellStart(builder)
                BooleanCollectionCell.BooleanCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(BooleanCollectionCell.BooleanCollectionCellEnd(builder))
                                       
            BooleanCollectionColumn.BooleanCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            BooleanCollectionColumn.BooleanCollectionColumnStart(builder)    
            BooleanCollectionColumn.BooleanCollectionColumnAddValues(builder,valVec)                                  
            colOffset = BooleanCollectionColumn.BooleanCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddBooleanSetColumn(builder, colOffset)
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
            
        elif table.get_type(colIdx) == _types_.LONG_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                LongCollectionCell.LongCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(col[valIdx]))):
                    builder.PrependInt64(col[valIdx][cellIdx])
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                LongCollectionCell.LongCollectionCellStart(builder)
                LongCollectionCell.LongCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))
                        
            LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            LongCollectionColumn.LongCollectionColumnStart(builder)    
            LongCollectionColumn.LongCollectionColumnAddValues(builder,valVec)                                  
            colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.LONG_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                LongCollectionCell.LongCollectionCellStartValueVector(builder, len(col[valIdx]))
                for elem in col[valIdx]:
                    builder.PrependInt64(elem)
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                LongCollectionCell.LongCollectionCellStart(builder)
                LongCollectionCell.LongCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(LongCollectionCell.LongCollectionCellEnd(builder))
                        
            LongCollectionColumn.LongCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            LongCollectionColumn.LongCollectionColumnStart(builder)    
            LongCollectionColumn.LongCollectionColumnAddValues(builder,valVec)                                  
            colOffset = LongCollectionColumn.LongCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddLongSetColumn(builder, colOffset)
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
            
        elif table.get_type(colIdx) == _types_.DOUBLE_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(col[valIdx]))):
                    builder.PrependFloat64(col[valIdx][cellIdx])
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                DoubleCollectionCell.DoubleCollectionCellStart(builder)
                DoubleCollectionCell.DoubleCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))
                        
            DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            DoubleCollectionColumn.DoubleCollectionColumnStart(builder)    
            DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder,valVec)                                  
            colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleListColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.DOUBLE_SET:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
               
                DoubleCollectionCell.DoubleCollectionCellStartValueVector(builder, len(col[valIdx]))
                for elem in col[valIdx]:
                    builder.PrependFloat64(elem)
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                DoubleCollectionCell.DoubleCollectionCellStart(builder)
                DoubleCollectionCell.DoubleCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(DoubleCollectionCell.DoubleCollectionCellEnd(builder))
                        
            DoubleCollectionColumn.DoubleCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            DoubleCollectionColumn.DoubleCollectionColumnStart(builder)    
            DoubleCollectionColumn.DoubleCollectionColumnAddValues(builder,valVec)                                  
            colOffset = DoubleCollectionColumn.DoubleCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddDoubleSetColumn(builder, colOffset)
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
            
        elif table.get_type(colIdx) == _types_.STRING_LIST:  
            col = table_column(table, colIdx)
            cellOffsets = []
            for valIdx in range(0,len(col)):
                
                strOffsets = []
#                print("Python->Flatbuffers: (String List Cell):", col[valIdx])
                for strIdx in range(0, len(col[valIdx])):
                    strOffsets.append(builder.CreateString(col[valIdx][strIdx]))
                   
               
                StringCollectionCell.StringCollectionCellStartValueVector(builder, len(strOffsets))
                for cellIdx in reversed(range(0, len(strOffsets))):
                    builder.PrependUOffsetTRelative(strOffsets[cellIdx])
                                     
                cellVec = builder.EndVector(len(strOffsets))
                StringCollectionCell.StringCollectionCellStart(builder)
                StringCollectionCell.StringCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))
                        
            StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            StringCollectionColumn.StringCollectionColumnStart(builder)    
            StringCollectionColumn.StringCollectionColumnAddValues(builder,valVec)                                  
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
                for elem in col[valIdx]:
                    strOffsets.append(builder.CreateString(elem))
               
                StringCollectionCell.StringCollectionCellStartValueVector(builder, len(col[valIdx]))
                for cellIdx in reversed(range(0, len(strOffsets))):
                    builder.PrependUOffsetTRelative(strOffsets[cellIdx])
                                     
                cellVec = builder.EndVector(len(col[valIdx]))
                StringCollectionCell.StringCollectionCellStart(builder)
                StringCollectionCell.StringCollectionCellAddValue(builder,cellVec)
                cellOffsets.append(StringCollectionCell.StringCollectionCellEnd(builder))
                        
            StringCollectionColumn.StringCollectionColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(cellOffsets))):
                builder.PrependUOffsetTRelative(cellOffsets[valIdx])
            valVec = builder.EndVector(len(cellOffsets))    
            
            StringCollectionColumn.StringCollectionColumnStart(builder)    
            StringCollectionColumn.StringCollectionColumnAddValues(builder,valVec)                                  
            colOffset = StringCollectionColumn.StringCollectionColumnEnd(builder)           
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddStringSetColumn(builder, colOffset)
            colOffsetList.append(Column.ColumnEnd(builder))
            
        elif table.get_type(colIdx) == _types_.BYTES:
          
            col = table_column(table, colIdx) 
          
            bytesOffsets = []
            for colIdx in range(0, len(col)):
                byteOffsets = []
                ByteCell.ByteCellStart(builder)
                ByteCell.ByteCellStartValueVector(builder, len(col[colIdx]))
                for byteIdx in range(reversed(0, len(col[colIdx]))):
                   ByteCell.ByteCellAddValue(builder, col[colIdx][byteIdx])
                byteOffsets.append(builder.EndVector(len(col[colIdx])))
                    
            
            ByteColumn.ByteColumnStartValuesVector(builder, len(col))
            for valIdx in reversed(range(0,len(bytesOffsets))):
                builder.PrependUOffsetTRelative(bytesOffsets[valIdx])
            valVec = builder.EndVector(len(col))
            ByteColumn.ByteColumnStart(builder)        
            ByteColumn.ByteColumnAddValues(builder, valVec)
            colOffset = ByteColumn.ByteColumnEnd(builder)
            Column.ColumnStart(builder)
            Column.ColumnAddType(builder, table.get_type(colIdx).value)
            Column.ColumnAddByteColumn(builder, colOffset)
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
      
