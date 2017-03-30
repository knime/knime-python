package org.knime.flatbuffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.knime.flatbuffers.flatc.BooleanCollectionCell;
import org.knime.flatbuffers.flatc.BooleanCollectionColumn;
import org.knime.flatbuffers.flatc.BooleanColumn;
import org.knime.flatbuffers.flatc.Column;
import org.knime.flatbuffers.flatc.DoubleColumn;
import org.knime.flatbuffers.flatc.IntCollectionColumn;
import org.knime.flatbuffers.flatc.IntColumn;
import org.knime.flatbuffers.flatc.IntegerCollectionCell;
import org.knime.flatbuffers.flatc.KnimeTable;
import org.knime.flatbuffers.flatc.LongColumn;
import org.knime.flatbuffers.flatc.StringColumn;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * @author Oliver Sampson, University of Konstanz
 *
 */
public class Flatbuffers implements SerializationLibrary {

	@Override
	public byte[] tableToBytes(TableIterator tableIterator) {

		FlatBufferBuilder builder = new FlatBufferBuilder();
		Map<String, List<Object>> columns = new LinkedHashMap<>();

		for (String colName : tableIterator.getTableSpec().getColumnNames()) {
			columns.put(colName, new ArrayList<>());
		}

		List<Integer> rowIdOffsets = new ArrayList<>();

		// Convert the rows to columns
		while (tableIterator.hasNext()) {
			Row row = tableIterator.next();
			rowIdOffsets.add(builder.createString(row.getRowKey()));

			Iterator<Cell> rowIt = row.iterator();
			for (String colName : columns.keySet()) {
				Cell cell = rowIt.next();
				switch (cell.getColumnType()) {
				case BOOLEAN: {
					columns.get(colName).add(cell.getBooleanValue());
					break;
				}
				case BOOLEAN_LIST: {
					columns.get(colName).add(cell.getBooleanArrayValue());
					break;
				}
				case BOOLEAN_SET: {
					columns.get(colName).add(cell.getBooleanArrayValue());
					break;
				}
				case INTEGER: {
					columns.get(colName).add(cell.getIntegerValue());
					break;
				}
				case INTEGER_LIST: {
					columns.get(colName).add(cell.getIntegerArrayValue());
					break;
				}
				case INTEGER_SET: {
					columns.get(colName).add(cell.getIntegerArrayValue());
					break;
				}
				case LONG: {
					columns.get(colName).add(cell.getLongValue());
					break;
				}
				case LONG_LIST: {
					break;
				}
				case LONG_SET: {
					break;
				}
				case DOUBLE: {
					columns.get(colName).add(cell.getDoubleValue());
					break;
				}
				case DOUBLE_LIST: {
					break;
				}
				case DOUBLE_SET: {
					break;
				}
				case STRING: {
					columns.get(colName).add(cell.getStringValue());
					break;
				}
				case STRING_LIST: {
					break;
				}
				case STRING_SET: {
					break;
				}
				case BYTES: {
					columns.get(colName).add(cell.getBytesValue());
					break;
				}
				case BYTES_LIST: {
					break;
				}
				case BYTES_SET: {
					break;
				}
				default:
					break;

				}
			}
		}
		List<Integer> colOffsets = new ArrayList<>();

		int i = 0;
		Type[] colTypes = tableIterator.getTableSpec().getColumnTypes();
		for (String colName : columns.keySet()) {
			switch (colTypes[i]) {
			case BOOLEAN: {
				int valuesOffset = BooleanColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Boolean[columns.get(colName).size()])));
				int colOffset = BooleanColumn.createBooleanColumn(builder, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN.getId());
				Column.addBooleanColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case BOOLEAN_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					int valuesOffset = BooleanCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Boolean[]) o));
					
					cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset));
				}

				int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int colOffset = BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN_LIST.getId());
				Column.addBooleanListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case BOOLEAN_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					int valuesOffset = BooleanCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Boolean[]) o));
					
					cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset));
				}

				int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int colOffset = BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN_SET.getId());
				Column.addBooleanSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER: {
				int valuesOffset = IntColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Integer[columns.get(colName).size()])));
				int colOffset = IntColumn.createIntColumn(builder, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER.getId());
				Column.addIntColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					int valuesOffset = IntegerCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Integer[]) o));
					
					cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset));
				}

				int valuesVector = IntCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER_LIST.getId());
				Column.addIntListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					int valuesOffset = IntegerCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Integer[]) o));
					
					cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset));
				}

				int valuesVector = IntCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER_SET.getId());
				Column.addIntSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case LONG: {
				int valuesOffset = LongColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Long[columns.get(colName).size()])));
				int colOffset = LongColumn.createLongColumn(builder, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.LONG.getId());
				Column.addLongColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case LONG_LIST: {
				break;
			}
			case LONG_SET: {
				break;
			}
			case DOUBLE: {
				int valuesOffset = DoubleColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Double[columns.get(colName).size()])));
				int colOffset = DoubleColumn.createDoubleColumn(builder, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.DOUBLE.getId());
				Column.addDoubleColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case DOUBLE_LIST: {
				break;
			}
			case DOUBLE_SET: {
				break;
			}
			case STRING: {
				List<Integer> strOffsets = new ArrayList<>();
				for (Object obj : columns.get(colName)) {
					strOffsets.add(builder.createString((String) obj));
				}
				int valuesOffset = StringColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[columns.get(colName).size()])));
				int colOffset = StringColumn.createStringColumn(builder, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.STRING.getId());
				Column.addStringColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case STRING_LIST: {
				break;
			}
			case STRING_SET: {
				break;
			}
			case BYTES: {
				break;
			}
			case BYTES_LIST: {
				break;
			}
			case BYTES_SET: {
				break;
			}
			default:
				break;

			}
			i++;
		}

		List<Integer> colNameOffsets = new ArrayList<>();

		for (String colName : columns.keySet()) {
			colNameOffsets.add(builder.createString(colName));
		}

		int rowIdVecOffset = KnimeTable.createRowIDsVector(builder,
				ArrayUtils.toPrimitive(rowIdOffsets.toArray(new Integer[rowIdOffsets.size()])));

		int colNameVecOffset = KnimeTable.createColNamesVector(builder,
				ArrayUtils.toPrimitive(colNameOffsets.toArray(new Integer[colNameOffsets.size()])));

		int colVecOffset = KnimeTable.createColumnsVector(builder,
				ArrayUtils.toPrimitive(colOffsets.toArray(new Integer[colOffsets.size()])));

		KnimeTable.startKnimeTable(builder);
		KnimeTable.addRowIDs(builder, rowIdVecOffset);
		KnimeTable.addColNames(builder, colNameVecOffset);
		KnimeTable.addColumns(builder, colVecOffset);
		int knimeTable = KnimeTable.endKnimeTable(builder);
		builder.finish(knimeTable);

		return builder.sizedByteArray();
	}

	@Override
	public void bytesIntoTable(TableCreator tableCreator, byte[] bytes) {

		KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

		List<String> rowIds = new ArrayList<>();
		List<String> colNames = new ArrayList<>();
		Map<String, Type> colTypes = new HashMap<>();
		Map<String, List<Object>> columns = new LinkedHashMap<>();

		if (table.colNamesLength() == 0)
			return;

		for (int h = 0; h < table.colNamesLength(); h++) {
			String colName = table.colNames(h);
			colNames.add(colName);
			columns.put(colName, new ArrayList<>());
		}

		for (int id = 0; id < table.rowIDsLength(); id++) {
			String rowId = table.rowIDs(id);
			rowIds.add(rowId);
		}

		for (int j = 0; j < table.columnsLength(); j++) {

			Column col = table.columns(j);
			switch (Type.getTypeForId(col.type())) {
			case BOOLEAN: {
				BooleanColumn colVec = col.booleanColumn();
				colTypes.put(table.colNames(j), Type.BOOLEAN);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
				}
				break;
			}
			case BOOLEAN_LIST: {
				BooleanCollectionColumn colVec = col.booleanListColumn();
				colTypes.put(table.colNames(j), Type.BOOLEAN_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					BooleanCollectionCell cell = colVec.values(i);

					List<Boolean> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					columns.get(table.colNames(j))
							.add(l.toArray(new Boolean[cell.valueLength()]));
				}
				break;
			}
			case BOOLEAN_SET: {
				BooleanCollectionColumn colVec = col.booleanSetColumn();
				colTypes.put(table.colNames(j), Type.BOOLEAN_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					BooleanCollectionCell cell = colVec.values(i);

					List<Boolean> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					columns.get(table.colNames(j))
							.add(l.toArray(new Boolean[cell.valueLength()]));
				}
				break;
			}
			case INTEGER: {
				IntColumn colVec = col.intColumn();
				colTypes.put(table.colNames(j), Type.INTEGER);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
				}
				break;
			}
			case INTEGER_LIST: {
				IntCollectionColumn colVec = col.intListColumn();
				colTypes.put(table.colNames(j), Type.INTEGER_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					IntegerCollectionCell cell = colVec.values(i);

					List<Integer> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					columns.get(table.colNames(j))
							.add(l.toArray(new Integer[cell.valueLength()]));
				}
				break;
			}
			case INTEGER_SET: {
				IntCollectionColumn colVec = col.intSetColumn();
				colTypes.put(table.colNames(j), Type.INTEGER_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					IntegerCollectionCell cell = colVec.values(i);

					List<Integer> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					columns.get(table.colNames(j))
							.add(l.toArray(new Integer[cell.valueLength()]));
				}
				break;
			}
			case LONG: {
				LongColumn colVec = col.longColumn();
				colTypes.put(table.colNames(j), Type.LONG);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
				}
				break;
			}
			case LONG_LIST: {
				break;
			}
			case LONG_SET: {
				break;
			}
			case DOUBLE: {
				DoubleColumn colVec = col.doubleColumn();
				colTypes.put(table.colNames(j), Type.DOUBLE);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
				}

				break;
			}
			case DOUBLE_LIST: {
				break;
			}
			case DOUBLE_SET: {
				break;
			}
			case STRING: {
				StringColumn colVec = col.stringColumn();
				colTypes.put(table.colNames(j), Type.STRING);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
				}
				break;
			}
			case STRING_LIST: {
				break;
			}
			case STRING_SET: {
				break;
			}
			case BYTES: {
				break;
			}
			case BYTES_LIST: {
				break;
			}
			case BYTES_SET: {
				break;
			}
			default:
				break;

			}
		}

		if (columns.isEmpty())
			return;

		int numRows = columns.get(columns.keySet().iterator().next()).size();
		for (int rowCount = 0; rowCount < numRows; rowCount++) {

			Row r = new RowImpl(rowIds.get(rowCount), columns.keySet().size());
			int colCount = 0;
			for (String colName : colNames) {
				switch (colTypes.get(colName)) {
				case BOOLEAN: {
					r.setCell(new CellImpl((Boolean) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case BOOLEAN_LIST: {
					r.setCell(new CellImpl((Boolean[]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case BOOLEAN_SET: {
					r.setCell(new CellImpl((Boolean[]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				case INTEGER: {
					r.setCell(new CellImpl((Integer) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case INTEGER_LIST: {
					r.setCell(new CellImpl((Integer[]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case INTEGER_SET: {
					r.setCell(new CellImpl((Integer[]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				case LONG: {
					r.setCell(new CellImpl((Long) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case LONG_LIST: {
					r.setCell(new CellImpl((Long[]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case LONG_SET: {
					r.setCell(new CellImpl((Long[]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				case DOUBLE: {
					r.setCell(new CellImpl((Double) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case DOUBLE_LIST: {
					r.setCell(new CellImpl((Double[]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case DOUBLE_SET: {
					r.setCell(new CellImpl((Double[]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				case STRING: {
					r.setCell(new CellImpl((String) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case STRING_LIST: {
					r.setCell(new CellImpl((String[]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case STRING_SET: {
					r.setCell(new CellImpl((String[]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				case BYTES: {
					r.setCell(new CellImpl((Byte[]) columns.get(colName).get(rowCount)), colCount);
					break;
				}
				case BYTES_LIST: {
					r.setCell(new CellImpl((Byte[][]) columns.get(colName).get(rowCount), false), colCount);
					break;
				}
				case BYTES_SET: {
					r.setCell(new CellImpl((Byte[][]) columns.get(colName).get(rowCount), true), colCount);
					break;
				}
				default:
					break;

				}
				colCount++;
			}
			tableCreator.addRow(r);

		}

	}

	@Override
	public TableSpec tableSpecFromBytes(byte[] bytes) {
		KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

		List<String> colNames = new ArrayList<>();
		Type[] types = new Type[table.colNamesLength()];

		for (int h = 0; h < table.colNamesLength(); h++) {
			String colName = table.colNames(h);
			colNames.add(colName);
		}

		for (int j = 0; j < table.columnsLength(); j++) {
			Column col = table.columns(j);
			switch (Type.getTypeForId(col.type())) {
			case BOOLEAN: {
				BooleanColumn colVec = col.booleanColumn();
				types[j] = Type.BOOLEAN;
				break;
			}
			case BOOLEAN_LIST: {
				types[j] = Type.BOOLEAN_LIST;
				break;
			}
			case BOOLEAN_SET: {
				types[j] = Type.BOOLEAN_SET;
				break;
			}
			case INTEGER: {
				IntColumn colVec = col.intColumn();
				types[j] = Type.INTEGER;
				break;
			}
			case INTEGER_LIST: {
				types[j] = Type.INTEGER_LIST;
				break;
			}
			case INTEGER_SET: {
				types[j] = Type.INTEGER_SET;
				break;
			}
			case LONG: {
				types[j] = Type.LONG;
				break;
			}
			case LONG_LIST: {
				types[j] = Type.LONG_LIST;
				break;
			}
			case LONG_SET: {
				types[j] = Type.LONG_SET;
				break;
			}
			case DOUBLE: {
				DoubleColumn colVec = col.doubleColumn();
				types[j] = Type.DOUBLE;
				break;
			}
			case DOUBLE_LIST: {
				types[j] = Type.DOUBLE_LIST;
				break;
			}
			case DOUBLE_SET: {
				types[j] = Type.DOUBLE_SET;
				break;
			}
			case STRING: {
				StringColumn colVec = col.stringColumn();
				types[j] = Type.STRING;
				break;
			}
			case STRING_LIST: {
				types[j] = Type.STRING_LIST;
				break;
			}
			case STRING_SET: {
				types[j] = Type.STRING_SET;
				break;
			}
			case BYTES: {
				types[j] = Type.BYTES;
				break;
			}
			case BYTES_LIST: {
				types[j] = Type.BYTES_LIST;
				break;
			}
			case BYTES_SET: {
				types[j] = Type.BYTES_SET;
				break;
			}
			default:
				break;

			}
		}

		return new TableSpecImpl(types, colNames.toArray(new String[colNames.size()]), new HashMap<String, String>());
	}

}
