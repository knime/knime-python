package org.knime.flatbuffers;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.flatbuffers.flatc.Column;
import org.knime.flatbuffers.flatc.DoubleColumn;
import org.knime.flatbuffers.flatc.Header;
import org.knime.flatbuffers.flatc.IntColumn;
import org.knime.flatbuffers.flatc.KnimeTable;
import org.knime.flatbuffers.flatc.StringColumn;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableCreator;
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

		tableIterator.getTableSpec().getColumnTypes();
		for (String colName : tableIterator.getTableSpec().getColumnNames()) {
			columns.put(colName, new ArrayList<>());
		}

		while (tableIterator.hasNext()) {
			Row row = tableIterator.next();
			Iterator<Cell> rowIt = row.iterator();
			for (String colName : columns.keySet()) {
				Cell cell = rowIt.next();
				switch (cell.getColumnType()) {
				case BOOLEAN: {
					break;
				}
				case BOOLEAN_LIST: {
					break;
				}
				case BOOLEAN_SET: {
					break;
				}
				case INTEGER: {
					columns.get(colName).add(cell.getIntegerValue());
					break;
				}
				case INTEGER_LIST: {
					break;
				}
				case INTEGER_SET: {
					break;
				}
				case LONG: {
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
				break;
			}
			case BOOLEAN_LIST: {
				break;
			}
			case BOOLEAN_SET: {
				break;
			}
			case INTEGER: {
				int nameOffset = builder.createString(colName);

				int valuesOffset = IntColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Integer[0])));
				int colOffset = IntColumn.createIntColumn(builder, nameOffset, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Types.INTEGER);
				Column.addIntColumns(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER_LIST: {
				break;
			}
			case INTEGER_SET: {
				break;
			}
			case LONG: {
				break;
			}
			case LONG_LIST: {
				break;
			}
			case LONG_SET: {
				break;
			}
			case DOUBLE: {
				int nameOffset = builder.createString(colName);

				int valuesOffset = DoubleColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Double[0])));
				int colOffset = DoubleColumn.createDoubleColumn(builder, nameOffset, valuesOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.DOUBLE.getId());
				Column.addDoubleColumns(builder, colOffset);
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
				int nameOffset = builder.createString(colName);
				List<Integer> strOffsets = new ArrayList<>();
				for(Object obj : columns.get(colName)){
					strOffsets.add(builder.createString((String)obj));
				}
				int valuesOffset = StringColumn.createValuesVector(builder, ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[0])));
				int colOffset = StringColumn.createStringColumn(builder, nameOffset, valuesOffset);			
				Column.startColumn(builder);
				Column.addType(builder, Type.STRING.getId());
				Column.addStringColumns(builder, colOffset);
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
			colNameOffsets.add(Header.createHeader(builder, builder.createString(colName)));
		}
	
		int colNameVecOffset = KnimeTable.createHeadersVector(builder,
				ArrayUtils.toPrimitive(colNameOffsets.toArray(new Integer[0])));
		
		int colVecOffset = KnimeTable.createColumnsVector(builder,
				ArrayUtils.toPrimitive(colOffsets.toArray(new Integer[0])));
		KnimeTable.startKnimeTable(builder);

		KnimeTable.addHeaders(builder, colNameVecOffset);
		KnimeTable.addColumns(builder, colVecOffset);

		int knimeTable = KnimeTable.endKnimeTable(builder);
		builder.finish(knimeTable);
	
		return builder.sizedByteArray();
	}

	@Override
	public void bytesIntoTable(TableCreator tableCreator, byte[] bytes) {

		KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

		List<String> colNames = new ArrayList<>();
		Map<String, Type> colTypes = new HashMap<>();
		Map<String, List<Object>> columns = new LinkedHashMap<>();


		if (table.headersLength() == 0)
			return;
		
		for (int h = 0; h < table.headersLength(); h++) {
			Header hdr = table.headers(h);
			colNames.add(hdr.name());
			columns.put(hdr.name(), new ArrayList<>());
		}


		for (int j = 0; j < table.columnsLength(); j++) {
			
			Column col = table.columns(j);
			switch (Type.getTypeForId(col.type())) {
			case BOOLEAN: {
				break;
			}
			case BOOLEAN_LIST: {
				break;
			}
			case BOOLEAN_SET: {
				break;
			}
			case INTEGER: {
				IntColumn colVec = col.intColumns();
				colTypes.put(colVec.name(), Type.INTEGER);
				for(int i = 0; i < colVec.valuesLength(); i++){
					columns.get(colVec.name()).add(colVec.values(i));
				}	
				break;
			}
			case INTEGER_LIST: {
				break;
			}
			case INTEGER_SET: {
				break;
			}
			case LONG: {
				break;
			}
			case LONG_LIST: {
				break;
			}
			case LONG_SET: {
				break;
			}
			case DOUBLE: {
				DoubleColumn colVec = col.doubleColumns();
				colTypes.put(colVec.name(), Type.DOUBLE);
				for(int i = 0; i < colVec.valuesLength(); i++){
					columns.get(colVec.name()).add(colVec.values(i));
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
				StringColumn colVec = col.stringColumns();
				colTypes.put(colVec.name(), Type.STRING);
				for(int i = 0; i < colVec.valuesLength(); i++){
					columns.get(colVec.name()).add(colVec.values(i));
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

		if(columns.isEmpty())
			return;
		
		int numRows = columns.get(columns.keySet().iterator().next()).size();
		for (int rowCount = 0; rowCount < numRows; rowCount++) {
			
			Row r = new RowImpl("Row" + rowCount, columns.keySet().size());
			int colCount = 0;
			for (String colName : colNames) {
				switch (colTypes.get(colName)) {
				case BOOLEAN: {
					break;
				}
				case BOOLEAN_LIST: {
					break;
				}
				case BOOLEAN_SET: {
					break;
				}
				case INTEGER: {
					r.setCell(new CellImpl((Integer) columns.get(colName).get(rowCount), colName), colCount);
					break;
				}
				case INTEGER_LIST: {
					break;
				}
				case INTEGER_SET: {
					break;
				}
				case LONG: {
					break;
				}
				case LONG_LIST: {
					break;
				}
				case LONG_SET: {
					break;
				}
				case DOUBLE: {
					r.setCell(new CellImpl((Double) columns.get(colName).get(rowCount), colName), colCount);

					break;
				}
				case DOUBLE_LIST: {
					break;
				}
				case DOUBLE_SET: {
					break;
				}
				case STRING: {
					r.setCell(new CellImpl((String) columns.get(colName).get(rowCount), colName), colCount);
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
				colCount++;
			}
			tableCreator.addRow(r);

		}


	}

	@Override
	public TableSpec tableSpecFromBytes(byte[] bytes) {
		KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

		List<String> colNames = new ArrayList<>();
		Type[] types = new Type[table.headersLength()];
		
		for (int h = 0; h < table.headersLength(); h++) {
			Header hdr = table.headers(h);
			colNames.add(hdr.name());
		}
		
		

		for (int j = 0; j < table.columnsLength(); j++) {
			Column col = table.columns(j);
			switch (Type.getTypeForId(col.type())) {
			case BOOLEAN: {
				break;
			}
			case BOOLEAN_LIST: {
				break;
			}
			case BOOLEAN_SET: {
				break;
			}
			case INTEGER: {
				IntColumn colVec = col.intColumns();
				types[j] = Type.INTEGER;				
				break;
			}
			case INTEGER_LIST: {
				break;
			}
			case INTEGER_SET: {
				break;
			}
			case LONG: {
				break;
			}
			case LONG_LIST: {
				break;
			}
			case LONG_SET: {
				break;
			}
			case DOUBLE: {
				DoubleColumn colVec = col.doubleColumns();
				types[j] =  Type.DOUBLE;	

				break;
			}
			case DOUBLE_LIST: {
				break;
			}
			case DOUBLE_SET: {
				break;
			}
			case STRING: {
				StringColumn colVec = col.stringColumns();
				types[j] = Type.STRING;	
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
		
		return new TableSpecImpl(types, colNames.toArray(new String[0]));
	}

}
