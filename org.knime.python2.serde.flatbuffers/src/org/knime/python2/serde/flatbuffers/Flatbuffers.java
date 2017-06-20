package org.knime.python2.serde.flatbuffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.knime.python2.serde.flatbuffers.flatc.BooleanCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.BooleanCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.BooleanColumn;
import org.knime.python2.serde.flatbuffers.flatc.ByteCell;
import org.knime.python2.serde.flatbuffers.flatc.ByteCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.ByteCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.ByteColumn;
import org.knime.python2.serde.flatbuffers.flatc.Column;
import org.knime.python2.serde.flatbuffers.flatc.DoubleCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.DoubleCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.DoubleColumn;
import org.knime.python2.serde.flatbuffers.flatc.IntCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.IntColumn;
import org.knime.python2.serde.flatbuffers.flatc.IntegerCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.KnimeTable;
import org.knime.python2.serde.flatbuffers.flatc.LongCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.LongCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.LongColumn;
import org.knime.python2.serde.flatbuffers.flatc.StringCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.StringCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.StringColumn;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
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
	public byte[] tableToBytes(TableIterator tableIterator, SerializationOptions serializationOptions) {

		FlatBufferBuilder builder = new FlatBufferBuilder();
		Map<String, List<Object>> columns = new LinkedHashMap<>();
		Map<String, boolean[]> missing = new HashMap<>();

		for (String colName : tableIterator.getTableSpec().getColumnNames()) {
			columns.put(colName, new ArrayList<>());
			missing.put(colName, new boolean[tableIterator.getNumberRemainingRows()]);
		}
		
		Type[] colTypes = tableIterator.getTableSpec().getColumnTypes();

		List<Integer> rowIdOffsets = new ArrayList<>();

		int rowIdx = 0;
		int ctr = 0;
		// Convert the rows to columns
		while (tableIterator.hasNext()) {
			Row row = tableIterator.next();
			rowIdOffsets.add(builder.createString(row.getRowKey()));

			Iterator<Cell> rowIt = row.iterator();
			ctr = 0;
			for (String colName : columns.keySet()) {
				Cell cell = rowIt.next();

				if (cell.isMissing()) {
					Type type = colTypes[ctr];
					boolean appended = false;
					if(serializationOptions.getConvertMissingToPython()) {
						if(type == Type.INTEGER) {
							columns.get(colName).add(new Integer((int) serializationOptions.getSentinelForType(type)));
							appended = true;
						} else if(type == Type.LONG) {
							columns.get(colName).add(new Long(serializationOptions.getSentinelForType(type)));
							appended = true;
						}
					} 
					if(!appended) {
						columns.get(colName).add(getMissingValue(tableIterator.getTableSpec().getColumnTypes()[tableIterator
								.getTableSpec().findColumn(colName)]));
						missing.get(colName)[rowIdx] = true;
					}
				} else {
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
						columns.get(colName).add(cell.getLongArrayValue());
						break;
					}
					case LONG_SET: {
						columns.get(colName).add(cell.getLongArrayValue());
						break;
					}
					case DOUBLE: {
						columns.get(colName).add(cell.getDoubleValue());
						break;
					}
					case DOUBLE_LIST: {
						columns.get(colName).add(cell.getDoubleArrayValue());
						break;
					}
					case DOUBLE_SET: {
						columns.get(colName).add(cell.getDoubleArrayValue());
						break;
					}
					case STRING: {
						columns.get(colName).add(cell.getStringValue());
						break;
					}
					case STRING_LIST: {
						columns.get(colName).add(cell.getStringArrayValue());
						break;
					}
					case STRING_SET: {
						columns.get(colName).add(cell.getStringArrayValue());
						break;
					}
					case BYTES: {
						columns.get(colName).add(cell.getBytesValue());
						break;
					}
					case BYTES_LIST: {
						columns.get(colName).add(cell.getBytesArrayValue());
						break;
					}
					case BYTES_SET: {
						columns.get(colName).add(cell.getBytesArrayValue());
						break;
					}
					default:
						break;
					}
				}
				ctr++;
			}
			rowIdx++;
		}
		List<Integer> colOffsets = new ArrayList<>();

		int i = 0;

		for (String colName : columns.keySet()) {
			switch (colTypes[i]) {
			case BOOLEAN: {
				int valuesOffset = BooleanColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Boolean[columns.get(colName).size()])));
				int missingOffset = BooleanColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = BooleanColumn.createBooleanColumn(builder, valuesOffset, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN.getId());
				Column.addBooleanColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case BOOLEAN_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean[] missingCells = new boolean[((Boolean[]) o).length];

					int cIdx = 0;
					for (Boolean c : (Boolean[]) o) {
						if (c == null) {
							((Boolean[]) o)[cIdx] = false; // change to boolean
															// missing
							missingCells[cIdx] = true;
						}
						cIdx++;
					}

					int valuesOffset = BooleanCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive((Boolean[]) o));

					int missingCellsOffset = BooleanCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = BooleanCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN_LIST.getId());
				Column.addBooleanListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case BOOLEAN_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {

					boolean addMissingValue = false;
					List<Boolean> l = new ArrayList<>();
					for (Boolean c : (Boolean[]) o) {
						if (c == null) {
							addMissingValue = true;
						} else {
							l.add(c);
						}
					}
					int valuesOffset = BooleanCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(l.toArray(new Boolean[l.size()])));
					boolean[] missingCells = new boolean[((Boolean[]) o).length];
					int missingCellsOffset = BooleanCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = BooleanCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BOOLEAN_SET.getId());
				Column.addBooleanSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER: {
				int valuesOffset = IntColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Integer[columns.get(colName).size()])));
				int missingOffset = IntColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = IntColumn.createIntColumn(builder, valuesOffset, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER.getId());
				Column.addIntColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean[] missingCells = new boolean[((Integer[]) o).length];
					int cIdx = 0;
					for (Integer c : (Integer[]) o) {
						if (c == null) {
							((Integer[]) o)[cIdx] = Integer.MIN_VALUE;
							missingCells[cIdx] = true;
						}
						cIdx++;
					}

					int valuesOffset = IntegerCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive((Integer[]) o));
					int missingCellsOffset = IntegerCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = IntCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = IntCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER_LIST.getId());
				Column.addIntListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case INTEGER_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean addMissingValue = false;
					List<Integer> l = new ArrayList<>();
					for (Integer c : (Integer[]) o) {
						if (c == null) {
							addMissingValue = true;
						} else {
							l.add(c);
						}
					}
					int valuesOffset = IntegerCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(l.toArray(new Integer[l.size()])));

					boolean[] missingCells = new boolean[((Integer[]) o).length];
					int missingCellsOffset = IntegerCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = IntCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = IntCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.INTEGER_SET.getId());
				Column.addIntSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case LONG: {
				int valuesOffset = LongColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Long[columns.get(colName).size()])));
				int missingOffset = LongColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = LongColumn.createLongColumn(builder, valuesOffset, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.LONG.getId());
				Column.addLongColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case LONG_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean[] missingCells = new boolean[((Long[]) o).length];
					int cIdx = 0;
					for (Long c : (Long[]) o) {
						if (c == null) {
							((Long[]) o)[cIdx] = Long.MIN_VALUE;
							missingCells[cIdx] = true;
						}
						cIdx++;
					}

					int valuesOffset = LongCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive((Long[]) o));

					int missingCellsOffset = LongCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(LongCollectionCell.createLongCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = LongCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = LongCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = LongCollectionColumn.createLongCollectionColumn(builder, valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.LONG_LIST.getId());
				Column.addLongListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case LONG_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					List<Long> l = new ArrayList<>();
					boolean addMissingValue = false;
					for (Long c : (Long[]) o) {
						if (c == null) {
							addMissingValue = true;
						} else {
							l.add(c);
						}
					}

					int valuesOffset = LongCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(l.toArray(new Long[l.size()])));

					boolean[] missingCells = new boolean[((Long[]) o).length];
					int missingCellsOffset = LongCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(LongCollectionCell.createLongCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = LongCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = LongCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = LongCollectionColumn.createLongCollectionColumn(builder, valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.LONG_SET.getId());
				Column.addLongSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case DOUBLE: {
				int valuesOffset = DoubleColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(columns.get(colName).toArray(new Double[columns.get(colName).size()])));
				int missingOffset = DoubleColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = DoubleColumn.createDoubleColumn(builder, valuesOffset, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.DOUBLE.getId());
				Column.addDoubleColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case DOUBLE_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean[] missingCells = new boolean[((Double[]) o).length];
					int cIdx = 0;
					for (Double c : (Double[]) o) {
						if (c == null) {
							((Double[]) o)[cIdx] = Double.NaN;
							missingCells[cIdx] = true;
						}
						cIdx++;
					}
					int valuesOffset = DoubleCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive((Double[]) o));
					int missingCellsOffset = DoubleCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(DoubleCollectionCell.createDoubleCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = DoubleCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = DoubleCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = DoubleCollectionColumn.createDoubleCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.DOUBLE_LIST.getId());
				Column.addDoubleListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case DOUBLE_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
				for (Object o : columns.get(colName)) {
					boolean addMissingValue = false;
					List<Double> l = new ArrayList<>();
					for (Double c : (Double[]) o) {
						if (c == null) {
							addMissingValue = true;
						} else {
							l.add(c);
						}
					}
					int valuesOffset = DoubleCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(l.toArray(new Double[l.size()])));

					boolean[] missingCells = new boolean[((Double[]) o).length];
					int missingCellsOffset = DoubleCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(DoubleCollectionCell.createDoubleCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = DoubleCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = DoubleCollectionColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = DoubleCollectionColumn.createDoubleCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.DOUBLE_SET.getId());
				Column.addDoubleSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case STRING: {
				List<Integer> strOffsets = new ArrayList<>();
				for (Object obj : columns.get(colName)) {
					strOffsets.add(builder.createString((String) obj));
				}
				int valuesOffset = StringColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[columns.get(colName).size()])));
				int missingOffset = StringColumn.createMissingVector(builder, missing.get(colName));
				int colOffset = StringColumn.createStringColumn(builder, valuesOffset, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.STRING.getId());
				Column.addStringColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case STRING_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

				for (Object o : columns.get(colName)) {
					List<Integer> strOffsets = new ArrayList<>(((String[]) o).length);
					boolean[] missingCells = new boolean[((String[]) o).length];
					int cIdx = 0;
					for (String s : (String[]) o) {
						if (s == null) {
							strOffsets.add(builder.createString((String) getMissingValue(Type.STRING)));
							missingCells[cIdx] = true;
						} else {
							strOffsets.add(builder.createString(s));
							missingCells[cIdx] = false;
						}
						cIdx++;
					}
					int valuesOffset = StringCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[strOffsets.size()])));

					int missingCellsOffset = StringCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(StringCollectionCell.createStringCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = StringCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = StringCollectionColumn.createMissingVector(builder, missing.get(colName));

				int colOffset = StringCollectionColumn.createStringCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.STRING_LIST.getId());
				Column.addStringListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case STRING_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

				for (Object o : columns.get(colName)) {
					List<Integer> strOffsets = new ArrayList<>(((String[]) o).length);
					boolean addMissingValue = false;
					for (String s : (String[]) o) {
						if (s == null) {
							addMissingValue = true;
						} else {
							strOffsets.add(builder.createString(s));
						}
					}
					int valuesOffset = StringCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[strOffsets.size()])));

					boolean[] missingCells = new boolean[((String[]) o).length];
					int missingCellsOffset = StringCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(StringCollectionCell.createStringCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = StringCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
				int missingOffset = StringCollectionColumn.createMissingVector(builder, missing.get(colName));

				int colOffset = StringCollectionColumn.createStringCollectionColumn(builder, valuesVector,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.STRING_SET.getId());
				Column.addStringSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case BYTES: {
				List<Integer> bytesOffsets = new ArrayList<>();
				for (Object obj : columns.get(colName)) {
					int byteCellOffset = Integer.MIN_VALUE;
					if (obj.getClass().isArray()) {
						byteCellOffset = ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[]) obj));
					} else {
						byteCellOffset = ByteCell.createValueVector(builder, new byte[] { ((Byte) obj).byteValue() });
					}
					bytesOffsets.add(ByteCell.createByteCell(builder, byteCellOffset));
				}
				int valuesOffset = ByteColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(bytesOffsets.toArray(new Integer[columns.get(colName).size()])));

				String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
				int missingOffset = ByteColumn.createMissingVector(builder, missing.get(colName));

				int colOffset = ByteColumn.createByteColumn(builder, builder.createString(serializer), valuesOffset,
						missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BYTES.getId());
				Column.addByteColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));

				break;
			}
			case BYTES_LIST: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

				for (Object o : columns.get(colName)) {
					List<Integer> bytesCellOffsets = new ArrayList<>();
					boolean[] missingCells = null;
					if (o.getClass().getComponentType().getComponentType() == null) {
						missingCells = new boolean[0];
						int bytesCellValVec = ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[]) o));
						bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
					} else {
						
						missingCells = new boolean[((Byte[][])o).length];
						
						int cIdx = 0;
						for (Object b : (Byte[][]) o) {
							if (b == null) {
								int bytesCellValVec = ByteCell.createValueVector(builder,
										ArrayUtils.toPrimitive((Byte[])getMissingValue(Type.BYTES_LIST)));
								bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
								missingCells[cIdx] = true;
							} else {
								int bytesCellValVec = ByteCell.createValueVector(builder,
										ArrayUtils.toPrimitive((Byte[]) b));
								bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
								missingCells[cIdx] = false;
							}
							cIdx++;
						}
					}
					int valuesOffset = ByteCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(bytesCellOffsets.toArray(new Integer[bytesCellOffsets.size()])));

					int missingCellsOffset = ByteCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(ByteCollectionCell.createByteCollectionCell(builder, valuesOffset,
							missingCellsOffset, false));
				}

				int valuesVector = ByteCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));

				int missingOffset = ByteCollectionColumn.createMissingVector(builder, missing.get(colName));

				String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
				int colOffset = ByteCollectionColumn.createByteCollectionColumn(builder,
						builder.createString(serializer), valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BYTES_LIST.getId());
				Column.addByteListColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
				break;
			}
			case BYTES_SET: {
				List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

				for (Object o : columns.get(colName)) {
					List<Integer> bytesCellOffsets = new ArrayList<>();
					boolean addMissingValue = false;
					if (o.getClass().getComponentType().getComponentType() == null) {
						int bytesCellValVec = ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[]) o));
						bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
					} else {
						for (Object b : (Byte[][]) o) {
							if (b == null) {
								addMissingValue = true;
							} else {
								int bytesCellValVec = ByteCell.createValueVector(builder,
										ArrayUtils.toPrimitive((Byte[]) b));
								bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
							}
						}
					}
					int valuesOffset = ByteCollectionCell.createValueVector(builder,
							ArrayUtils.toPrimitive(bytesCellOffsets.toArray(new Integer[bytesCellOffsets.size()])));

					boolean[] missingCells = null;
					if (o.getClass().getComponentType().getComponentType() == null) {
						missingCells = new boolean[((Byte[]) o).length];
					} else {
						missingCells = new boolean[((Byte[][]) o).length];
					}
					int missingCellsOffset = ByteCollectionCell.createMissingVector(builder, missingCells);
					cellOffsets.add(ByteCollectionCell.createByteCollectionCell(builder, valuesOffset,
							missingCellsOffset, addMissingValue));
				}

				int valuesVector = ByteCollectionColumn.createValuesVector(builder,
						ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));

				int missingOffset = ByteCollectionColumn.createMissingVector(builder, missing.get(colName));

				String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
				int colOffset = ByteCollectionColumn.createByteCollectionColumn(builder,
						builder.createString(serializer), valuesVector, missingOffset);
				Column.startColumn(builder);
				Column.addType(builder, Type.BYTES_SET.getId());
				Column.addByteSetColumn(builder, colOffset);
				colOffsets.add(Column.endColumn(builder));
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

	private static Object getMissingValue(Type type) {
		switch (type) {
		case BOOLEAN: {
			return false;
		}
		case BOOLEAN_LIST: {
			return new Boolean[] { false };
		}
		case BOOLEAN_SET: {
			return new Boolean[] { false };
		}
		case INTEGER: {
			return Integer.MIN_VALUE;
		}
		case INTEGER_LIST: {
			return new Integer[] { Integer.MIN_VALUE };
		}
		case INTEGER_SET: {
			return new Integer[] { Integer.MIN_VALUE };
		}
		case LONG: {
			return Long.MIN_VALUE;
		}
		case LONG_LIST: {
			return new Long[] { Long.MIN_VALUE };
		}
		case LONG_SET: {
			return new Long[] { Long.MIN_VALUE };
		}
		case DOUBLE: {
			return Double.NaN;
		}
		case DOUBLE_LIST: {
			return new Double[] { Double.NaN };
		}
		case DOUBLE_SET: {
			return new Double[] { Double.NaN };
		}
		case STRING: {
			return "Missing Value";
		}
		case STRING_LIST: {
			return new String[] { "Missing Value" };
		}
		case STRING_SET: {
			return new String[] { "Missing Value" };
		}
		case BYTES: {
			return new Byte((byte) 0);
		}
		case BYTES_LIST: {
			return new Byte[] { 0 };
		}
		case BYTES_SET: {
			return new Byte[] { 0 };
		}
		default:
			break;

		}
		return null;
	}

	@Override
	public void bytesIntoTable(TableCreator<?> tableCreator, byte[] bytes, SerializationOptions serializationOptions) {

		KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

		List<String> rowIds = new ArrayList<>();
		List<String> colNames = new ArrayList<>();
		Map<String, Type> colTypes = new HashMap<>();
		Map<String, List<Object>> columns = new LinkedHashMap<>();
		Map<String, boolean[]> missing = new HashMap<>();

		if (table.colNamesLength() == 0)
			return;

		for (int id = 0; id < table.rowIDsLength(); id++) {
			String rowId = table.rowIDs(id);
			rowIds.add(rowId);
		}

		for (int h = 0; h < table.colNamesLength(); h++) {
			String colName = table.colNames(h);
			colNames.add(colName);
			columns.put(colName, new ArrayList<>());
			missing.put(colName, new boolean[rowIds.size()]);
		}

		for (int j = 0; j < table.columnsLength(); j++) {

			Column col = table.columns(j);
			switch (Type.getTypeForId(col.type())) {
			case BOOLEAN: {
				BooleanColumn colVec = col.booleanColumn();
				colTypes.put(table.colNames(j), Type.BOOLEAN);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
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
						if (cell.missing(k)) {
							l.add(null);
						} else {
							l.add(cell.value(k));
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Boolean[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
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
					if (cell.keepDummy()) {
						l.add(null);
					}
					columns.get(table.colNames(j)).add(l.toArray(new Boolean[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case INTEGER: {
				IntColumn colVec = col.intColumn();
				colTypes.put(table.colNames(j), Type.INTEGER);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					if(serializationOptions.getConvertMissingFromPython()
							&& serializationOptions.isSentinel(Type.INTEGER, colVec.values(i))) {
						missing.get(table.colNames(j))[i] = true;
					} else {
						missing.get(table.colNames(j))[i] = colVec.missing(i);
					}
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
						if (cell.missing(k)) {
							l.add(null);
						} else {
							l.add(cell.value(k));
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Integer[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
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
					if (cell.keepDummy()) {
						l.add(null);
					}
					columns.get(table.colNames(j)).add(l.toArray(new Integer[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case LONG: {
				LongColumn colVec = col.longColumn();
				colTypes.put(table.colNames(j), Type.LONG);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					if(serializationOptions.getConvertMissingFromPython()
							&& serializationOptions.isSentinel(Type.LONG, colVec.values(i))) {
						missing.get(table.colNames(j))[i] = true;
					} else {
						missing.get(table.colNames(j))[i] = colVec.missing(i);
					}
					columns.get(table.colNames(j)).add(colVec.values(i));	
				}
				break;
			}
			case LONG_LIST: {
				LongCollectionColumn colVec = col.longListColumn();
				colTypes.put(table.colNames(j), Type.LONG_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					LongCollectionCell cell = colVec.values(i);

					List<Long> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						if (cell.missing(k)) {
							l.add(null);
						} else {
							l.add(cell.value(k));
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Long[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case LONG_SET: {
				LongCollectionColumn colVec = col.longSetColumn();
				colTypes.put(table.colNames(j), Type.LONG_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					LongCollectionCell cell = colVec.values(i);

					List<Long> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					if (cell.keepDummy()) {
						l.add(null);
					}
					columns.get(table.colNames(j)).add(l.toArray(new Long[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case DOUBLE: {
				DoubleColumn colVec = col.doubleColumn();
				colTypes.put(table.colNames(j), Type.DOUBLE);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}

				break;
			}
			case DOUBLE_LIST: {
				DoubleCollectionColumn colVec = col.doubleListColumn();
				colTypes.put(table.colNames(j), Type.DOUBLE_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					DoubleCollectionCell cell = colVec.values(i);

					List<Double> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						if (cell.missing(k)) {
							l.add(null);
						} else {
							l.add(cell.value(k));
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Double[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case DOUBLE_SET: {
				DoubleCollectionColumn colVec = col.doubleSetColumn();
				colTypes.put(table.colNames(j), Type.DOUBLE_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					DoubleCollectionCell cell = colVec.values(i);

					List<Double> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					if (cell.keepDummy()) {
						l.add(null);
					}
					columns.get(table.colNames(j)).add(l.toArray(new Double[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case STRING: {
				StringColumn colVec = col.stringColumn();
				colTypes.put(table.colNames(j), Type.STRING);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					columns.get(table.colNames(j)).add(colVec.values(i));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case STRING_LIST: {
				StringCollectionColumn colVec = col.stringListColumn();
				colTypes.put(table.colNames(j), Type.STRING_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					StringCollectionCell cell = colVec.values(i);

					List<String> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						if (cell.missing(k)) {
							l.add(null);
						} else {
							l.add(cell.value(k));
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new String[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case STRING_SET: {
				StringCollectionColumn colVec = col.stringSetColumn();
				colTypes.put(table.colNames(j), Type.STRING_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					StringCollectionCell cell = colVec.values(i);

					List<String> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						l.add(cell.value(k));
					}
					if (cell.keepDummy()) {
						l.add(null);
					}
					columns.get(table.colNames(j)).add(l.toArray(new String[cell.valueLength()]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case BYTES: {

				ByteColumn colVec = col.byteColumn();
				colTypes.put(table.colNames(j), Type.BYTES);

				@SuppressWarnings("unused")
				int xxx = colVec.valuesLength();

				for (int i = 0; i < colVec.valuesLength(); i++) {
					ByteCell bc = colVec.values(i);
					Byte[] byteArray = new Byte[bc.valueLength()];
					for (int k = 0; k < bc.valueLength(); k++) {
						byteArray[k] = (byte) bc.value(k);
					}
					columns.get(table.colNames(j)).add(byteArray);
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}

				break;
			}
			case BYTES_LIST: {
				ByteCollectionColumn colVec = col.byteListColumn();
				colTypes.put(table.colNames(j), Type.BYTES_LIST);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					ByteCollectionCell cell = colVec.values(i);

					List<Byte[]> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						if (cell.missing(k)) {
							l.add(null);
						} else {
							Byte[] bb = new Byte[cell.value(k).valueLength()];
							for (int b = 0; b < cell.value(k).valueLength(); b++) {
								bb[b] = (byte) cell.value(k).value(b);
							}
							l.add(bb);
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Byte[cell.valueLength()][]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
				break;
			}
			case BYTES_SET: {
				ByteCollectionColumn colVec = col.byteSetColumn();
				colTypes.put(table.colNames(j), Type.BYTES_SET);
				for (int i = 0; i < colVec.valuesLength(); i++) {
					ByteCollectionCell cell = colVec.values(i);

					List<Byte[]> l = new ArrayList<>(cell.valueLength());
					for (int k = 0; k < cell.valueLength(); k++) {
						Byte[] bb = new Byte[cell.value(k).valueLength()];
						for (int b = 0; b < cell.value(k).valueLength(); b++) {
							bb[b] = (byte) cell.value(k).value(b);
						}
						l.add(bb);
						if (cell.keepDummy()) {
							l.add(null);
						}
					}
					columns.get(table.colNames(j)).add(l.toArray(new Byte[cell.valueLength()][]));
					missing.get(table.colNames(j))[i] = colVec.missing(i);
				}
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
				if (missing.get(colName)[rowCount]) {
					r.setCell(new CellImpl(), colCount);
				} else {

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

		Map<String, String> serializers = new HashMap<>();

		for (int h = 0; h < table.colNamesLength(); h++) {
			String colName = table.colNames(h);
			colNames.add(colName);
		}

		for (int j = 0; j < table.columnsLength(); j++) {
			Column col = table.columns(j);
			types[j] = Type.getTypeForId(col.type());

			switch (Type.getTypeForId(col.type())) {
			case BYTES: {
				serializers.put(colNames.get(j), col.byteColumn().serializer());
				break;
			}
			case BYTES_LIST: {
				serializers.put(colNames.get(j), col.byteListColumn().serializer());
				break;
			}
			case BYTES_SET: {
				serializers.put(colNames.get(j), col.byteSetColumn().serializer());
				break;
			}
			default:
				break;

			}

		}

		return new TableSpecImpl(types, colNames.toArray(new String[colNames.size()]), serializers);
	}
}
