/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */

package org.knime.python2.serde.flatbuffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
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

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Serializes KNIME tables using the google flatbuffers library according to the shema found in:
 * flatbuffersSchema/schemaCol.fbs
 *
 * @author Oliver Sampson, University of Konstanz
 *
 */
public class Flatbuffers implements SerializationLibrary {

    @Override
    public byte[] tableToBytes(final TableIterator tableIterator, final SerializationOptions serializationOptions) {

        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final Map<String, List<Object>> columns = new LinkedHashMap<>();
        final Map<String, boolean[]> missing = new HashMap<>();

        for (final String colName : tableIterator.getTableSpec().getColumnNames()) {
            columns.put(colName, new ArrayList<>());
            missing.put(colName, new boolean[tableIterator.getNumberRemainingRows()]);
        }

        final Type[] colTypes = tableIterator.getTableSpec().getColumnTypes();

        final List<Integer> rowIdOffsets = new ArrayList<>();

        int rowIdx = 0;
        int ctr = 0;
        // Convert the rows to columns
        while (tableIterator.hasNext()) {
            final Row row = tableIterator.next();
            rowIdOffsets.add(builder.createString(row.getRowKey()));

            final Iterator<Cell> rowIt = row.iterator();
            ctr = 0;
            for (final String colName : columns.keySet()) {
                final Cell cell = rowIt.next();

                if (cell.isMissing()) {
                    final Type type = colTypes[ctr];
                    boolean appended = false;
                    if (serializationOptions.getConvertMissingToPython()) {
                        if (type == Type.INTEGER) {
                            columns.get(colName).add(new Integer((int)serializationOptions.getSentinelForType(type)));
                            appended = true;
                        } else if (type == Type.LONG) {
                            columns.get(colName).add(new Long(serializationOptions.getSentinelForType(type)));
                            appended = true;
                        }
                    }
                    if (!appended) {
                        columns.get(colName).add(getMissingValue(tableIterator.getTableSpec()
                            .getColumnTypes()[tableIterator.getTableSpec().findColumn(colName)]));
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
        final List<Integer> colOffsets = new ArrayList<>();

        int i = 0;

        for (final String colName : columns.keySet()) {
            switch (colTypes[i]) {
                case BOOLEAN: {
                    final int valuesOffset = BooleanColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(columns.get(colName).toArray(new Boolean[columns.get(colName).size()])));
                    final int missingOffset = BooleanColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = BooleanColumn.createBooleanColumn(builder, valuesOffset, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.BOOLEAN.getId());
                    Column.addBooleanColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));

                    break;
                }
                case BOOLEAN_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        final boolean[] missingCells = new boolean[((Boolean[])o).length];

                        int cIdx = 0;
                        for (final Boolean c : (Boolean[])o) {
                            if (c == null) {
                                ((Boolean[])o)[cIdx] = false; // change to boolean
                                // missing
                                missingCells[cIdx] = true;
                            }
                            cIdx++;
                        }

                        final int valuesOffset =
                                BooleanCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Boolean[])o));

                        final int missingCellsOffset = BooleanCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = BooleanCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.BOOLEAN_LIST.getId());
                    Column.addBooleanListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case BOOLEAN_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {

                        boolean addMissingValue = false;
                        final List<Boolean> l = new ArrayList<>();
                        for (final Boolean c : (Boolean[])o) {
                            if (c == null) {
                                addMissingValue = true;
                            } else {
                                l.add(c);
                            }
                        }
                        final int valuesOffset = BooleanCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(l.toArray(new Boolean[l.size()])));
                        final boolean[] missingCells = new boolean[((Boolean[])o).length];
                        final int missingCellsOffset = BooleanCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(BooleanCollectionCell.createBooleanCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = BooleanCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = BooleanCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            BooleanCollectionColumn.createBooleanCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.BOOLEAN_SET.getId());
                    Column.addBooleanSetColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case INTEGER: {
                    final int valuesOffset = IntColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(columns.get(colName).toArray(new Integer[columns.get(colName).size()])));
                    final int missingOffset = IntColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = IntColumn.createIntColumn(builder, valuesOffset, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.INTEGER.getId());
                    Column.addIntColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case INTEGER_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        final boolean[] missingCells = new boolean[((Integer[])o).length];
                        int cIdx = 0;
                        for (final Integer c : (Integer[])o) {
                            if (c == null) {
                                ((Integer[])o)[cIdx] = Integer.MIN_VALUE;
                                missingCells[cIdx] = true;
                            }
                            cIdx++;
                        }

                        final int valuesOffset =
                                IntegerCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Integer[])o));
                        final int missingCellsOffset = IntegerCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = IntCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = IntCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.INTEGER_LIST.getId());
                    Column.addIntListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case INTEGER_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        boolean addMissingValue = false;
                        final List<Integer> l = new ArrayList<>();
                        for (final Integer c : (Integer[])o) {
                            if (c == null) {
                                addMissingValue = true;
                            } else {
                                l.add(c);
                            }
                        }
                        final int valuesOffset = IntegerCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(l.toArray(new Integer[l.size()])));

                        final boolean[] missingCells = new boolean[((Integer[])o).length];
                        final int missingCellsOffset = IntegerCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(IntegerCollectionCell.createIntegerCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = IntCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = IntCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = IntCollectionColumn.createIntCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.INTEGER_SET.getId());
                    Column.addIntSetColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case LONG: {
                    final int valuesOffset = LongColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(columns.get(colName).toArray(new Long[columns.get(colName).size()])));
                    final int missingOffset = LongColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = LongColumn.createLongColumn(builder, valuesOffset, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.LONG.getId());
                    Column.addLongColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case LONG_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        final boolean[] missingCells = new boolean[((Long[])o).length];
                        int cIdx = 0;
                        for (final Long c : (Long[])o) {
                            if (c == null) {
                                ((Long[])o)[cIdx] = Long.MIN_VALUE;
                                missingCells[cIdx] = true;
                            }
                            cIdx++;
                        }

                        final int valuesOffset =
                                LongCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Long[])o));

                        final int missingCellsOffset = LongCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(LongCollectionCell.createLongCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = LongCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = LongCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            LongCollectionColumn.createLongCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.LONG_LIST.getId());
                    Column.addLongListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case LONG_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        final List<Long> l = new ArrayList<>();
                        boolean addMissingValue = false;
                        for (final Long c : (Long[])o) {
                            if (c == null) {
                                addMissingValue = true;
                            } else {
                                l.add(c);
                            }
                        }

                        final int valuesOffset = LongCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(l.toArray(new Long[l.size()])));

                        final boolean[] missingCells = new boolean[((Long[])o).length];
                        final int missingCellsOffset = LongCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(LongCollectionCell.createLongCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = LongCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = LongCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            LongCollectionColumn.createLongCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.LONG_SET.getId());
                    Column.addLongSetColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case DOUBLE: {
                    final int valuesOffset = DoubleColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(columns.get(colName).toArray(new Double[columns.get(colName).size()])));
                    final int missingOffset = DoubleColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = DoubleColumn.createDoubleColumn(builder, valuesOffset, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.DOUBLE.getId());
                    Column.addDoubleColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));

                    break;
                }
                case DOUBLE_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        final boolean[] missingCells = new boolean[((Double[])o).length];
                        int cIdx = 0;
                        for (final Double c : (Double[])o) {
                            if (c == null) {
                                ((Double[])o)[cIdx] = Double.NaN;
                                missingCells[cIdx] = true;
                            }
                            cIdx++;
                        }
                        final int valuesOffset =
                                DoubleCollectionCell.createValueVector(builder, ArrayUtils.toPrimitive((Double[])o));
                        final int missingCellsOffset = DoubleCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(DoubleCollectionCell.createDoubleCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = DoubleCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = DoubleCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            DoubleCollectionColumn.createDoubleCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.DOUBLE_LIST.getId());
                    Column.addDoubleListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case DOUBLE_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());
                    for (final Object o : columns.get(colName)) {
                        boolean addMissingValue = false;
                        final List<Double> l = new ArrayList<>();
                        for (final Double c : (Double[])o) {
                            if (c == null) {
                                addMissingValue = true;
                            } else {
                                l.add(c);
                            }
                        }
                        final int valuesOffset = DoubleCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(l.toArray(new Double[l.size()])));

                        final boolean[] missingCells = new boolean[((Double[])o).length];
                        final int missingCellsOffset = DoubleCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(DoubleCollectionCell.createDoubleCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = DoubleCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = DoubleCollectionColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset =
                            DoubleCollectionColumn.createDoubleCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.DOUBLE_SET.getId());
                    Column.addDoubleSetColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case STRING: {
                    final List<Integer> strOffsets = new ArrayList<>();
                    for (final Object obj : columns.get(colName)) {
                        strOffsets.add(builder.createString((String)obj));
                    }
                    final int valuesOffset = StringColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[columns.get(colName).size()])));
                    final int missingOffset = StringColumn.createMissingVector(builder, missing.get(colName));
                    final int colOffset = StringColumn.createStringColumn(builder, valuesOffset, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.STRING.getId());
                    Column.addStringColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));

                    break;
                }
                case STRING_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

                    for (final Object o : columns.get(colName)) {
                        final List<Integer> strOffsets = new ArrayList<>(((String[])o).length);
                        final boolean[] missingCells = new boolean[((String[])o).length];
                        int cIdx = 0;
                        for (final String s : (String[])o) {
                            if (s == null) {
                                strOffsets.add(builder.createString((String)getMissingValue(Type.STRING)));
                                missingCells[cIdx] = true;
                            } else {
                                strOffsets.add(builder.createString(s));
                                missingCells[cIdx] = false;
                            }
                            cIdx++;
                        }
                        final int valuesOffset = StringCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[strOffsets.size()])));

                        final int missingCellsOffset = StringCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(StringCollectionCell.createStringCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = StringCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = StringCollectionColumn.createMissingVector(builder, missing.get(colName));

                    final int colOffset =
                            StringCollectionColumn.createStringCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.STRING_LIST.getId());
                    Column.addStringListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case STRING_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

                    for (final Object o : columns.get(colName)) {
                        final List<Integer> strOffsets = new ArrayList<>(((String[])o).length);
                        boolean addMissingValue = false;
                        for (final String s : (String[])o) {
                            if (s == null) {
                                addMissingValue = true;
                            } else {
                                strOffsets.add(builder.createString(s));
                            }
                        }
                        final int valuesOffset = StringCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(strOffsets.toArray(new Integer[strOffsets.size()])));

                        final boolean[] missingCells = new boolean[((String[])o).length];
                        final int missingCellsOffset = StringCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(StringCollectionCell.createStringCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = StringCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));
                    final int missingOffset = StringCollectionColumn.createMissingVector(builder, missing.get(colName));

                    final int colOffset =
                            StringCollectionColumn.createStringCollectionColumn(builder, valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.STRING_SET.getId());
                    Column.addStringSetColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case BYTES: {
                    final List<Integer> bytesOffsets = new ArrayList<>();
                    for (final Object obj : columns.get(colName)) {
                        int byteCellOffset = Integer.MIN_VALUE;
                        if (obj.getClass().isArray()) {
                            byteCellOffset = ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[])obj));
                        } else {
                            byteCellOffset = ByteCell.createValueVector(builder, new byte[]{((Byte)obj).byteValue()});
                        }
                        bytesOffsets.add(ByteCell.createByteCell(builder, byteCellOffset));
                    }
                    final int valuesOffset = ByteColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(bytesOffsets.toArray(new Integer[columns.get(colName).size()])));

                    final String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
                    final int missingOffset = ByteColumn.createMissingVector(builder, missing.get(colName));

                    final int colOffset = ByteColumn.createByteColumn(builder, builder.createString(serializer), valuesOffset,
                        missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.BYTES.getId());
                    Column.addByteColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));

                    break;
                }
                case BYTES_LIST: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

                    for (final Object o : columns.get(colName)) {
                        final List<Integer> bytesCellOffsets = new ArrayList<>();
                        boolean[] missingCells = null;
                        if (o.getClass().getComponentType().getComponentType() == null) {
                            missingCells = new boolean[0];
                            final int bytesCellValVec =
                                    ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[])o));
                            bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
                        } else {

                            missingCells = new boolean[((Byte[][])o).length];

                            int cIdx = 0;
                            for (final Object b : (Byte[][])o) {
                                if (b == null) {
                                    final int bytesCellValVec = ByteCell.createValueVector(builder,
                                        ArrayUtils.toPrimitive((Byte[])getMissingValue(Type.BYTES_LIST)));
                                    bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
                                    missingCells[cIdx] = true;
                                } else {
                                    final int bytesCellValVec =
                                            ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[])b));
                                    bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
                                    missingCells[cIdx] = false;
                                }
                                cIdx++;
                            }
                        }
                        final int valuesOffset = ByteCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(bytesCellOffsets.toArray(new Integer[bytesCellOffsets.size()])));

                        final int missingCellsOffset = ByteCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(ByteCollectionCell.createByteCollectionCell(builder, valuesOffset,
                            missingCellsOffset, false));
                    }

                    final int valuesVector = ByteCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));

                    final int missingOffset = ByteCollectionColumn.createMissingVector(builder, missing.get(colName));

                    final String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
                    final int colOffset = ByteCollectionColumn.createByteCollectionColumn(builder,
                        builder.createString(serializer), valuesVector, missingOffset);
                    Column.startColumn(builder);
                    Column.addType(builder, Type.BYTES_LIST.getId());
                    Column.addByteListColumn(builder, colOffset);
                    colOffsets.add(Column.endColumn(builder));
                    break;
                }
                case BYTES_SET: {
                    final List<Integer> cellOffsets = new ArrayList<>(columns.get(colName).size());

                    for (final Object o : columns.get(colName)) {
                        final List<Integer> bytesCellOffsets = new ArrayList<>();
                        boolean addMissingValue = false;
                        if (o.getClass().getComponentType().getComponentType() == null) {
                            final int bytesCellValVec =
                                    ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[])o));
                            bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
                        } else {
                            for (final Object b : (Byte[][])o) {
                                if (b == null) {
                                    addMissingValue = true;
                                } else {
                                    final int bytesCellValVec =
                                            ByteCell.createValueVector(builder, ArrayUtils.toPrimitive((Byte[])b));
                                    bytesCellOffsets.add(ByteCell.createByteCell(builder, bytesCellValVec));
                                }
                            }
                        }
                        final int valuesOffset = ByteCollectionCell.createValueVector(builder,
                            ArrayUtils.toPrimitive(bytesCellOffsets.toArray(new Integer[bytesCellOffsets.size()])));

                        boolean[] missingCells = null;
                        if (o.getClass().getComponentType().getComponentType() == null) {
                            missingCells = new boolean[((Byte[])o).length];
                        } else {
                            missingCells = new boolean[((Byte[][])o).length];
                        }
                        final int missingCellsOffset = ByteCollectionCell.createMissingVector(builder, missingCells);
                        cellOffsets.add(ByteCollectionCell.createByteCollectionCell(builder, valuesOffset,
                            missingCellsOffset, addMissingValue));
                    }

                    final int valuesVector = ByteCollectionColumn.createValuesVector(builder,
                        ArrayUtils.toPrimitive(cellOffsets.toArray(new Integer[cellOffsets.size()])));

                    final int missingOffset = ByteCollectionColumn.createMissingVector(builder, missing.get(colName));

                    final String serializer = tableIterator.getTableSpec().getColumnSerializers().get(colName);
                    final int colOffset = ByteCollectionColumn.createByteCollectionColumn(builder,
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

        final List<Integer> colNameOffsets = new ArrayList<>();

        for (final String colName : columns.keySet()) {
            colNameOffsets.add(builder.createString(colName));
        }

        final int rowIdVecOffset = KnimeTable.createRowIDsVector(builder,
            ArrayUtils.toPrimitive(rowIdOffsets.toArray(new Integer[rowIdOffsets.size()])));

        final int colNameVecOffset = KnimeTable.createColNamesVector(builder,
            ArrayUtils.toPrimitive(colNameOffsets.toArray(new Integer[colNameOffsets.size()])));

        final int colVecOffset = KnimeTable.createColumnsVector(builder,
            ArrayUtils.toPrimitive(colOffsets.toArray(new Integer[colOffsets.size()])));

        KnimeTable.startKnimeTable(builder);
        KnimeTable.addRowIDs(builder, rowIdVecOffset);
        KnimeTable.addColNames(builder, colNameVecOffset);
        KnimeTable.addColumns(builder, colVecOffset);
        final int knimeTable = KnimeTable.endKnimeTable(builder);
        builder.finish(knimeTable);

        return builder.sizedByteArray();
    }

    private static Object getMissingValue(final Type type) {
        switch (type) {
            case BOOLEAN: {
                return false;
            }
            case BOOLEAN_LIST: {
                return new Boolean[]{false};
            }
            case BOOLEAN_SET: {
                return new Boolean[]{false};
            }
            case INTEGER: {
                return Integer.MIN_VALUE;
            }
            case INTEGER_LIST: {
                return new Integer[]{Integer.MIN_VALUE};
            }
            case INTEGER_SET: {
                return new Integer[]{Integer.MIN_VALUE};
            }
            case LONG: {
                return Long.MIN_VALUE;
            }
            case LONG_LIST: {
                return new Long[]{Long.MIN_VALUE};
            }
            case LONG_SET: {
                return new Long[]{Long.MIN_VALUE};
            }
            case DOUBLE: {
                return Double.NaN;
            }
            case DOUBLE_LIST: {
                return new Double[]{Double.NaN};
            }
            case DOUBLE_SET: {
                return new Double[]{Double.NaN};
            }
            case STRING: {
                return "Missing Value";
            }
            case STRING_LIST: {
                return new String[]{"Missing Value"};
            }
            case STRING_SET: {
                return new String[]{"Missing Value"};
            }
            case BYTES: {
                return new Byte((byte)0);
            }
            case BYTES_LIST: {
                return new Byte[]{0};
            }
            case BYTES_SET: {
                return new Byte[]{0};
            }
            default:
                break;

        }
        return null;
    }

    @Override
    public void bytesIntoTable(final TableCreator<?> tableCreator, final byte[] bytes,
        final SerializationOptions serializationOptions) {

        final KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

        final List<String> rowIds = new ArrayList<>();
        final List<String> colNames = new ArrayList<>();
        final Map<String, Type> colTypes = new HashMap<>();
        final Map<String, List<Object>> columns = new LinkedHashMap<>();
        final Map<String, boolean[]> missing = new HashMap<>();

        if (table.colNamesLength() == 0) {
            return;
        }

        for (int id = 0; id < table.rowIDsLength(); id++) {
            final String rowId = table.rowIDs(id);
            rowIds.add(rowId);
        }

        for (int h = 0; h < table.colNamesLength(); h++) {
            final String colName = table.colNames(h);
            colNames.add(colName);
            columns.put(colName, new ArrayList<>());
            missing.put(colName, new boolean[rowIds.size()]);
        }

        for (int j = 0; j < table.columnsLength(); j++) {

            final Column col = table.columns(j);
            switch (Type.getTypeForId(col.type())) {
                case BOOLEAN: {
                    final BooleanColumn colVec = col.booleanColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        columns.get(table.colNames(j)).add(colVec.values(i));
                        missing.get(table.colNames(j))[i] = colVec.missing(i);
                    }
                    break;
                }
                case BOOLEAN_LIST: {
                    final BooleanCollectionColumn colVec = col.booleanListColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final BooleanCollectionCell cell = colVec.values(i);

                        final List<Boolean> l = new ArrayList<>(cell.valueLength());
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
                    final BooleanCollectionColumn colVec = col.booleanSetColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final BooleanCollectionCell cell = colVec.values(i);

                        final List<Boolean> l = new ArrayList<>(cell.valueLength());
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
                    final IntColumn colVec = col.intColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        if (serializationOptions.getConvertMissingFromPython()
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
                    final IntCollectionColumn colVec = col.intListColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final IntegerCollectionCell cell = colVec.values(i);

                        final List<Integer> l = new ArrayList<>(cell.valueLength());
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
                    final IntCollectionColumn colVec = col.intSetColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final IntegerCollectionCell cell = colVec.values(i);

                        final List<Integer> l = new ArrayList<>(cell.valueLength());
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
                    final LongColumn colVec = col.longColumn();
                    colTypes.put(table.colNames(j), Type.LONG);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        if (serializationOptions.getConvertMissingFromPython()
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
                    final LongCollectionColumn colVec = col.longListColumn();
                    colTypes.put(table.colNames(j), Type.LONG_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final LongCollectionCell cell = colVec.values(i);

                        final List<Long> l = new ArrayList<>(cell.valueLength());
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
                    final LongCollectionColumn colVec = col.longSetColumn();
                    colTypes.put(table.colNames(j), Type.LONG_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final LongCollectionCell cell = colVec.values(i);

                        final List<Long> l = new ArrayList<>(cell.valueLength());
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
                    final DoubleColumn colVec = col.doubleColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        columns.get(table.colNames(j)).add(colVec.values(i));
                        missing.get(table.colNames(j))[i] = colVec.missing(i);
                    }

                    break;
                }
                case DOUBLE_LIST: {
                    final DoubleCollectionColumn colVec = col.doubleListColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final DoubleCollectionCell cell = colVec.values(i);

                        final List<Double> l = new ArrayList<>(cell.valueLength());
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
                    final DoubleCollectionColumn colVec = col.doubleSetColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final DoubleCollectionCell cell = colVec.values(i);

                        final List<Double> l = new ArrayList<>(cell.valueLength());
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
                    final StringColumn colVec = col.stringColumn();
                    colTypes.put(table.colNames(j), Type.STRING);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        columns.get(table.colNames(j)).add(colVec.values(i));
                        missing.get(table.colNames(j))[i] = colVec.missing(i);
                    }
                    break;
                }
                case STRING_LIST: {
                    final StringCollectionColumn colVec = col.stringListColumn();
                    colTypes.put(table.colNames(j), Type.STRING_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final StringCollectionCell cell = colVec.values(i);

                        final List<String> l = new ArrayList<>(cell.valueLength());
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
                    final StringCollectionColumn colVec = col.stringSetColumn();
                    colTypes.put(table.colNames(j), Type.STRING_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final StringCollectionCell cell = colVec.values(i);

                        final List<String> l = new ArrayList<>(cell.valueLength());
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

                    final ByteColumn colVec = col.byteColumn();
                    colTypes.put(table.colNames(j), Type.BYTES);

                    @SuppressWarnings("unused")
                    final
                    int xxx = colVec.valuesLength();

                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final ByteCell bc = colVec.values(i);
                        final Byte[] byteArray = new Byte[bc.valueLength()];
                        for (int k = 0; k < bc.valueLength(); k++) {
                            byteArray[k] = (byte)bc.value(k);
                        }
                        columns.get(table.colNames(j)).add(byteArray);
                        missing.get(table.colNames(j))[i] = colVec.missing(i);
                    }

                    break;
                }
                case BYTES_LIST: {
                    final ByteCollectionColumn colVec = col.byteListColumn();
                    colTypes.put(table.colNames(j), Type.BYTES_LIST);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final ByteCollectionCell cell = colVec.values(i);

                        final List<Byte[]> l = new ArrayList<>(cell.valueLength());
                        for (int k = 0; k < cell.valueLength(); k++) {
                            if (cell.missing(k)) {
                                l.add(null);
                            } else {
                                final Byte[] bb = new Byte[cell.value(k).valueLength()];
                                for (int b = 0; b < cell.value(k).valueLength(); b++) {
                                    bb[b] = (byte)cell.value(k).value(b);
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
                    final ByteCollectionColumn colVec = col.byteSetColumn();
                    colTypes.put(table.colNames(j), Type.BYTES_SET);
                    for (int i = 0; i < colVec.valuesLength(); i++) {
                        final ByteCollectionCell cell = colVec.values(i);

                        final List<Byte[]> l = new ArrayList<>(cell.valueLength());
                        for (int k = 0; k < cell.valueLength(); k++) {
                            final Byte[] bb = new Byte[cell.value(k).valueLength()];
                            for (int b = 0; b < cell.value(k).valueLength(); b++) {
                                bb[b] = (byte)cell.value(k).value(b);
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

        if (columns.isEmpty()) {
            return;
        }

        final int numRows = columns.get(columns.keySet().iterator().next()).size();
        for (int rowCount = 0; rowCount < numRows; rowCount++) {

            final Row r = new RowImpl(rowIds.get(rowCount), columns.keySet().size());
            int colCount = 0;
            for (final String colName : colNames) {
                if (missing.get(colName)[rowCount]) {
                    r.setCell(new CellImpl(), colCount);
                } else {

                    switch (colTypes.get(colName)) {
                        case BOOLEAN: {
                            r.setCell(new CellImpl((Boolean)columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case BOOLEAN_LIST: {
                            r.setCell(new CellImpl((Boolean[])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case BOOLEAN_SET: {
                            r.setCell(new CellImpl((Boolean[])columns.get(colName).get(rowCount), true), colCount);
                            break;
                        }
                        case INTEGER: {
                            r.setCell(new CellImpl((Integer)columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case INTEGER_LIST: {
                            r.setCell(new CellImpl((Integer[])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case INTEGER_SET: {
                            r.setCell(new CellImpl((Integer[])columns.get(colName).get(rowCount), true), colCount);
                            break;
                        }
                        case LONG: {
                            r.setCell(new CellImpl((Long)columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case LONG_LIST: {
                            r.setCell(new CellImpl((Long[])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case LONG_SET: {
                            r.setCell(new CellImpl((Long[])columns.get(colName).get(rowCount), true), colCount);
                            break;
                        }
                        case DOUBLE: {
                            r.setCell(new CellImpl((Double)columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case DOUBLE_LIST: {
                            r.setCell(new CellImpl((Double[])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case DOUBLE_SET: {
                            r.setCell(new CellImpl((Double[])columns.get(colName).get(rowCount), true), colCount);
                            break;
                        }
                        case STRING: {
                            r.setCell(new CellImpl((String)columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case STRING_LIST: {
                            r.setCell(new CellImpl((String[])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case STRING_SET: {
                            r.setCell(new CellImpl((String[])columns.get(colName).get(rowCount), true), colCount);
                            break;
                        }
                        case BYTES: {
                            r.setCell(new CellImpl((Byte[])columns.get(colName).get(rowCount)), colCount);
                            break;
                        }
                        case BYTES_LIST: {
                            r.setCell(new CellImpl((Byte[][])columns.get(colName).get(rowCount), false), colCount);
                            break;
                        }
                        case BYTES_SET: {
                            r.setCell(new CellImpl((Byte[][])columns.get(colName).get(rowCount), true), colCount);
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
    public TableSpec tableSpecFromBytes(final byte[] bytes) {
        final KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));

        final List<String> colNames = new ArrayList<>();
        final Type[] types = new Type[table.colNamesLength()];

        final Map<String, String> serializers = new HashMap<>();

        for (int h = 0; h < table.colNamesLength(); h++) {
            final String colName = table.colNames(h);
            colNames.add(colName);
        }

        for (int j = 0; j < table.columnsLength(); j++) {
            final Column col = table.columns(j);
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
