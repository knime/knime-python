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

package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.io.IOException;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.collection.SetDataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.Serializer;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 * Iterates over a chunk of a {@link BufferedDataTable}.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class BufferedDataTableIterator implements TableIterator {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableIterator.class);

    private final int m_numberRows;

    private int m_remainingRows;

    private final CloseableRowIterator m_iterator;

    private final TableSpec m_spec;

    private final KnimeToPythonExtensions m_knimeToPythonExtensions;

    private final ExecutionMonitor m_executionMonitor;

    private final BufferedDataTableChunker.IterationProperties m_iterIterationProperties;

    /**
     * Constructor.
     *
     * @param spec the spec of the table to chunk in the standard KNIME format
     * @param rowIterator an iterator for the table to chunk
     * @param numberRows the number of rows of the table to chunk
     * @param monitor an execution monitor for reporting progress
     * @param ip iteration properties shared with the associated chunker to ensure a consistent state
     */
    public BufferedDataTableIterator(final DataTableSpec spec, final CloseableRowIterator rowIterator,
        final int numberRows, final ExecutionMonitor monitor, final BufferedDataTableChunker.IterationProperties ip) {
        this(dataTableSpecToTableSpec(spec), rowIterator, numberRows, monitor, ip);
    }

    /**
     * Constructor.
     *
     * @param spec the spec of the table to chunk in the python table representation format
     * @param rowIterator an iterator for the table to chunk
     * @param numberRows the number of rows of the table to chunk
     * @param monitor an execution monitor for reporting progress
     * @param ip iteration properties shared with the associated chunker to ensure a consistent state
     */
    public BufferedDataTableIterator(final TableSpec spec, final CloseableRowIterator rowIterator, final int numberRows,
        final ExecutionMonitor monitor, final BufferedDataTableChunker.IterationProperties ip) {
        m_numberRows = numberRows;
        m_spec = spec;
        m_remainingRows = numberRows;
        m_iterator = rowIterator;
        m_knimeToPythonExtensions = new KnimeToPythonExtensions();
        m_executionMonitor = monitor;
        m_iterIterationProperties = ip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row next() {
        if (m_remainingRows > 0) {
            if (m_executionMonitor != null) {
                try {
                    m_executionMonitor.checkCanceled();
                    m_executionMonitor.setProgress((m_numberRows - m_remainingRows) / (double)m_numberRows);
                } catch (final CanceledExecutionException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
            m_remainingRows--;
            m_iterIterationProperties.m_remainingRows--;
            return dataRowToRow(m_iterator.next());
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return m_remainingRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberRemainingRows() {
        return m_remainingRows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableSpec getTableSpec() {
        return m_spec;
    }

    /**
     * Close this iterator
     */
    public void close() {
        m_remainingRows = 0;
    }

    /**
     * Convert a {@link DataRow} to a {@link Row}
     *
     * @param dataRow a {@link DataRow}
     * @return a {@link Row}
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Row dataRowToRow(final DataRow dataRow) {
        final Row row = new RowImpl(dataRow.getKey().getString(), dataRow.getNumCells());
        for (int i = 0; i < dataRow.getNumCells(); i++) {
            final DataCell dataCell = dataRow.getCell(i);
            final Type type = m_spec.getColumnTypes()[i];
            if (dataCell.isMissing()) {
                row.setCell(new CellImpl(), i);
            } else if (type == Type.BOOLEAN) {
                final boolean value = ((BooleanValue)dataCell).getBooleanValue();
                row.setCell(new CellImpl(value), i);
            } else if (type == Type.BOOLEAN_LIST) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final boolean[] values = new boolean[colCell.size()];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((BooleanValue)innerCell).getBooleanValue();
                        missings[j / 8] += (1 << (j % 8));
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.BOOLEAN_SET) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                boolean[] values = new boolean[colCell.size()];
                boolean hasMissing = false;
                int ctr = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[ctr] = ((BooleanValue)innerCell).getBooleanValue();
                        ctr++;
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(values, hasMissing), i);
                } else {
                    row.setCell(new CellImpl(ArrayUtils.subarray(values, 0, colCell.size() - 1), hasMissing), i);
                }

            } else if (type == Type.INTEGER) {
                final int value = ((IntValue)dataCell).getIntValue();
                row.setCell(new CellImpl(value), i);
            } else if (type == Type.INTEGER_LIST) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final int[] values = new int[colCell.size()];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((IntValue)innerCell).getIntValue();
                        missings[j / 8] += (1 << (j % 8));
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.INTEGER_SET) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                IntBuffer buff = IntBuffer.allocate(colCell.size());
                boolean hasMissing = false;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        buff.put(((IntValue)innerCell).getIntValue());
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(buff.array(), hasMissing), i);
                } else {
                    int[] values = new int[colCell.size() - 1];
                    buff.position(0);
                    buff.get(values);
                    row.setCell(new CellImpl(values, hasMissing), i);
                }

            } else if (type == Type.LONG) {
                final long value = ((LongValue)dataCell).getLongValue();
                row.setCell(new CellImpl(value), i);
            } else if (type == Type.LONG_LIST) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final long[] values = new long[colCell.size()];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((LongValue)innerCell).getLongValue();
                        missings[j / 8] += (1 << (j % 8));
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.LONG_SET) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                LongBuffer buff = LongBuffer.allocate(colCell.size());
                boolean hasMissing = false;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        buff.put(((LongValue)innerCell).getLongValue());
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(buff.array(), hasMissing), i);
                } else {
                    long[] values = new long[colCell.size() - 1];
                    buff.position(0);
                    buff.get(values);
                    row.setCell(new CellImpl(values, hasMissing), i);
                }

            } else if (type == Type.DOUBLE) {
                final double value = ((DoubleValue)dataCell).getDoubleValue();
                row.setCell(new CellImpl(value), i);
            } else if (type == Type.DOUBLE_LIST) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final double[] values = new double[colCell.size()];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((DoubleValue)innerCell).getDoubleValue();
                        missings[j / 8] += (1 << (j % 8));
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.DOUBLE_SET) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                DoubleBuffer buff = DoubleBuffer.allocate(colCell.size());
                boolean hasMissing = false;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        buff.put(((DoubleValue)innerCell).getDoubleValue());
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(buff.array(), hasMissing), i);
                } else {
                    double[] values = new double[colCell.size() - 1];
                    buff.position(0);
                    buff.get(values);
                    row.setCell(new CellImpl(values, hasMissing), i);
                }

            } else if (type == Type.STRING) {
                String value;
                if (dataCell.getType().isCompatible(StringValue.class)) {
                    value = ((StringValue)dataCell).getStringValue();
                } else {
                    value = dataCell.toString();
                }
                row.setCell(new CellImpl(value), i);
            } else if (type == Type.STRING_LIST) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final String[] values = new String[colCell.size()];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((StringValue)innerCell).getStringValue();
                        missings[j / 8] += (1 << (j % 8));
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.STRING_SET) {
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final String[] values = new String[colCell.size()];
                boolean hasMissing = false;
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        values[j] = ((StringValue)innerCell).getStringValue();
                        j++;
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(values, hasMissing), i);
                } else {
                    row.setCell(new CellImpl((String[]) ArrayUtils.subarray(values, 0, colCell.size() - 1), hasMissing), i);
                }

            } else if (type == Type.BYTES) {
                final Serializer serializer = m_knimeToPythonExtensions
                        .getSerializer(KnimeToPythonExtensions.getExtension(dataCell.getType()).getId());
                try {
                    final byte[] value = serializer.serialize(dataCell);
                    row.setCell(new CellImpl(value), i);
                } catch (final IOException e) {
                    LOGGER.error(e.getMessage(), e);
                    row.setCell(new CellImpl(), i);
                }
            } else if (type == Type.BYTES_LIST) {
                final Serializer serializer = m_knimeToPythonExtensions.getSerializer(
                    KnimeToPythonExtensions.getExtension(dataCell.getType().getCollectionElementType()).getId());
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final byte[][] values = new byte[colCell.size()][];
                final byte[] missings = new byte[colCell.size() / 8 + (colCell.size() % 8 == 0 ? 0:1)];
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        try {
                            values[j] = serializer.serialize(innerCell);
                            missings[j / 8] += (1 << (j % 8));
                        } catch (final IOException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                    j++;
                }
                row.setCell(new CellImpl(values, missings), i);

            } else if (type == Type.BYTES_SET) {
                final Serializer serializer = m_knimeToPythonExtensions.getSerializer(
                    KnimeToPythonExtensions.getExtension(dataCell.getType().getCollectionElementType()).getId());
                final CollectionDataValue colCell = (CollectionDataValue)dataCell;
                final byte[][] values = new byte[colCell.size()][];
                boolean hasMissing = false;
                int j = 0;
                for (final DataCell innerCell : colCell) {
                    if (!innerCell.isMissing()) {
                        try {
                            values[j] = serializer.serialize(innerCell);
                        } catch (final IOException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                        j++;
                    } else {
                        hasMissing = true;
                    }
                }
                if(!hasMissing) {
                    row.setCell(new CellImpl(values, hasMissing), i);
                } else {
                    row.setCell(new CellImpl((byte[][]) ArrayUtils.subarray(values, 0, colCell.size() - 1), hasMissing), i);
                }
            }
        }
        return row;
    }

    /**
     * Convert a {@link DataTableSpec} to a {@link TableSpec}
     *
     * @param dataRow a {@link DataTableSpec}
     * @return a {@link TableSpec}
     */

    static TableSpec dataTableSpecToTableSpec(final DataTableSpec dataTableSpec) {
        final Type[] types = new Type[dataTableSpec.getNumColumns()];
        final String[] names = new String[dataTableSpec.getNumColumns()];
        final Map<String, String> columnSerializers = new HashMap<String, String>();
        int i = 0;
        for (final DataColumnSpec colSpec : dataTableSpec) {
            names[i] = colSpec.getName();
            if (colSpec.getType().isCompatible(BooleanValue.class)) {
                types[i] = Type.BOOLEAN;
            } else if (colSpec.getType().isCompatible(IntValue.class)) {
                types[i] = Type.INTEGER;
            } else if (colSpec.getType().isCompatible(LongValue.class)) {
                types[i] = Type.LONG;
            } else if (colSpec.getType().isCompatible(DoubleValue.class)) {
                types[i] = Type.DOUBLE;
            } else if (colSpec.getType().isCollectionType()) {
                if (colSpec.getType().isCompatible(SetDataValue.class)) {
                    if (colSpec.getType().getCollectionElementType().isCompatible(BooleanValue.class)) {
                        types[i] = Type.BOOLEAN_SET;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(IntValue.class)) {
                        types[i] = Type.INTEGER_SET;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(LongValue.class)) {
                        types[i] = Type.LONG_SET;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(DoubleValue.class)) {
                        types[i] = Type.DOUBLE_SET;
                    } else {
                        final KnimeToPythonExtension typeExtension =
                                KnimeToPythonExtensions.getExtension(colSpec.getType().getCollectionElementType());
                        if (typeExtension != null) {
                            types[i] = Type.BYTES_SET;
                            columnSerializers.put(colSpec.getName(), typeExtension.getId());
                        } else {
                            types[i] = Type.STRING_SET;
                        }
                    }
                } else {
                    if (colSpec.getType().getCollectionElementType().isCompatible(BooleanValue.class)) {
                        types[i] = Type.BOOLEAN_LIST;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(IntValue.class)) {
                        types[i] = Type.INTEGER_LIST;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(LongValue.class)) {
                        types[i] = Type.LONG_LIST;
                    } else if (colSpec.getType().getCollectionElementType().isCompatible(DoubleValue.class)) {
                        types[i] = Type.DOUBLE_LIST;
                    } else {
                        final KnimeToPythonExtension typeExtension =
                                KnimeToPythonExtensions.getExtension(colSpec.getType().getCollectionElementType());
                        if (typeExtension != null) {
                            types[i] = Type.BYTES_LIST;
                            columnSerializers.put(colSpec.getName(), typeExtension.getId());
                        } else {
                            types[i] = Type.STRING_LIST;
                        }
                    }
                }
            } else {
                final KnimeToPythonExtension typeExtension = KnimeToPythonExtensions.getExtension(colSpec.getType());
                if (typeExtension != null) {
                    types[i] = Type.BYTES;
                    columnSerializers.put(colSpec.getName(), typeExtension.getId());
                } else {
                    types[i] = Type.STRING;
                }
            }
            i++;
        }
        return new TableSpecImpl(types, names, columnSerializers);
    }
}
