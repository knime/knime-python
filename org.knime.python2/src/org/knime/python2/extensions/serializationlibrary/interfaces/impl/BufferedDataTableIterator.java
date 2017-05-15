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
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.typeextension.KnimeToPythonExtension;
import org.knime.python2.typeextension.KnimeToPythonExtensions;
import org.knime.python2.typeextension.Serializer;

public class BufferedDataTableIterator implements TableIterator {
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableIterator.class);
	
	private final int m_numberRows;
	private int m_remainingRows;
	private CloseableRowIterator m_iterator;
	private TableSpec m_spec;
	private KnimeToPythonExtensions m_knimeToPythonExtensions;
	private final ExecutionMonitor m_executionMonitor;
	
	public BufferedDataTableIterator(final DataTableSpec spec, final CloseableRowIterator rowIterator, final int numberRows, final ExecutionMonitor monitor) {
		m_numberRows = numberRows;
		m_spec = dataTableSpecToTableSpec(spec);
		m_remainingRows = numberRows;
		m_iterator = rowIterator;
		m_knimeToPythonExtensions = new KnimeToPythonExtensions();
		m_executionMonitor = monitor;
	}

	@Override
	public Row next() {
		if (m_remainingRows > 0) {
			if (m_executionMonitor != null) {
				try {
					m_executionMonitor.checkCanceled();
					m_executionMonitor.setProgress((m_numberRows-m_remainingRows)/(double)m_numberRows);
				} catch (CanceledExecutionException e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			}
			m_remainingRows--;
			return dataRowToRow(m_iterator.next());
		} else {
			return null;
		}
	}

	@Override
	public boolean hasNext() {
		return m_remainingRows > 0;
	}

	@Override
	public int getNumberRemainingRows() {
		return m_remainingRows;
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Row dataRowToRow(final DataRow dataRow) {
		Row row = new RowImpl(dataRow.getKey().getString(), dataRow.getNumCells());
		for (int i = 0; i < dataRow.getNumCells(); i++) {
			DataCell dataCell = dataRow.getCell(i);
			Type type = m_spec.getColumnTypes()[i];
			if (dataCell.isMissing()) {
				row.setCell(new CellImpl(), i);
			} else if (type == Type.BOOLEAN) {
				Boolean value = ((BooleanValue)dataCell).getBooleanValue();
				row.setCell(new CellImpl(value), i);
			} else if (type == Type.BOOLEAN_LIST || type == Type.BOOLEAN_SET) {
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				Boolean[] values = new Boolean[colCell.size()];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						values[j++] = ((BooleanValue)innerCell).getBooleanValue();
					}
				}
				row.setCell(new CellImpl(values, type == Type.BOOLEAN_SET), i);
			} else if (type == Type.INTEGER) {
				Integer value = ((IntValue)dataCell).getIntValue();
				row.setCell(new CellImpl(value), i);
			} else if (type == Type.INTEGER_LIST || type == Type.INTEGER_SET) {
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				Integer[] values = new Integer[colCell.size()];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						values[j++] = ((IntValue)innerCell).getIntValue();
					}
				}
				row.setCell(new CellImpl(values, type == Type.INTEGER_SET), i);
			} else if (type == Type.LONG) {
				Long value = ((LongValue)dataCell).getLongValue();
				row.setCell(new CellImpl(value), i);
			} else if (type == Type.LONG_LIST || type == Type.LONG_SET) {
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				Long[] values = new Long[colCell.size()];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						values[j++] = ((LongValue)innerCell).getLongValue();
					}
				}
				row.setCell(new CellImpl(values, type == Type.LONG_SET), i);
			} else if (type == Type.DOUBLE) {
				Double value = ((DoubleValue)dataCell).getDoubleValue();
				row.setCell(new CellImpl(value), i);
			} else if (type == Type.DOUBLE_LIST || type == Type.DOUBLE_SET) {
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				Double[] values = new Double[colCell.size()];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						values[j++] = ((DoubleValue)innerCell).getDoubleValue();
					}
				}
				row.setCell(new CellImpl(values, type == Type.DOUBLE_SET), i);
			} else if (type == Type.STRING) {
				String value;
				if (dataCell.getType().isCompatible(StringValue.class)) {
					value = ((StringValue)dataCell).getStringValue();
				} else {
					value = dataCell.toString();
				}
				row.setCell(new CellImpl(value), i);
			} else if (type == Type.STRING_LIST || type == Type.STRING_SET) {
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				String[] values = new String[colCell.size()];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						if (innerCell.getType().isCompatible(StringValue.class)) {
							values[j++] = ((StringValue)innerCell).getStringValue();
						} else {
							values[j++] = innerCell.toString();
						}
					}
				}
				row.setCell(new CellImpl(values, type == Type.STRING_SET), i);
			} else if (type == Type.BYTES) {
				Serializer serializer = m_knimeToPythonExtensions.getSerializer(KnimeToPythonExtensions.getExtension(dataCell.getType()).getId());
				try {
					Byte[] value = ArrayUtils.toObject(serializer.serialize(dataCell));
					row.setCell(new CellImpl(value), i);
				} catch (IOException e) {
					LOGGER.error(e.getMessage(), e);
					row.setCell(new CellImpl(), i);
				}
			} else if (type == Type.BYTES_LIST || type == Type.BYTES_SET) {
				Serializer serializer = m_knimeToPythonExtensions.getSerializer(KnimeToPythonExtensions.getExtension(dataCell.getType().getCollectionElementType()).getId());
				CollectionDataValue colCell = (CollectionDataValue)dataCell;
				Byte[][] values = new Byte[colCell.size()][];
				int j = 0;
				for (DataCell innerCell : colCell) {
					if (innerCell.isMissing()) {
						values[j++] = null;
					} else {
						try {
							values[j++] = ArrayUtils.toObject(serializer.serialize(innerCell));
						} catch (IOException e) {
							LOGGER.error(e.getMessage(), e);
							values[j++] = null;
						}
					}
				}
				row.setCell(new CellImpl(values, type == Type.BYTES_SET), i);
			}
		}
		return row;
	}
	
	private TableSpec dataTableSpecToTableSpec(final DataTableSpec dataTableSpec) {
		Type[] types = new Type[dataTableSpec.getNumColumns()];
		String[] names = new String[dataTableSpec.getNumColumns()];
		Map<String, String> columnSerializers = new HashMap<String, String>();
		int i = 0;
		for (DataColumnSpec colSpec : dataTableSpec) {
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
						final KnimeToPythonExtension typeExtension = KnimeToPythonExtensions.getExtension(colSpec.getType()
								.getCollectionElementType());
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
						final KnimeToPythonExtension typeExtension = KnimeToPythonExtensions.getExtension(colSpec.getType()
								.getCollectionElementType());
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
