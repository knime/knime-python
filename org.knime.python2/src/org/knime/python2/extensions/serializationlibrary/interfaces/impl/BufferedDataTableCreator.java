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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.SetCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.vector.bytevector.DenseByteVector;
import org.knime.core.data.vector.bytevector.DenseByteVectorCell;
import org.knime.core.data.vector.bytevector.DenseByteVectorCellFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.PythonToKnimeExtensions;

public class BufferedDataTableCreator implements TableCreator<BufferedDataTable> {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableCreator.class);

	private final BufferedDataContainer m_container;
	private final TableSpec m_spec;
	private final PythonToKnimeExtensions m_pythonToKnimeExtensions;
	private final FileStoreFactory m_fileStoreFactory;
	private final ExecutionMonitor m_executionMonitor;
	private final int m_tableSize;
	private int m_rowsDone = 0;

	private HashMap<Integer, DataTypeContainer> m_columnsToRetype;
	private DataTableSpec m_dataTableSpec;
	private ExecutionContext m_exec;

	public BufferedDataTableCreator(final TableSpec spec, final ExecutionContext context,
			final ExecutionMonitor executionMonitor, int tableSize) {
		m_tableSize = tableSize;
		m_executionMonitor = executionMonitor;
		m_fileStoreFactory = FileStoreFactory.createWorkflowFileStoreFactory(context);
		m_spec = spec;
		m_exec = context;
		m_columnsToRetype = new HashMap<Integer, DataTypeContainer>();
		m_pythonToKnimeExtensions = new PythonToKnimeExtensions();
		DataColumnSpec[] colSpecs = new DataColumnSpec[m_spec.getNumberColumns()];
		String key;
		for (int i = 0; i < colSpecs.length; i++) {
			String columnName = spec.getColumnNames()[i];
			switch (spec.getColumnTypes()[i]) {
			case BOOLEAN:
				colSpecs[i] = new DataColumnSpecCreator(columnName, BooleanCell.TYPE).createSpec();
				break;
			case BOOLEAN_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(BooleanCell.TYPE))
						.createSpec();
				break;
			case BOOLEAN_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(BooleanCell.TYPE))
						.createSpec();
				break;
			case INTEGER:
				colSpecs[i] = new DataColumnSpecCreator(columnName, IntCell.TYPE).createSpec();
				break;
			case INTEGER_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(IntCell.TYPE))
						.createSpec();
				break;
			case INTEGER_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(IntCell.TYPE))
						.createSpec();
				break;
			case LONG:
				colSpecs[i] = new DataColumnSpecCreator(columnName, LongCell.TYPE).createSpec();
				break;
			case LONG_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(LongCell.TYPE))
						.createSpec();
				break;
			case LONG_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(LongCell.TYPE))
						.createSpec();
				break;
			case DOUBLE:
				colSpecs[i] = new DataColumnSpecCreator(columnName, DoubleCell.TYPE).createSpec();
				break;
			case DOUBLE_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(DoubleCell.TYPE))
						.createSpec();
				break;
			case DOUBLE_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(DoubleCell.TYPE))
						.createSpec();
				break;
			case STRING:
				colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
				break;
			case STRING_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE))
						.createSpec();
				break;
			case STRING_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE))
						.createSpec();
				break;
			case BYTES:
				key = spec.getColumnSerializers().get(columnName);
				if(key != null)
				{
					DataType type = PythonToKnimeExtensions.getExtension(key)
							.getJavaDeserializerFactory().getDataType();
					if (type.getCellClass() == null) {
						m_columnsToRetype.put(i, new DataTypeContainer(ResultType.PRIMITIVE));
					}
					colSpecs[i] = new DataColumnSpecCreator(columnName, type).createSpec();
				}
				else
					colSpecs[i] = new DataColumnSpecCreator(columnName, DenseByteVectorCell.TYPE).createSpec(); 
				break;
			case BYTES_LIST:
				key = spec.getColumnSerializers().get(columnName);
				if(key != null)
				{
					DataType list_type = PythonToKnimeExtensions.getExtension(key)
							.getJavaDeserializerFactory().getDataType();
					if (list_type.getCellClass() == null) {
						m_columnsToRetype.put(i, new DataTypeContainer(ResultType.LIST));
					}
					colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(list_type)).createSpec();
				} else {
					colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE)).createSpec(); 
				}
				break;
			case BYTES_SET:
				key = spec.getColumnSerializers().get(columnName);
				if(key != null)
				{
					DataType set_type = PythonToKnimeExtensions.getExtension(key)
							.getJavaDeserializerFactory().getDataType();
					if (set_type.getCellClass() == null) {
						m_columnsToRetype.put(i, new DataTypeContainer(ResultType.SET));
					}
					colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(set_type)).createSpec();
				} else {
					colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE)).createSpec(); 
				}
				break;
			default:
				colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
				break;
			}
		}
		m_dataTableSpec = new DataTableSpec(colSpecs);
		m_container = context.createDataContainer(m_dataTableSpec);
	}

	@Override
	public void addRow(final Row row) {
		try {
			m_executionMonitor.checkCanceled();
		} catch (CanceledExecutionException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		DataCell[] cells = new DataCell[row.getNumberCells()];
		int i = 0;
		for (Cell cell : row) {
			if (cell.isMissing()) {
				cells[i] = new MissingCell(null);
			} else {
				switch (cell.getColumnType()) {
				case BOOLEAN:
					cells[i] = BooleanCellFactory.create(cell.getBooleanValue());
					break;
				case BOOLEAN_LIST:
					List<DataCell> booleanListCells = new ArrayList<DataCell>();
					for (Boolean value : cell.getBooleanArrayValue()) {
						if (value == null) {
							booleanListCells.add(new MissingCell(null));
						} else {
							booleanListCells.add(BooleanCellFactory.create(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(booleanListCells);
					break;
				case BOOLEAN_SET:
					List<DataCell> booleanSetCells = new ArrayList<DataCell>();
					for (Boolean value : cell.getBooleanArrayValue()) {
						if (value == null) {
							booleanSetCells.add(new MissingCell(null));
						} else {
							booleanSetCells.add(BooleanCellFactory.create(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(booleanSetCells);
					break;
				case INTEGER:
					cells[i] = new IntCell(cell.getIntegerValue());
					break;
				case INTEGER_LIST:
					List<DataCell> integerListCells = new ArrayList<DataCell>();
					for (Integer value : cell.getIntegerArrayValue()) {
						if (value == null) {
							integerListCells.add(new MissingCell(null));
						} else {
							integerListCells.add(new IntCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(integerListCells);
					break;
				case INTEGER_SET:
					List<DataCell> integerSetCells = new ArrayList<DataCell>();
					for (Integer value : cell.getIntegerArrayValue()) {
						if (value == null) {
							integerSetCells.add(new MissingCell(null));
						} else {
							integerSetCells.add(new IntCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(integerSetCells);
					break;
				case LONG:
					cells[i] = new LongCell(cell.getLongValue());
					break;
				case LONG_LIST:
					List<DataCell> longListCells = new ArrayList<DataCell>();
					for (Long value : cell.getLongArrayValue()) {
						if (value == null) {
							longListCells.add(new MissingCell(null));
						} else {
							longListCells.add(new LongCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(longListCells);
					break;
				case LONG_SET:
					List<DataCell> longSetCells = new ArrayList<DataCell>();
					for (Long value : cell.getLongArrayValue()) {
						if (value == null) {
							longSetCells.add(new MissingCell(null));
						} else {
							longSetCells.add(new LongCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(longSetCells);
					break;
				case DOUBLE:
					cells[i] = new DoubleCell(cell.getDoubleValue());
					break;
				case DOUBLE_LIST:
					List<DataCell> doubleListCells = new ArrayList<DataCell>();
					for (Double value : cell.getDoubleArrayValue()) {
						if (value == null) {
							doubleListCells.add(new MissingCell(null));
						} else {
							doubleListCells.add(new DoubleCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(doubleListCells);
					break;
				case DOUBLE_SET:
					List<DataCell> doubleSetCells = new ArrayList<DataCell>();
					for (Double value : cell.getDoubleArrayValue()) {
						if (value == null) {
							doubleSetCells.add(new MissingCell(null));
						} else {
							doubleSetCells.add(new DoubleCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(doubleSetCells);
					break;
				case STRING:
					cells[i] = new StringCell(cell.getStringValue());
					break;
				case STRING_LIST:
					List<DataCell> stringListCells = new ArrayList<DataCell>();
					for (String value : cell.getStringArrayValue()) {
						if (value == null) {
							stringListCells.add(new MissingCell(null));
						} else {
							stringListCells.add(new StringCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(stringListCells);
					break;
				case STRING_SET:
					List<DataCell> stringSetCells = new ArrayList<DataCell>();
					for (String value : cell.getStringArrayValue()) {
						if (value == null) {
							stringSetCells.add(new MissingCell(null));
						} else {
							stringSetCells.add(new StringCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(stringSetCells);
					break;
				case BYTES:
					String bytesTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
					if(bytesTypeId != null) {
						Deserializer bytesDeserializer = m_pythonToKnimeExtensions
								.getDeserializer(PythonToKnimeExtensions.getExtension(bytesTypeId).getId());
						try {
							if (cell.getBytesValue() == null) {
								cells[i] = new MissingCell(null);
							} else {
								cells[i] = bytesDeserializer.deserialize(ArrayUtils.toPrimitive(cell.getBytesValue()),
										m_fileStoreFactory);
							}
							DataTypeContainer dataTypeContainer = (DataTypeContainer) m_columnsToRetype.get(i);
							if (dataTypeContainer != null) {
								dataTypeContainer.m_dataTypes.add(cells[i].getType());
							}
						} catch (IllegalStateException | IOException e) {
							LOGGER.error(e.getMessage(), e);
							cells[i] = new MissingCell(null);
						}
					} else {
						try {
							if (cell.getBytesValue() == null) {
								cells[i] = new MissingCell(null);
							} else {
								cells[i] = new DenseByteVectorCellFactory(new DenseByteVector(
										ArrayUtils.toPrimitive(cell.getBytesValue()))).createDataCell();
							}
						} catch (IllegalStateException e) {
							LOGGER.error(e.getMessage(), e);
							cells[i] = new MissingCell(null);
						}
					}
					break;
				case BYTES_LIST:
					String bytesListTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
					if(bytesListTypeId != null)
					{
						Deserializer bytesListDeserializer = m_pythonToKnimeExtensions
								.getDeserializer(PythonToKnimeExtensions.getExtension(bytesListTypeId).getId());
						List<DataCell> listCells = new ArrayList<DataCell>();
						if (cell.getBytesArrayValue() == null) {
							cells[i] = new MissingCell(null);
						} else {
							for (Byte[] value : cell.getBytesArrayValue()) {
								if (value == null) {
									listCells.add(new MissingCell(null));
								} else {
									try {
										DataCell dc = bytesListDeserializer.deserialize(ArrayUtils.toPrimitive(value),
													m_fileStoreFactory);
										DataTypeContainer dataTypeContainer = (DataTypeContainer) m_columnsToRetype.get(i);
										if (dataTypeContainer != null) {
											dataTypeContainer.m_dataTypes.add(dc.getType());
										}
										listCells.add(dc);
									} catch (IllegalStateException | IOException e) {
										LOGGER.error(e.getMessage(), e);
										listCells.add(new MissingCell(null));
									}
								}
							}
							cells[i] = CollectionCellFactory.createListCell(listCells);
						}
					} else {
						if (cell.getBytesArrayValue() == null) {
							cells[i] = new MissingCell(null);
						} else {
							List<DataCell> listCells = new ArrayList<DataCell>();
							for (Byte[] value : cell.getBytesArrayValue()) {
								if (value == null) {
									listCells.add(new MissingCell(null));
								} else {
									try {
										listCells.add(new StringCell(value.toString()));
									} catch (IllegalStateException e) {
										LOGGER.error(e.getMessage(), e);
										listCells.add(new MissingCell(null));
									}
								}
							}
							cells[i] = CollectionCellFactory.createListCell(listCells);
						}
					}
					break;
				case BYTES_SET:
					String bytesSetTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
					if(bytesSetTypeId != null)
					{
						Deserializer bytesSetDeserializer = m_pythonToKnimeExtensions
								.getDeserializer(PythonToKnimeExtensions.getExtension(bytesSetTypeId).getId());
						List<DataCell> setCells = new ArrayList<DataCell>();
						if (cell.getBytesArrayValue() == null) {
							cells[i] = new MissingCell(null);
						} else {
							for (Byte[] value : cell.getBytesArrayValue()) {
								if (value == null) {
									setCells.add(new MissingCell(null));
								} else {
									try {
										DataCell dc = bytesSetDeserializer.deserialize(ArrayUtils.toPrimitive(value),
												m_fileStoreFactory);
										DataTypeContainer dataTypeContainer = (DataTypeContainer) m_columnsToRetype.get(i);
										if (dataTypeContainer != null) {
											dataTypeContainer.m_dataTypes.add(dc.getType());
										}
										setCells.add(dc);
									} catch (IllegalStateException | IOException e) {
										LOGGER.error(e.getMessage(), e);
										setCells.add(new MissingCell(null));
									}
								}
							}
							cells[i] = CollectionCellFactory.createSetCell(setCells);
						}
					} else {
						List<DataCell> setCells = new ArrayList<DataCell>();
						if (cell.getBytesArrayValue() == null) {
							cells[i] = new MissingCell(null);
						} else {
							for (Byte[] value : cell.getBytesArrayValue()) {
								if (value == null) {
									setCells.add(new MissingCell(null));
								} else {
									try {
										setCells.add(new StringCell(value.toString()));
									} catch (IllegalStateException e) {
										LOGGER.error(e.getMessage(), e);
										setCells.add(new MissingCell(null));
									}
								}
							}
							cells[i] = CollectionCellFactory.createSetCell(setCells);
						}
					}
					break;
				default:
					cells[i] = new MissingCell(null);
				}
			}
			i++;
		}
		m_container.addRowToTable(new DefaultRow(row.getRowKey(), cells));
		m_rowsDone++;
		m_executionMonitor.setProgress(m_rowsDone / (double) m_tableSize);
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}

	private DataType getMostCommonAncestor(HashSet<DataType> types) {
		Iterator<DataType> iter = types.iterator();
		DataType mca = iter.next();
		while (iter.hasNext()) {
			mca = DataType.getCommonSuperType(mca, iter.next());
		}
		return mca;
	}

	public BufferedDataTable getTable() {
		m_container.close();
		DataColumnSpec[] colSpecs = new DataColumnSpec[m_dataTableSpec.getNumColumns()];
		for (int i = 0; i < colSpecs.length; i++) {
			DataColumnSpec dcs = m_dataTableSpec.getColumnSpec(i);
			DataColumnSpecCreator dcsc = new DataColumnSpecCreator(dcs);
			if (m_columnsToRetype.containsKey(i)) {
				DataTypeContainer dtContainer = m_columnsToRetype.get(i);
				DataType elementType = getMostCommonAncestor(dtContainer.m_dataTypes);
				if (dtContainer.m_resultType == ResultType.PRIMITIVE) {
					dcsc.setType(elementType);
				} else if (dtContainer.m_resultType == ResultType.LIST) {
					dcsc.setType(ListCell.getCollectionType(elementType));
				} else if (dtContainer.m_resultType == ResultType.SET) {
					dcsc.setType(SetCell.getCollectionType(elementType));
				}
			}
			colSpecs[i] = dcsc.createSpec();
		}
		DataTableSpec correctedSpec = new DataTableSpec(m_dataTableSpec.getName(), colSpecs);
		return m_exec.createSpecReplacerTable(m_container.getTable(), correctedSpec);
	}

	/**
	 * Enum for distinguishing if a cell contains primitives or collections
	 * (either lists or sets).
	 */
	private enum ResultType {
		PRIMITIVE, LIST, SET;
	}

	/**
	 * Container class used for storing all data types present in a certain
	 * column. Also indicates if the objects in the column have a primitive or a
	 * collection type.
	 */
	private class DataTypeContainer {
		ResultType m_resultType;
		HashSet<DataType> m_dataTypes;

		public DataTypeContainer(ResultType type) {
			m_resultType = type;
			m_dataTypes = new HashSet<DataType>();
		}
	}

}
