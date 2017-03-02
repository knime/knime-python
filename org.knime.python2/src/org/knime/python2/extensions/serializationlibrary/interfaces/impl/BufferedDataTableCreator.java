package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
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

public class BufferedDataTableCreator implements TableCreator {
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableCreator.class);
	
	private final BufferedDataContainer m_container;
	private final TableSpec m_spec;
	private final PythonToKnimeExtensions m_pythonToKnimeExtensions;
	private final FileStoreFactory m_fileStoreFactory;
	private final ExecutionMonitor m_executionMonitor;
	private final int m_tableSize;
	private int m_rowsDone = 0;
	
	public BufferedDataTableCreator(final TableSpec spec, final ExecutionContext context, final ExecutionMonitor executionMonitor, int tableSize) {
		m_tableSize = tableSize;
		m_executionMonitor = executionMonitor;
		m_fileStoreFactory = FileStoreFactory.createWorkflowFileStoreFactory(context);
		m_spec = spec;
		m_pythonToKnimeExtensions = new PythonToKnimeExtensions();
		DataColumnSpec[] colSpecs = new DataColumnSpec[m_spec.getNumberColumns()];
		for (int i = 0; i < colSpecs.length; i++) {
			String columnName = spec.getColumnNames()[i];
			switch (spec.getColumnTypes()[i]) {
			case BOOLEAN:
				colSpecs[i] = new DataColumnSpecCreator(columnName, BooleanCell.TYPE).createSpec();
				break;
			case BOOLEAN_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(BooleanCell.TYPE)).createSpec();
				break;
			case BOOLEAN_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(BooleanCell.TYPE)).createSpec();
				break;
			case INTEGER:
				colSpecs[i] = new DataColumnSpecCreator(columnName, IntCell.TYPE).createSpec();
				break;
			case INTEGER_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(IntCell.TYPE)).createSpec();
				break;
			case INTEGER_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(IntCell.TYPE)).createSpec();
				break;
			case LONG:
				colSpecs[i] = new DataColumnSpecCreator(columnName, LongCell.TYPE).createSpec();
				break;
			case LONG_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(LongCell.TYPE)).createSpec();
				break;
			case LONG_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(LongCell.TYPE)).createSpec();
				break;
			case DOUBLE:
				colSpecs[i] = new DataColumnSpecCreator(columnName, DoubleCell.TYPE).createSpec();
				break;
			case DOUBLE_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(DoubleCell.TYPE)).createSpec();
				break;
			case DOUBLE_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(DoubleCell.TYPE)).createSpec();
				break;
			case STRING:
				colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
				break;
			case STRING_LIST:
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE)).createSpec();
				break;
			case STRING_SET:
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE)).createSpec();
				break;
			case BYTES:
				// TODO we need to figure out the type based on an ID
				colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
				break;
			case BYTES_LIST:
				// TODO we need to figure out the type based on an ID
				colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE)).createSpec();
				break;
			case BYTES_SET:
				// TODO we need to figure out the type based on an ID
				colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE)).createSpec();
				break;
			default:
				colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
				break;
			}
		}
		m_container = context.createDataContainer(new DataTableSpec(colSpecs));
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
					Deserializer bytesDeserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(bytesTypeId).getId());
					try {
						cells[i] = bytesDeserializer.deserialize(ArrayUtils.toPrimitive(cell.getBytesValue()), m_fileStoreFactory);
					} catch (IllegalStateException | IOException e) {
						LOGGER.error(e.getMessage(), e);
						cells[i] = new MissingCell(null);
					}
					break;
				case BYTES_LIST:
					String bytesListTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
					Deserializer bytesListDeserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(bytesListTypeId).getId());
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Byte[] value : cell.getBytesArrayValue()) {
						try {
							listCells.add(bytesListDeserializer.deserialize(ArrayUtils.toPrimitive(value), m_fileStoreFactory));
						} catch (IllegalStateException | IOException e) {
							LOGGER.error(e.getMessage(), e);
							listCells.add(new MissingCell(null));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
					break;
				case BYTES_SET:
					String bytesSetTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
					Deserializer bytesSetDeserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(bytesSetTypeId).getId());
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Byte[] value : cell.getBytesArrayValue()) {
						try {
							setCells.add(bytesSetDeserializer.deserialize(ArrayUtils.toPrimitive(value), m_fileStoreFactory));
						} catch (IllegalStateException | IOException e) {
							LOGGER.error(e.getMessage(), e);
							setCells.add(new MissingCell(null));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(setCells);
					break;
				default:
					cells[i] = new MissingCell(null);
				}
			}
			i++;
		}
		m_container.addRowToTable(new DefaultRow(row.getRowKey(), cells));
		m_rowsDone++;
		m_executionMonitor.setProgress(m_rowsDone/(double)m_tableSize);
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}
	
	public BufferedDataTable getTable() {
		m_container.close();
		return m_container.getTable();
	}

}
