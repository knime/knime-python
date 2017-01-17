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
import org.knime.core.node.ExecutionContext;
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
	
	public BufferedDataTableCreator(final TableSpec spec, final ExecutionContext context) {
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
		DataCell[] cells = new DataCell[row.getNumberCells()];
		int i = 0;
		for (Cell cell : row) {
			switch (cell.getColumnType()) {
			case BOOLEAN:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					cells[i] = BooleanCellFactory.create(cell.getBooleanValue());
				}
				break;
			case BOOLEAN_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Boolean value : cell.getBooleanArrayValue()) {
						if (value == null) {
							listCells.add(new MissingCell(null));
						} else {
							listCells.add(BooleanCellFactory.create(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case BOOLEAN_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Boolean value : cell.getBooleanArrayValue()) {
						if (value == null) {
							setCells.add(new MissingCell(null));
						} else {
							setCells.add(BooleanCellFactory.create(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(setCells);
				}
				break;
			case INTEGER:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					cells[i] = new IntCell(cell.getIntegerValue());
				}
				break;
			case INTEGER_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Integer value : cell.getIntegerArrayValue()) {
						if (value == null) {
							listCells.add(new MissingCell(null));
						} else {
							listCells.add(new IntCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case INTEGER_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Integer value : cell.getIntegerArrayValue()) {
						if (value == null) {
							setCells.add(new MissingCell(null));
						} else {
							setCells.add(new IntCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(setCells);
				}
				break;
			case LONG:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					cells[i] = new LongCell(cell.getLongValue());
				}
				break;
			case LONG_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Long value : cell.getLongArrayValue()) {
						if (value == null) {
							listCells.add(new MissingCell(null));
						} else {
							listCells.add(new LongCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case LONG_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Long value : cell.getLongArrayValue()) {
						if (value == null) {
							setCells.add(new MissingCell(null));
						} else {
							setCells.add(new LongCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(setCells);
				}
				break;
			case DOUBLE:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					cells[i] = new DoubleCell(cell.getDoubleValue());
				}
				break;
			case DOUBLE_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Double value : cell.getDoubleArrayValue()) {
						if (value == null) {
							listCells.add(new MissingCell(null));
						} else {
							listCells.add(new DoubleCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case DOUBLE_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Double value : cell.getDoubleArrayValue()) {
						if (value == null) {
							setCells.add(new MissingCell(null));
						} else {
							setCells.add(new DoubleCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(setCells);
				}
				break;
			case STRING:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					cells[i] = new StringCell(cell.getStringValue());
				}
				break;
			case STRING_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (String value : cell.getStringArrayValue()) {
						if (value == null) {
							listCells.add(new MissingCell(null));
						} else {
							listCells.add(new StringCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case STRING_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (String value : cell.getStringArrayValue()) {
						if (value == null) {
							setCells.add(new MissingCell(null));
						} else {
							setCells.add(new StringCell(value));
						}
					}
					cells[i] = CollectionCellFactory.createSetCell(setCells);
				}
				break;
			case BYTES:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					String typeId = null;
					Deserializer deserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(typeId).getId());
					try {
						cells[i] = deserializer.deserialize(ArrayUtils.toPrimitive(cell.getBytesValue()), m_fileStoreFactory);
					} catch (IllegalStateException | IOException e) {
						LOGGER.error(e.getMessage(), e);
						cells[i] = new MissingCell(null);
					}
				}
				break;
			case BYTES_LIST:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					String typeId = null;
					Deserializer deserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(typeId).getId());
					List<DataCell> listCells = new ArrayList<DataCell>();
					for (Byte[] value : cell.getBytesArrayValue()) {
						try {
							listCells.add(deserializer.deserialize(ArrayUtils.toPrimitive(value), m_fileStoreFactory));
						} catch (IllegalStateException | IOException e) {
							LOGGER.error(e.getMessage(), e);
							listCells.add(new MissingCell(null));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(listCells);
				}
				break;
			case BYTES_SET:
				if (cell.isMissing()) {
					cells[i] = new MissingCell(null);
				} else {
					String typeId = null;
					Deserializer deserializer = m_pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(typeId).getId());
					List<DataCell> setCells = new ArrayList<DataCell>();
					for (Byte[] value : cell.getBytesArrayValue()) {
						try {
							setCells.add(deserializer.deserialize(ArrayUtils.toPrimitive(value), m_fileStoreFactory));
						} catch (IllegalStateException | IOException e) {
							LOGGER.error(e.getMessage(), e);
							setCells.add(new MissingCell(null));
						}
					}
					cells[i] = CollectionCellFactory.createListCell(setCells);
				}
				break;
			default:
				cells[i] = new MissingCell(null);
			}
			i++;
		}
		m_container.addRowToTable(new DefaultRow(row.getRowKey(), cells));
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
