/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
import org.knime.python.typeextension.Deserializer;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

/**
 * Used for creating a {@link BufferedDataTable} out of a python integration specific table structure.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class BufferedDataTableCreator implements TableCreator<BufferedDataTable> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableCreator.class);

    private final BufferedDataContainer m_container;

    private final TableSpec m_spec;

    private final PythonToKnimeExtensions m_pythonToKnimeExtensions;

    private final FileStoreFactory m_fileStoreFactory;

    private final ExecutionMonitor m_executionMonitor;

    private final int m_tableSize;

    private int m_rowsDone = 0;

    private final HashMap<Integer, DataTypeContainer> m_columnsToRetype;

    private final DataTableSpec m_dataTableSpec;

    private final ExecutionContext m_exec;

    /**
     * Constructor.
     *
     * @param spec a table spec in the python integration specific format
     * @param context a node's execution context
     * @param executionMonitor an execution monitor to report progress to
     * @param tableSize the number of rows of the table to create
     */
    public BufferedDataTableCreator(final TableSpec spec, final ExecutionContext context,
        final ExecutionMonitor executionMonitor, final int tableSize) {
        m_tableSize = tableSize;
        m_executionMonitor = executionMonitor;
        m_fileStoreFactory = FileStoreFactory.createWorkflowFileStoreFactory(context);
        m_spec = spec;
        m_exec = context;
        m_columnsToRetype = new HashMap<Integer, DataTypeContainer>();
        m_pythonToKnimeExtensions = new PythonToKnimeExtensions();
        final DataColumnSpec[] colSpecs = new DataColumnSpec[m_spec.getNumberColumns()];
        String key;
        for (int i = 0; i < colSpecs.length; i++) {
            final String columnName = spec.getColumnNames()[i];
            switch (spec.getColumnTypes()[i]) {
                case BOOLEAN:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, BooleanCell.TYPE).createSpec();
                    break;
                case BOOLEAN_LIST:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(BooleanCell.TYPE))
                        .createSpec();
                    break;
                case BOOLEAN_SET:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, SetCell.getCollectionType(BooleanCell.TYPE)).createSpec();
                    break;
                case INTEGER:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, IntCell.TYPE).createSpec();
                    break;
                case INTEGER_LIST:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, ListCell.getCollectionType(IntCell.TYPE)).createSpec();
                    break;
                case INTEGER_SET:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, SetCell.getCollectionType(IntCell.TYPE)).createSpec();
                    break;
                case LONG:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, LongCell.TYPE).createSpec();
                    break;
                case LONG_LIST:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, ListCell.getCollectionType(LongCell.TYPE)).createSpec();
                    break;
                case LONG_SET:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, SetCell.getCollectionType(LongCell.TYPE)).createSpec();
                    break;
                case DOUBLE:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, DoubleCell.TYPE).createSpec();
                    break;
                case DOUBLE_LIST:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, ListCell.getCollectionType(DoubleCell.TYPE)).createSpec();
                    break;
                case DOUBLE_SET:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, SetCell.getCollectionType(DoubleCell.TYPE)).createSpec();
                    break;
                case STRING:
                    colSpecs[i] = new DataColumnSpecCreator(columnName, StringCell.TYPE).createSpec();
                    break;
                case STRING_LIST:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE)).createSpec();
                    break;
                case STRING_SET:
                    colSpecs[i] =
                        new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE)).createSpec();
                    break;
                case BYTES:
                    key = spec.getColumnSerializers().get(columnName);
                    if (key != null) {
                        final DataType type =
                            PythonToKnimeExtensions.getExtension(key).getJavaDeserializerFactory().getDataType();
                        if (type.getCellClass() == null) {
                            m_columnsToRetype.put(i, new DataTypeContainer(ResultType.PRIMITIVE));
                        }
                        colSpecs[i] = new DataColumnSpecCreator(columnName, type).createSpec();
                    } else {
                        colSpecs[i] = new DataColumnSpecCreator(columnName, DenseByteVectorCell.TYPE).createSpec();
                    }
                    break;
                case BYTES_LIST:
                    key = spec.getColumnSerializers().get(columnName);
                    if (key != null) {
                        final DataType list_type =
                            PythonToKnimeExtensions.getExtension(key).getJavaDeserializerFactory().getDataType();
                        if (list_type.getCellClass() == null) {
                            m_columnsToRetype.put(i, new DataTypeContainer(ResultType.LIST));
                        }
                        colSpecs[i] =
                            new DataColumnSpecCreator(columnName, ListCell.getCollectionType(list_type)).createSpec();
                    } else {
                        colSpecs[i] = new DataColumnSpecCreator(columnName, ListCell.getCollectionType(StringCell.TYPE))
                            .createSpec();
                    }
                    break;
                case BYTES_SET:
                    key = spec.getColumnSerializers().get(columnName);
                    if (key != null) {
                        final DataType set_type =
                            PythonToKnimeExtensions.getExtension(key).getJavaDeserializerFactory().getDataType();
                        if (set_type.getCellClass() == null) {
                            m_columnsToRetype.put(i, new DataTypeContainer(ResultType.SET));
                        }
                        colSpecs[i] =
                            new DataColumnSpecCreator(columnName, SetCell.getCollectionType(set_type)).createSpec();
                    } else {
                        colSpecs[i] = new DataColumnSpecCreator(columnName, SetCell.getCollectionType(StringCell.TYPE))
                            .createSpec();
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
        } catch (final CanceledExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        final DataCell[] cells = new DataCell[row.getNumberCells()];
        int i = 0;
        for (final Cell cell : row) {
            if (cell.isMissing()) {
                cells[i] = new MissingCell(null);
            } else {
                switch (cell.getColumnType()) {
                    case BOOLEAN:
                        cells[i] = BooleanCellFactory.create(cell.getBooleanValue());
                        break;
                    case BOOLEAN_LIST:
                        final List<DataCell> booleanListCells = new ArrayList<DataCell>();
                        int pos = 0;
                        for (final boolean value : cell.getBooleanArrayValue()) {
                            if (cell.isMissing(pos)) {
                                booleanListCells.add(new MissingCell(null));
                            } else {
                                booleanListCells.add(BooleanCellFactory.create(value));
                            }
                            pos++;
                        }
                        cells[i] = CollectionCellFactory.createListCell(booleanListCells);
                        break;
                    case BOOLEAN_SET:
                        final List<DataCell> booleanSetCells = new ArrayList<DataCell>();
                        for (final boolean value : cell.getBooleanArrayValue()) {
                            booleanSetCells.add(BooleanCellFactory.create(value));
                        }
                        if (cell.hasMissingInSet()) {
                            booleanSetCells.add(new MissingCell(null));
                        }
                        cells[i] = CollectionCellFactory.createSetCell(booleanSetCells);
                        break;
                    case INTEGER:
                        cells[i] = new IntCell(cell.getIntegerValue());
                        break;
                    case INTEGER_LIST:
                        final List<DataCell> integerListCells = new ArrayList<DataCell>();
                        for (int ipos = 0; ipos < cell.getIntegerArrayValue().length; ipos++) {
                            if (cell.isMissing(ipos)) {
                                integerListCells.add(new MissingCell(null));
                            } else {
                                integerListCells.add(new IntCell(cell.getIntegerArrayValue()[ipos]));
                            }
                        }
                        cells[i] = CollectionCellFactory.createListCell(integerListCells);
                        break;
                    case INTEGER_SET:
                        final List<DataCell> integerSetCells = new ArrayList<DataCell>();
                        for (final Integer value : cell.getIntegerArrayValue()) {
                            integerSetCells.add(new IntCell(value));
                        }
                        if (cell.hasMissingInSet()) {
                            integerSetCells.add(new MissingCell(null));
                        }
                        cells[i] = CollectionCellFactory.createSetCell(integerSetCells);
                        break;
                    case LONG:
                        cells[i] = new LongCell(cell.getLongValue());
                        break;
                    case LONG_LIST:
                        final List<DataCell> longListCells = new ArrayList<DataCell>();
                        int lpos = 0;
                        for (final long value : cell.getLongArrayValue()) {
                            if (cell.isMissing(lpos)) {
                                longListCells.add(new MissingCell(null));
                            } else {
                                longListCells.add(new LongCell(value));
                            }
                            lpos++;
                        }
                        cells[i] = CollectionCellFactory.createListCell(longListCells);
                        break;
                    case LONG_SET:
                        final List<DataCell> longSetCells = new ArrayList<DataCell>();
                        for (final long value : cell.getLongArrayValue()) {
                            longSetCells.add(new LongCell(value));
                        }
                        if (cell.hasMissingInSet()) {
                            longSetCells.add(new MissingCell(null));
                        }
                        cells[i] = CollectionCellFactory.createSetCell(longSetCells);
                        break;
                    case DOUBLE:
                        cells[i] = new DoubleCell(cell.getDoubleValue());
                        break;
                    case DOUBLE_LIST:
                        final List<DataCell> doubleListCells = new ArrayList<DataCell>();
                        int dpos = 0;
                        for (final double value : cell.getDoubleArrayValue()) {
                            if (cell.isMissing(dpos)) {
                                doubleListCells.add(new MissingCell(null));
                            } else {
                                doubleListCells.add(new DoubleCell(value));
                            }
                            dpos++;
                        }
                        cells[i] = CollectionCellFactory.createListCell(doubleListCells);
                        break;
                    case DOUBLE_SET:
                        final List<DataCell> doubleSetCells = new ArrayList<DataCell>();
                        for (final double value : cell.getDoubleArrayValue()) {
                            doubleSetCells.add(new DoubleCell(value));
                        }
                        if (cell.hasMissingInSet()) {
                            doubleSetCells.add(new MissingCell(null));
                        }
                        cells[i] = CollectionCellFactory.createSetCell(doubleSetCells);
                        break;
                    case STRING:
                        cells[i] = new StringCell(cell.getStringValue());
                        break;
                    case STRING_LIST:
                        final List<DataCell> stringListCells = new ArrayList<DataCell>();
                        int spos = 0;
                        for (final String value : cell.getStringArrayValue()) {
                            if (cell.isMissing(spos)) {
                                stringListCells.add(new MissingCell(null));
                            } else {
                                stringListCells.add(new StringCell(value));
                            }
                            spos++;
                        }
                        cells[i] = CollectionCellFactory.createListCell(stringListCells);
                        break;
                    case STRING_SET:
                        final List<DataCell> stringSetCells = new ArrayList<DataCell>();
                        for (final String value : cell.getStringArrayValue()) {
                            stringSetCells.add(new StringCell(value));
                        }
                        if (cell.hasMissingInSet()) {
                            stringSetCells.add(new MissingCell(null));
                        }
                        cells[i] = CollectionCellFactory.createSetCell(stringSetCells);
                        break;
                    case BYTES:
                        final String bytesTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
                        if (bytesTypeId != null) {
                            final Deserializer bytesDeserializer = m_pythonToKnimeExtensions
                                .getDeserializer(PythonToKnimeExtensions.getExtension(bytesTypeId).getId());
                            try {
                                if (cell.isMissing()) {
                                    cells[i] = new MissingCell(null);
                                } else {
                                    cells[i] = bytesDeserializer.deserialize(cell.getBytesValue(), m_fileStoreFactory);
                                }
                                final DataTypeContainer dataTypeContainer = m_columnsToRetype.get(i);
                                if (dataTypeContainer != null) {
                                    dataTypeContainer.m_dataTypes.add(cells[i].getType());
                                }
                            } catch (IllegalStateException | IOException e) {
                                LOGGER.error(e.getMessage(), e);
                                cells[i] = new MissingCell(null);
                            }
                        } else {
                            try {
                                if (cell.isMissing()) {
                                    cells[i] = new MissingCell(null);
                                } else {
                                    cells[i] = new DenseByteVectorCellFactory(new DenseByteVector(cell.getBytesValue()))
                                        .createDataCell();
                                }
                            } catch (final IllegalStateException e) {
                                LOGGER.error(e.getMessage(), e);
                                cells[i] = new MissingCell(null);
                            }
                        }
                        break;
                    case BYTES_LIST:
                        final String bytesListTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
                        if (bytesListTypeId != null) {
                            final Deserializer bytesListDeserializer = m_pythonToKnimeExtensions
                                .getDeserializer(PythonToKnimeExtensions.getExtension(bytesListTypeId).getId());
                            final List<DataCell> listCells = new ArrayList<DataCell>();
                            if (cell.isMissing()) {
                                cells[i] = new MissingCell(null);
                            } else {
                                int blpos = 0;
                                for (final byte[] value : cell.getBytesArrayValue()) {
                                    if (cell.isMissing(blpos)) {
                                        listCells.add(new MissingCell(null));
                                    } else {
                                        try {
                                            final DataCell dc =
                                                bytesListDeserializer.deserialize(value, m_fileStoreFactory);
                                            final DataTypeContainer dataTypeContainer = m_columnsToRetype.get(i);
                                            if (dataTypeContainer != null) {
                                                dataTypeContainer.m_dataTypes.add(dc.getType());
                                            }
                                            listCells.add(dc);
                                        } catch (IllegalStateException | IOException e) {
                                            LOGGER.error(e.getMessage(), e);
                                            listCells.add(new MissingCell(null));
                                        }
                                    }
                                    blpos++;
                                }
                                cells[i] = CollectionCellFactory.createListCell(listCells);
                            }
                        } else {
                            if (cell.isMissing()) {
                                cells[i] = new MissingCell(null);
                            } else {
                                final List<DataCell> listCells = new ArrayList<DataCell>();
                                int blpos = 0;
                                for (final byte[] value : cell.getBytesArrayValue()) {
                                    if (cell.isMissing(blpos)) {
                                        listCells.add(new MissingCell(null));
                                    } else {
                                        try {
                                            listCells.add(new StringCell(value.toString()));
                                        } catch (final IllegalStateException e) {
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
                        final String bytesSetTypeId = m_spec.getColumnSerializers().get(m_spec.getColumnNames()[i]);
                        if (bytesSetTypeId != null) {
                            final Deserializer bytesSetDeserializer = m_pythonToKnimeExtensions
                                .getDeserializer(PythonToKnimeExtensions.getExtension(bytesSetTypeId).getId());
                            final List<DataCell> setCells = new ArrayList<DataCell>();
                            if (cell.isMissing()) {
                                cells[i] = new MissingCell(null);
                            } else {
                                for (final byte[] value : cell.getBytesArrayValue()) {
                                    try {
                                        final DataCell dc = bytesSetDeserializer.deserialize(value, m_fileStoreFactory);
                                        final DataTypeContainer dataTypeContainer = m_columnsToRetype.get(i);
                                        if (dataTypeContainer != null) {
                                            dataTypeContainer.m_dataTypes.add(dc.getType());
                                        }
                                        setCells.add(dc);
                                    } catch (IllegalStateException | IOException e) {
                                        LOGGER.error(e.getMessage(), e);
                                        setCells.add(new MissingCell(null));
                                    }

                                }
                                if (cell.hasMissingInSet()) {
                                    setCells.add(new MissingCell(null));
                                }
                                cells[i] = CollectionCellFactory.createSetCell(setCells);
                            }
                        } else {
                            final List<DataCell> setCells = new ArrayList<DataCell>();
                            if (cell.isMissing()) {
                                cells[i] = new MissingCell(null);
                            } else {
                                for (final byte[] value : cell.getBytesArrayValue()) {
                                    try {
                                        setCells.add(new StringCell(value.toString()));
                                    } catch (final IllegalStateException e) {
                                        LOGGER.error(e.getMessage(), e);
                                        setCells.add(new MissingCell(null));
                                    }
                                }
                                if (cell.hasMissingInSet()) {
                                    setCells.add(new MissingCell(null));
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
        m_executionMonitor.setProgress(m_rowsDone / (double)m_tableSize);
    }

    @Override
    public TableSpec getTableSpec() {
        return m_spec;
    }

    private DataType getMostCommonAncestor(final HashSet<DataType> types) {
        final Iterator<DataType> iter = types.iterator();
        DataType mca = iter.next();
        while (iter.hasNext()) {
            mca = DataType.getCommonSuperType(mca, iter.next());
        }
        return mca;
    }

    @Override
    public BufferedDataTable getTable() {
        m_container.close();
        final DataTableSpec tableSpec = m_container.getTableSpec();
        final DataColumnSpec[] colSpecs = new DataColumnSpec[tableSpec.getNumColumns()];
        for (int i = 0; i < colSpecs.length; i++) {
            final DataColumnSpec dcs = tableSpec.getColumnSpec(i);
            final DataColumnSpecCreator dcsc = new DataColumnSpecCreator(dcs);
            if (m_columnsToRetype.containsKey(i)) {
                final DataTypeContainer dtContainer = m_columnsToRetype.get(i);
                final DataType elementType = getMostCommonAncestor(dtContainer.m_dataTypes);
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
        final DataTableSpec correctedSpec = new DataTableSpec(tableSpec.getName(), colSpecs);
        return m_exec.createSpecReplacerTable(m_container.getTable(), correctedSpec);
    }

    /**
     * Enum for distinguishing if a cell contains primitives or collections (either lists or sets).
     */
    private enum ResultType {
            PRIMITIVE, LIST, SET;
    }

    /**
     * Container class used for storing all data types present in a certain column. Also indicates if the objects in the
     * column have a primitive or a collection type.
     */
    private class DataTypeContainer {
        ResultType m_resultType;

        HashSet<DataType> m_dataTypes;

        public DataTypeContainer(final ResultType type) {
            m_resultType = type;
            m_dataTypes = new HashSet<DataType>();
        }
    }

}
