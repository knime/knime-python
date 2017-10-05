/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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

package org.knime.python2.serde.flatbuffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.VectorExtractor;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.serde.flatbuffers.extractors.BooleanExtractor;
import org.knime.python2.serde.flatbuffers.extractors.BooleanListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.BooleanSetExtractor;
import org.knime.python2.serde.flatbuffers.extractors.BytesExtractor;
import org.knime.python2.serde.flatbuffers.extractors.BytesListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.BytesSetExtractor;
import org.knime.python2.serde.flatbuffers.extractors.DoubleExtractor;
import org.knime.python2.serde.flatbuffers.extractors.DoubleListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.DoubleSetExtractor;
import org.knime.python2.serde.flatbuffers.extractors.IntExtractor;
import org.knime.python2.serde.flatbuffers.extractors.IntListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.IntSetExtractor;
import org.knime.python2.serde.flatbuffers.extractors.LongExtractor;
import org.knime.python2.serde.flatbuffers.extractors.LongListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.LongSetExtractor;
import org.knime.python2.serde.flatbuffers.extractors.StringExtractor;
import org.knime.python2.serde.flatbuffers.extractors.StringListExtractor;
import org.knime.python2.serde.flatbuffers.extractors.StringSetExtractor;
import org.knime.python2.serde.flatbuffers.flatc.BooleanCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.BooleanColumn;
import org.knime.python2.serde.flatbuffers.flatc.ByteCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.ByteColumn;
import org.knime.python2.serde.flatbuffers.flatc.Column;
import org.knime.python2.serde.flatbuffers.flatc.DoubleCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.DoubleColumn;
import org.knime.python2.serde.flatbuffers.flatc.IntCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.IntColumn;
import org.knime.python2.serde.flatbuffers.flatc.KnimeTable;
import org.knime.python2.serde.flatbuffers.flatc.LongCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.LongColumn;
import org.knime.python2.serde.flatbuffers.flatc.StringCollectionColumn;
import org.knime.python2.serde.flatbuffers.flatc.StringColumn;
import org.knime.python2.serde.flatbuffers.inserters.BooleanInserter;
import org.knime.python2.serde.flatbuffers.inserters.BooleanListInserter;
import org.knime.python2.serde.flatbuffers.inserters.BooleanSetInserter;
import org.knime.python2.serde.flatbuffers.inserters.BytesInserter;
import org.knime.python2.serde.flatbuffers.inserters.BytesListInserter;
import org.knime.python2.serde.flatbuffers.inserters.BytesSetInserter;
import org.knime.python2.serde.flatbuffers.inserters.DoubleInserter;
import org.knime.python2.serde.flatbuffers.inserters.DoubleListInserter;
import org.knime.python2.serde.flatbuffers.inserters.DoubleSetInserter;
import org.knime.python2.serde.flatbuffers.inserters.FlatbuffersVectorInserter;
import org.knime.python2.serde.flatbuffers.inserters.IntInserter;
import org.knime.python2.serde.flatbuffers.inserters.IntListInserter;
import org.knime.python2.serde.flatbuffers.inserters.IntSetInserter;
import org.knime.python2.serde.flatbuffers.inserters.LongInserter;
import org.knime.python2.serde.flatbuffers.inserters.LongListInserter;
import org.knime.python2.serde.flatbuffers.inserters.LongSetInserter;
import org.knime.python2.serde.flatbuffers.inserters.StringInserter;
import org.knime.python2.serde.flatbuffers.inserters.StringListInserter;
import org.knime.python2.serde.flatbuffers.inserters.StringSetInserter;

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

        List<FlatbuffersVectorInserter> inserters = new ArrayList<FlatbuffersVectorInserter>();

        Type[] types = tableIterator.getTableSpec().getColumnTypes();
        String[] names = tableIterator.getTableSpec().getColumnNames();
        Map<String,String> serializers = tableIterator.getTableSpec().getColumnSerializers();
        int numRows = tableIterator.getNumberRemainingRows();
        for(int i=0; i<types.length; i++) {
            switch (types[i]) {
                case BOOLEAN: {
                    inserters.add(new BooleanInserter(numRows));
                    break;
                }
                case BOOLEAN_LIST: {
                    inserters.add(new BooleanListInserter(numRows));
                    break;
                }
                case BOOLEAN_SET: {
                    inserters.add(new BooleanSetInserter(numRows));
                    break;
                }
                case INTEGER: {
                    inserters.add(new IntInserter(numRows, serializationOptions));
                    break;
                }
                case INTEGER_LIST: {
                    inserters.add(new IntListInserter(numRows));
                    break;
                }
                case INTEGER_SET: {
                    inserters.add(new IntSetInserter(numRows));
                    break;
                }
                case LONG: {
                    inserters.add(new LongInserter(numRows, serializationOptions));
                    break;
                }
                case LONG_LIST: {
                    inserters.add(new LongListInserter(numRows));
                    break;
                }
                case LONG_SET: {
                    inserters.add(new LongSetInserter(numRows));
                    break;
                }
                case DOUBLE: {
                    inserters.add(new DoubleInserter(numRows));
                    break;
                }
                case DOUBLE_LIST: {
                    inserters.add(new DoubleListInserter(numRows));
                    break;
                }
                case DOUBLE_SET: {
                    inserters.add(new DoubleSetInserter(numRows));
                    break;
                }
                case STRING: {
                    inserters.add(new StringInserter(numRows));
                    break;
                }
                case STRING_LIST: {
                    inserters.add(new StringListInserter(numRows));
                    break;
                }
                case STRING_SET: {
                    inserters.add(new StringSetInserter(numRows));
                    break;
                }
                case BYTES: {
                    inserters.add(new BytesInserter(numRows, serializers.get(names[i])));
                    break;
                }
                case BYTES_LIST: {
                    inserters.add(new BytesListInserter(numRows, serializers.get(names[i])));
                    break;
                }
                case BYTES_SET: {
                    inserters.add(new BytesSetInserter(numRows, serializers.get(names[i])));
                    break;
                }
                default:
                    break;
            }
        }

        final int[] rowIdOffsets = new int[numRows];

        int rowIdx = 0;
        // Convert the rows to columns
        while (tableIterator.hasNext()) {
            final Row row = tableIterator.next();
            rowIdOffsets[rowIdx] = builder.createString(row.getRowKey());

            for(int i=0; i<inserters.size(); i++) {
                inserters.get(i).put(row.getCell(i));
            }
            rowIdx++;
        }
        int numCols = tableIterator.getTableSpec().getNumberColumns();
        final int[] colOffsets = new int[numCols];
        final int[] colNameOffsets = new int[numCols];

        int colIdx = 0;
        for (final FlatbuffersVectorInserter inserter:inserters) {
            colOffsets[colIdx] = inserter.createColumn(builder);
            colNameOffsets[colIdx] = builder.createString(names[colIdx]);
            colIdx++;
        }

        final int rowIdVecOffset = KnimeTable.createRowIDsVector(builder, rowIdOffsets);

        final int colNameVecOffset = KnimeTable.createColNamesVector(builder, colNameOffsets);

        final int colVecOffset = KnimeTable.createColumnsVector(builder, colOffsets);

        KnimeTable.startKnimeTable(builder);
        KnimeTable.addRowIDs(builder, rowIdVecOffset);
        KnimeTable.addColNames(builder, colNameVecOffset);
        KnimeTable.addColumns(builder, colVecOffset);
        final int knimeTable = KnimeTable.endKnimeTable(builder);
        builder.finish(knimeTable);

        return builder.sizedByteArray();
    }

    @Override
    public void bytesIntoTable(final TableCreator<?> tableCreator, final byte[] bytes,
        final SerializationOptions serializationOptions) {

        final KnimeTable table = KnimeTable.getRootAsKnimeTable(ByteBuffer.wrap(bytes));
        final Map<String, Type> colTypes = new HashMap<>();

        List<VectorExtractor> extractors = new ArrayList<VectorExtractor>();

        if (table.colNamesLength() == 0) {
            return;
        }

        for (int j = 0; j < table.columnsLength(); j++) {

            final Column col = table.columns(j);
            switch (Type.getTypeForId(col.type())) {
                case BOOLEAN: {
                    final BooleanColumn colVec = col.booleanColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN);
                    extractors.add(new BooleanExtractor(colVec));
                    break;
                }
                case BOOLEAN_LIST: {
                    final BooleanCollectionColumn colVec = col.booleanListColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN_LIST);
                    extractors.add(new BooleanListExtractor(colVec));
                    break;
                }
                case BOOLEAN_SET: {
                    final BooleanCollectionColumn colVec = col.booleanSetColumn();
                    colTypes.put(table.colNames(j), Type.BOOLEAN_SET);
                    extractors.add(new BooleanSetExtractor(colVec));
                    break;
                }
                case INTEGER: {
                    final IntColumn colVec = col.intColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER);
                    extractors.add(new IntExtractor(colVec, serializationOptions));
                    break;
                }
                case INTEGER_LIST: {
                    final IntCollectionColumn colVec = col.intListColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER_LIST);
                    extractors.add(new IntListExtractor(colVec));
                    break;
                }
                case INTEGER_SET: {
                    final IntCollectionColumn colVec = col.intSetColumn();
                    colTypes.put(table.colNames(j), Type.INTEGER_SET);
                    extractors.add(new IntSetExtractor(colVec));
                    break;
                }
                case LONG: {
                    final LongColumn colVec = col.longColumn();
                    colTypes.put(table.colNames(j), Type.LONG);
                    extractors.add(new LongExtractor(colVec, serializationOptions));
                    break;
                }
                case LONG_LIST: {
                    final LongCollectionColumn colVec = col.longListColumn();
                    colTypes.put(table.colNames(j), Type.LONG_LIST);
                    extractors.add(new LongListExtractor(colVec));
                    break;
                }
                case LONG_SET: {
                    final LongCollectionColumn colVec = col.longSetColumn();
                    colTypes.put(table.colNames(j), Type.LONG_SET);
                    extractors.add(new LongSetExtractor(colVec));
                    break;
                }
                case DOUBLE: {
                    final DoubleColumn colVec = col.doubleColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE);
                    extractors.add(new DoubleExtractor(colVec));

                    break;
                }
                case DOUBLE_LIST: {
                    final DoubleCollectionColumn colVec = col.doubleListColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE_LIST);
                    extractors.add(new DoubleListExtractor(colVec));
                    break;
                }
                case DOUBLE_SET: {
                    final DoubleCollectionColumn colVec = col.doubleSetColumn();
                    colTypes.put(table.colNames(j), Type.DOUBLE_SET);
                    extractors.add(new DoubleSetExtractor(colVec));
                    break;
                }
                case STRING: {
                    final StringColumn colVec = col.stringColumn();
                    colTypes.put(table.colNames(j), Type.STRING);
                    extractors.add(new StringExtractor(colVec));
                    break;
                }
                case STRING_LIST: {
                    final StringCollectionColumn colVec = col.stringListColumn();
                    colTypes.put(table.colNames(j), Type.STRING_LIST);
                    extractors.add(new StringListExtractor(colVec));
                    break;
                }
                case STRING_SET: {
                    final StringCollectionColumn colVec = col.stringSetColumn();
                    colTypes.put(table.colNames(j), Type.STRING_SET);
                    extractors.add(new StringSetExtractor(colVec));
                    break;
                }
                case BYTES: {

                    final ByteColumn colVec = col.byteColumn();
                    colTypes.put(table.colNames(j), Type.BYTES);
                    extractors.add(new BytesExtractor(colVec));

                    break;
                }
                case BYTES_LIST: {
                    final ByteCollectionColumn colVec = col.byteListColumn();
                    colTypes.put(table.colNames(j), Type.BYTES_LIST);
                    extractors.add(new BytesListExtractor(colVec));
                    break;
                }
                case BYTES_SET: {
                    final ByteCollectionColumn colVec = col.byteSetColumn();
                    colTypes.put(table.colNames(j), Type.BYTES_SET);
                    extractors.add(new BytesSetExtractor(colVec));
                    break;
                }
                default:
                    break;

            }
        }

        final int numRows = table.rowIDsLength();
        final int numCols = table.colNamesLength();

        for (int rowCount = 0; rowCount < numRows; rowCount++) {
            //final Row r = new RowImpl(table.rowIDs(rowCount), numCols);
            final Row r = new RowImpl(table.rowID(rowCount), numCols);
            for (int colCount=0; colCount<numCols; colCount++) {
                r.setCell(extractors.get(colCount).extract(), colCount);
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
