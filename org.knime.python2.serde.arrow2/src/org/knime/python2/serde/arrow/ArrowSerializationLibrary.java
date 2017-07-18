package org.knime.python2.serde.arrow;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.NodeLogger;
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

public class ArrowSerializationLibrary implements SerializationLibrary {

    @Override
    public byte[] tableToBytes(TableIterator tableIterator, SerializationOptions serializationOptions) {
        String path = "/tmp/memory_mapped.dat";
        try {
            FileChannel fc = new RandomAccessFile(new File(path), "rw").getChannel();
            //Get metadata
            JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
            TableSpec spec = tableIterator.getTableSpec();
            List<FieldVector> vecs = new ArrayList<FieldVector>();
            List<Field> fields = new ArrayList<Field>();
            JsonArrayBuilder icBuilder = Json.createArrayBuilder();
            icBuilder.add("__index_level_0__");
            metadataBuilder.add("index_columns", icBuilder);
            JsonArrayBuilder colBuilder = Json.createArrayBuilder();
            //Row ids
            JsonObjectBuilder rowIdBuilder = Json.createObjectBuilder();
            rowIdBuilder.add("name", "__index_level_0__");
            rowIdBuilder.add("pandas_type", "unicode");
            rowIdBuilder.add("numpy_type", "object");
            //TODO add serializer
            rowIdBuilder.add("metadata", Type.STRING.getId());
            colBuilder.add(rowIdBuilder);
            //TODO calculate space and set Allocator size accordingly
            NullableVarCharVector rowIdVector = new NullableVarCharVector("__index_level_0__", new RootAllocator(1024*1024));
            rowIdVector.allocateNew();
            vecs.add(rowIdVector);
            //Field rowIdField = new Field("__index_level_0__", new FieldType(false, new ArrowType.Utf8(), null), null);
            fields.add(rowIdVector.getField());
            
            for(int i=0; i<spec.getNumberColumns(); i++) {
                JsonObjectBuilder colMetadataBuilder = Json.createObjectBuilder();
                colMetadataBuilder.add("name", spec.getColumnNames()[i]);
                FieldVector vec;
                switch(spec.getColumnTypes()[i]) {
                    //TODO null value handling
                    case INTEGER:
                        colMetadataBuilder.add("pandas", "int");
                        colMetadataBuilder.add("numpy", "int32");
                      //TODO add serializer
                        colMetadataBuilder.add("metadata", Type.INTEGER.getId());
                        //Allocate vector for column
                        vec = new NullableIntVector(spec.getColumnNames()[i], new RootAllocator(1024*1024));
                        vecs.add(vec);
                        fields.add(vec.getField());
                        break;
                    case STRING:
                        colMetadataBuilder.add("pandas_type", "unicode");
                        colMetadataBuilder.add("numpy_type", "object");
                        //TODO add serializer
                        colMetadataBuilder.add("metadata", Type.STRING.getId());
                        vec = new NullableVarCharVector(spec.getColumnNames()[i], new RootAllocator(10000*1024));
                        vecs.add(vec);
                        fields.add(vec.getField());
                        break;
                    default:
                        throw new IllegalStateException("Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                }
                colBuilder.add(colMetadataBuilder);   
            }
            metadataBuilder.add("columns", colBuilder);
            //Schema schema = Schema.fromJSON(metadataBuilder.build().toString());
            int ctr = 0;
            while(tableIterator.hasNext()) {
                Row row = tableIterator.next();
                ((NullableVarCharVector) vecs.get(0)).allocateNew();
                ((NullableVarCharVector.Mutator) vecs.get(0).getMutator()).set(ctr, row.getRowKey().getBytes("UTF-8"));
                ((NullableVarCharVector.Mutator) vecs.get(0).getMutator()).setValueCount(ctr + 1);
                for(int i=0; i<spec.getNumberColumns(); i++) {
                    switch(spec.getColumnTypes()[i]) {
                    //TODO null value handling
                    case INTEGER:
                        ((NullableIntVector) vecs.get(i+1)).allocateNew();
                        ((NullableIntVector.Mutator) vecs.get(i+1).getMutator()).set(ctr, row.getCell(i).getIntegerValue().intValue());
                        ((NullableIntVector.Mutator) vecs.get(i+1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case STRING:
                        ((NullableVarCharVector) vecs.get(i+1)).allocateNew();
                        ((NullableVarCharVector.Mutator) vecs.get(i+1).getMutator()).set(ctr, row.getCell(i).getStringValue().getBytes("UTF-8"));
                        ((NullableVarCharVector.Mutator) vecs.get(i+1).getMutator()).setValueCount(ctr + 1);
                        break;
                    default:
                        throw new IllegalStateException("Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                    }
                }
                ctr++;
            }
            
            //TODO custom meta data ?   
            VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vecs, ctr);
            //TODO schöner
            /*JsonReader jsreader = Json.createReader(new StringReader(vsr.getSchema().toJson()));
            JsonObject obj = jsreader.readObject();
            JsonObjectBuilder builder = Json.createObjectBuilder();
            for(Entry<String,JsonValue> entry:obj.entrySet()) {
                builder.add(entry.getKey(), entry.getValue());
            }
            //builder.add("customMetadata", metadataBuilder.build());
            vsr.setSchema(Schema.fromJSON(builder.build().toString()));
            VectorSchemaRoot extendedVsr = VectorSchemaRoot.create(Schema.fromJSON(builder.build().toString()), ) */
            
            ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, fc);
            writer.writeBatch();
            writer.close();
            
            return path.getBytes("UTF-8");
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return null;
    } 
    
    /*class ExchangableSchemaVectorSchemaRoot extends VectorSchemaRoot {
        public ExchangableSchemaVectorSchemaRoot(List<Field> fields, List<FieldVector> fieldVectors, int rowCount) {
            super(fields, fieldVectors, rowCount);
        }

        public void setSchema(Schema schema) {
            this.
        }
    }*/
    
   /* @Override
    public byte[] tableToBytes(final TableIterator tableIterator, final SerializationOptions serializationOptions) {
        try {
            final File file = File.createTempFile("java-to-python-", ".csv");
            file.deleteOnExit();
            final FileWriter writer = new FileWriter(file);
            String types = "#";
            String names = "";
            final TableSpec spec = tableIterator.getTableSpec();
            for (int i = 0; i < spec.getNumberColumns(); i++) {
                types += "," + spec.getColumnTypes()[i].getId();
                names += "," + spec.getColumnNames()[i];
            }
            String serializers = "#";
            for (final Entry<String, String> entry : spec.getColumnSerializers().entrySet()) {
                serializers += ',' + entry.getKey() + '=' + entry.getValue();
            }
            writer.write(types + "\n");
            writer.write(serializers + "\n");
            writer.write(names + "\n");
            int ctr;
            while (tableIterator.hasNext()) {
                final Row row = tableIterator.next();
                String line = row.getRowKey();
                ctr = 0;
                for (final Cell cell : row) {
                    String value = "";
                    if (cell.isMissing()) {
                        value = "MissingCell";
                        final Type type = spec.getColumnTypes()[ctr];
                        if (serializationOptions.getConvertMissingToPython()
                                && ((type == Type.INTEGER) || (type == Type.LONG))) {
                            value = Long.toString(serializationOptions.getSentinelForType(type));
                        }
                    } else {
                        switch (cell.getColumnType()) {
                            case BOOLEAN:
                                value = cell.getBooleanValue() ? "True" : "False";
                                break;
                            case BOOLEAN_LIST:
                            case BOOLEAN_SET:
                                final Boolean[] booleanArray = cell.getBooleanArrayValue();
                                final StringBuilder booleanBuilder = new StringBuilder();
                                booleanBuilder.append(cell.getColumnType() == Type.BOOLEAN_LIST ? "[" : "{");
                                for (int i = 0; i < booleanArray.length; i++) {
                                    if (booleanArray[i] == null) {
                                        booleanBuilder.append("None");
                                    } else {
                                        booleanBuilder.append(booleanArray[i] ? "True" : "False");
                                    }
                                    if ((i + 1) < booleanArray.length) {
                                        booleanBuilder.append(",");
                                    }
                                }
                                booleanBuilder.append(cell.getColumnType() == Type.BOOLEAN_LIST ? "]" : "}");
                                value = booleanBuilder.toString();
                                break;
                            case INTEGER:
                                value = cell.getIntegerValue().toString();
                                break;
                            case INTEGER_LIST:
                            case INTEGER_SET:
                                final Integer[] integerArray = cell.getIntegerArrayValue();
                                final StringBuilder integerBuilder = new StringBuilder();
                                integerBuilder.append(cell.getColumnType() == Type.INTEGER_LIST ? "[" : "{");
                                for (int i = 0; i < integerArray.length; i++) {
                                    if (integerArray[i] == null) {
                                        integerBuilder.append("None");
                                    } else {
                                        integerBuilder.append(integerArray[i].toString());
                                    }
                                    if ((i + 1) < integerArray.length) {
                                        integerBuilder.append(",");
                                    }
                                }
                                integerBuilder.append(cell.getColumnType() == Type.INTEGER_LIST ? "]" : "}");
                                value = integerBuilder.toString();
                                break;
                            case LONG:
                                value = cell.getLongValue().toString();
                                break;
                            case LONG_LIST:
                            case LONG_SET:
                                final Long[] longArray = cell.getLongArrayValue();
                                final StringBuilder longBuilder = new StringBuilder();
                                longBuilder.append(cell.getColumnType() == Type.LONG_LIST ? "[" : "{");
                                for (int i = 0; i < longArray.length; i++) {
                                    if (longArray[i] == null) {
                                        longBuilder.append("None");
                                    } else {
                                        longBuilder.append(longArray[i].toString());
                                    }
                                    if ((i + 1) < longArray.length) {
                                        longBuilder.append(",");
                                    }
                                }
                                longBuilder.append(cell.getColumnType() == Type.LONG_LIST ? "]" : "}");
                                value = longBuilder.toString();
                                break;
                            case DOUBLE:
                                final Double doubleValue = cell.getDoubleValue();
                                if (Double.isInfinite(doubleValue)) {
                                    if (doubleValue > 0) {
                                        value = "inf";
                                    } else {
                                        value = "-inf";
                                    }
                                } else if (Double.isNaN(doubleValue)) {
                                    value = "NaN";
                                } else {
                                    value = doubleValue.toString();
                                }
                                break;
                            case DOUBLE_LIST:
                            case DOUBLE_SET:
                                final Double[] doubleArray = cell.getDoubleArrayValue();
                                final StringBuilder doubleBuilder = new StringBuilder();
                                doubleBuilder.append(cell.getColumnType() == Type.DOUBLE_LIST ? "[" : "{");
                                for (int i = 0; i < doubleArray.length; i++) {
                                    if (doubleArray[i] == null) {
                                        doubleBuilder.append("None");
                                    } else {
                                        String doubleVal = doubleArray[i].toString();
                                        if (doubleVal.equals("NaN")) {
                                            doubleVal = "float('nan')";
                                        } else if (doubleVal.equals("-Infinity")) {
                                            doubleVal = "float('-inf')";
                                        } else if (doubleVal.equals("Infinity")) {
                                            doubleVal = "float('inf')";
                                        }
                                        doubleBuilder.append(doubleVal);
                                    }
                                    if ((i + 1) < doubleArray.length) {
                                        doubleBuilder.append(",");
                                    }
                                }
                                doubleBuilder.append(cell.getColumnType() == Type.DOUBLE_LIST ? "]" : "}");
                                value = doubleBuilder.toString();
                                break;
                            case STRING:
                                value = cell.getStringValue();
                                break;
                            case STRING_LIST:
                            case STRING_SET:
                                final String[] stringArray = cell.getStringArrayValue();
                                final StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append(cell.getColumnType() == Type.STRING_LIST ? "[" : "{");
                                for (int i = 0; i < stringArray.length; i++) {
                                    String stringValue = stringArray[i];
                                    if (stringValue == null) {
                                        stringBuilder.append("None");
                                    } else {
                                        stringValue = stringValue.replace("\\", "\\\\");
                                        stringValue = stringValue.replace("'", "\\'");
                                        stringValue = stringValue.replace("\r", "\\r");
                                        stringValue = stringValue.replace("\n", "\\n");
                                        stringBuilder.append("'" + stringValue + "'");
                                    }
                                    if ((i + 1) < stringArray.length) {
                                        stringBuilder.append(",");
                                    }
                                }
                                stringBuilder.append(cell.getColumnType() == Type.STRING_LIST ? "]" : "}");
                                value = stringBuilder.toString();
                                break;
                            case BYTES:
                                value = bytesToBase64(cell.getBytesValue());
                                break;
                            case BYTES_LIST:
                            case BYTES_SET:
                                final Byte[][] bytesArray = cell.getBytesArrayValue();
                                final StringBuilder bytesBuilder = new StringBuilder();
                                bytesBuilder.append(cell.getColumnType() == Type.BYTES_LIST ? "[" : "{");
                                for (int i = 0; i < bytesArray.length; i++) {
                                    final Byte[] bytesValue = bytesArray[i];
                                    if (bytesValue == null) {
                                        bytesBuilder.append("None");
                                    } else {
                                        bytesBuilder.append("'" + bytesToBase64(bytesValue) + "'");
                                    }
                                    if ((i + 1) < bytesArray.length) {
                                        bytesBuilder.append(",");
                                    }
                                }
                                bytesBuilder.append(cell.getColumnType() == Type.BYTES_LIST ? "]" : "}");
                                value = bytesBuilder.toString();
                                break;
                            default:
                                break;
                        }
                    }
                    value = escapeValue(value);
                    line += "," + value;
                    ctr++;
                }
                writer.write(line + "\n");
            }
            writer.close();
            return file.getAbsolutePath().getBytes();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    } */
    
    ArrowStreamReader m_streamReader = null;
    
    private ArrowStreamReader getReader(String path) throws FileNotFoundException {
        if(m_streamReader == null) {
                FileChannel fc = new RandomAccessFile(new File(path), "rw").getChannel();
                ArrowStreamReader reader = new ArrowStreamReader((ReadableByteChannel) fc, 
                        (BufferAllocator) new RootAllocator(1024));
                m_streamReader = reader;
        }
        return m_streamReader;
    }

    @Override
    public void bytesIntoTable(TableCreator<?> tableCreator, byte[] bytes, SerializationOptions serializationOptions) {
        //TODO sentinel
        String path = new String(bytes);
        TableSpec spec = tableSpecFromBytes(bytes);
        try {
            ArrowStreamReader reader = getReader(path);
            //Index is always string
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            NullableVarCharVector indexCol = (NullableVarCharVector) root.getVector(m_indexColumnName);
            NullableVarCharVector.Accessor rowKeyAccessor = (NullableVarCharVector.Accessor) indexCol.getAccessor();
            for(int i=0; i<rowKeyAccessor.getValueCount(); i++) {
               Row row = new RowImpl(rowKeyAccessor.getObject(i).toString(), spec.getNumberColumns());
               for(int j=0; j<spec.getNumberColumns(); j++) {
                   if(spec.getColumnTypes()[j] == Type.STRING) {
                       row.setCell( new CellImpl(((NullableVarCharVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).getObject(i).toString()), j );
                   } else if(spec.getColumnTypes()[j] == Type.LONG) {
                       row.setCell( new CellImpl(((NullableBigIntVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).getObject(i)), j );
                   } else {
                       throw new IllegalStateException("Unknown column type!");
                   }
               }
               tableCreator.addRow(row);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        m_streamReader = null;
        m_tableSpec = null;
    }
    
    TableSpec m_tableSpec = null;
    //NOTE: we will never get a multiindex due to index standartization in FromPandasTable
    String m_indexColumnName = null;

    @Override
    public TableSpec tableSpecFromBytes(byte[] bytes) {
        
        if(m_tableSpec == null) {
            String path = new String(bytes);
            try {
                ArrowStreamReader reader = getReader(path);
                boolean succ = reader.loadNextBatch();
                NodeLogger.getLogger("test").warn("Success on loading batch: " + succ);
                Schema schema = reader.getVectorSchemaRoot().getSchema();
                Map<String,String> metadata = schema.getCustomMetadata();
                String pandas_metadata = metadata.get("pandas");
                if(pandas_metadata != null) {
                    NodeLogger.getLogger("test").warn("pandas metadata found: " + pandas_metadata);
                    JsonReader jsreader = Json.createReader(new StringReader(pandas_metadata));
                    JsonObject jpandas_metadata = jsreader.readObject();
                    JsonArray index_cols = jpandas_metadata.getJsonArray("index_columns");
                    JsonArray cols = jpandas_metadata.getJsonArray("columns");
                    String[] names = new String[cols.size() - index_cols.size()];
                    Type[] types = new Type[cols.size() - index_cols.size()];
                    //TODO send and get serializers, send tpyes (use metadata field in pandas metadata ?!)
                    int noIdxCtr = 0;
                    for(int i=0; i<cols.size(); i++) {
                        JsonObject col = cols.getJsonObject(i);
                        boolean contained = false;
                        for(int j=0; j<index_cols.size(); j++) {
                            if(index_cols.getString(j).contentEquals(col.getString("name"))) {
                                contained = true;
                                break;
                            }
                        }
                        if(contained) {
                            m_indexColumnName = col.getString("name");
                            continue;
                        }
                        names[noIdxCtr] = col.getString("name");
                        String type = col.getString("pandas_type");
                        switch(type) {
                            case "unicode":
                                types[noIdxCtr] = Type.STRING;
                                break;
                            case "int64":
                                types[noIdxCtr] = Type.LONG;
                                break;
                            default:
                                throw new IllegalStateException("Cannot process type: " + type);
                        }
                        noIdxCtr++;
                    }
                    m_tableSpec = new TableSpecImpl(types, names, null);
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        if(m_tableSpec == null) {
            throw new IllegalStateException("Could not build TableSpec!");
        }
        
        return m_tableSpec;
    }

    private static String escapeValue(String value) {
        value = value.replace("\"", "\"\"");
        if (value.contains("\"") || value.contains("\n") || value.contains(",") || value.contains("\r")) {
            value = "\"" + value + "\"";
        }
        return value;
    }

    private static String bytesToBase64(final Byte[] bytes) {
        return new String(Base64.getEncoder().encode(ArrayUtils.toPrimitive(bytes)));
    }

}
