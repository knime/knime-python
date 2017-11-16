package org.knime.python.typeextension.builtin.bytevector;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.vector.bytevector.DenseByteVector;
import org.knime.core.data.vector.bytevector.DenseByteVectorCell;
import org.knime.core.data.vector.bytevector.DenseByteVectorCellFactory;
import org.knime.python.typeextension.Deserializer;
import org.knime.python.typeextension.DeserializerFactory;

public class ByteVectorDeserializerFactory extends DeserializerFactory{
    
    public ByteVectorDeserializerFactory() {
        super(DataType.getType(DenseByteVectorCell.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Deserializer createDeserializer() {
        return new ByteVectorDeserializerDeserializer();
    }

    private class ByteVectorDeserializerDeserializer implements Deserializer {

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
            DenseByteVector vector = new DenseByteVector(bytes);
            DenseByteVectorCellFactory df = new DenseByteVectorCellFactory(vector);
            return df.createDataCell();
        }

    }
}
