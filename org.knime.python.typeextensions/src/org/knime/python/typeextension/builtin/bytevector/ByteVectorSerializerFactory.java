package org.knime.python.typeextension.builtin.bytevector;

import java.io.IOException;

import org.knime.core.data.vector.bytevector.ByteVectorValue;
import org.knime.python.typeextension.Serializer;
import org.knime.python.typeextension.SerializerFactory;

/**
 * Serialize DenseByteVector to bytearray in python.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class ByteVectorSerializerFactory extends SerializerFactory<ByteVectorValue> {

    public ByteVectorSerializerFactory() {
        super(ByteVectorValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializer<? extends ByteVectorValue> createSerializer() {
        return new ByteVectorSerializer();
    }

    private class ByteVectorSerializer implements Serializer<ByteVectorValue> {

        /**
         * {@inheritDoc}
         */
        @Override
        public byte[] serialize(ByteVectorValue value) throws IOException {
            byte[] bytearray = new byte[(int) value.length()];
            for (int i = 0; i < (int) value.length(); i++) {
                bytearray[i] = (byte) value.get(i);
            }
            return bytearray;
        }

    }

}