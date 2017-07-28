package org.knime.python2.serde.arrow.libraryextensions;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Based on ArrowStreamWriter. Exposing writeRecordBatch for writing output batch by batch.
 * 
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */

public class ArrowBatchWriter extends ArrowWriter {

    public ArrowBatchWriter(VectorSchemaRoot root, DictionaryProvider provider, OutputStream out) {
       this(root, provider, Channels.newChannel(out));
    }

    public ArrowBatchWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
       super(root, provider, out);
    }

    @Override
    protected void startInternal(WriteChannel out) throws IOException {}

    @Override
    protected void endInternal(WriteChannel out,
                               Schema schema,
                               List<ArrowBlock> dictionaries,
                               List<ArrowBlock> records) throws IOException {
       out.writeIntLittleEndian(0);
    }
    
    public void writeRecordBatch(ArrowRecordBatch batch) throws IOException {
        super.start();
        super.writeRecordBatch(batch);
    }
    
    @Override
    public void close() {
        try {
            super.end();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        super.close();
    }
}