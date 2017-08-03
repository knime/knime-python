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

package org.knime.python2.serde.arrow.libraryextensions;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Based on ArrowStreamWriter. Exposing writeRecordBatch for writing output batch by batch.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class ArrowBatchWriter extends ArrowWriter {

    /**
     * @param root
     * @param provider
     * @param out
     */
    public ArrowBatchWriter(final VectorSchemaRoot root, final DictionaryProvider provider, final OutputStream out) {
        this(root, provider, Channels.newChannel(out));
    }

    /**
     *
     * @param root
     * @param provider
     * @param out
     */
    public ArrowBatchWriter(final VectorSchemaRoot root, final DictionaryProvider provider,
        final WritableByteChannel out) {
        super(root, provider, out);
    }

    @Override
    protected void startInternal(final WriteChannel out) throws IOException {
        // noop
    }

    @Override
    protected void endInternal(final WriteChannel out, final Schema schema, final List<ArrowBlock> dictionaries,
        final List<ArrowBlock> records) throws IOException {
        out.writeIntLittleEndian(0);
    }

    @Override
    public void writeRecordBatch(final ArrowRecordBatch batch) throws IOException {
        super.start();
        super.writeRecordBatch(batch);
    }

    @Override
    public void close() {
        try {
            super.end();
        } catch (IOException e) {
            // TODO logging
            // nothing we can really do
        }
        super.close();
    }
}