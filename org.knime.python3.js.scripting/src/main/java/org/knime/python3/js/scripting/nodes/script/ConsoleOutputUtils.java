/*
 * ------------------------------------------------------------------------
 *
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Sep 9, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.PathBackedFileHandle;
import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.util.PathUtils;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

/**
 * Utilities for saving and loading the standard streams output of a process.
 *
 * The usual workflow is as following.
 * <ol>
 * <li>Create the console consumer with {@link #createConsoleConsumer()}</li>
 * <li>Feed the console outputs of a process to the {@link ConsoleOutputConsumer}</li>
 * <li>Finish the consumer by calling {@link ConsoleOutputConsumer#finish()} which returns a
 * {@link ConsoleOutputStorage}</li>
 * <li>Re-send the output to another consumer with {@link #sendConsoleOutputs(ConsoleOutputStorage, Consumer)}</li>
 * <li>Persist the storage to a file system directory with {@link #saveConsoleOutput(ConsoleOutputStorage, Path)}</li>
 * <li>Open the storage from a file system directory with {@link #openConsoleOutput(Path)}</li>
 * </ol>
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class ConsoleOutputUtils {

    private ConsoleOutputUtils() {
        // Utility class
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ConsoleOutputUtils.class);

    private static final ColumnStoreFactory STORE_FACTORY = new ArrowColumnStoreFactory();

    private static final ColumnarSchema TABLE_SCHEMA = //
        DefaultColumnarSchema.builder() //
            .addColumn(DataSpec.longSpec()) //
            .addColumn(DataSpec.stringSpec()) //
            .addColumn(DataSpec.booleanSpec()) //
            .build();

    /**
     * Create a new {@link ConsoleOutputConsumer} that accepts {@link ConsoleText} objects and saves them to a columnar
     * table.
     *
     * @return a new {@link ConsoleOutputConsumer}
     * @throws IOException if the creation of a temporary file failed
     */
    public static ConsoleOutputConsumer createConsoleConsumer() throws IOException {
        return new ConsoleOutputConsumer();
    }

    /**
     * Send the console output which was saved to the given storage to the consumer.
     *
     * @param storage the storage that has the console output saved in it
     * @param consumer a consumer that the console output is sent to
     * @throws IOException if reading the console outputs failed
     */
    public static void sendConsoleOutputs(final ConsoleOutputStorage storage, final Consumer<ConsoleText> consumer)
        throws IOException {
        try (final var cursor = ColumnarCursorFactory.create(storage.m_store, storage.m_size)) {
            while (cursor.forward()) {
                final ReadAccessRow access = cursor.access();
                consumer.accept(new ConsoleText( //
                    ((StringReadAccess)access.getAccess(1)).getStringValue(), //
                    ((BooleanReadAccess)access.getAccess(2)).getBooleanValue() //
                ));
            }
        }
    }

    /**
     * Save the console output to the given directory. It can be loaded again with {@link #openConsoleOutput(Path)}.
     * Note that the storage will be closed by this method.
     *
     * @param storage the console output storage
     * @param dir the directory to save to
     * @throws IOException if writing the files failed
     */
    public static void saveConsoleOutput(final ConsoleOutputStorage storage, final Path dir) throws IOException {
        Files.move(storage.m_store.getFileHandle().asPath(), tableFilePath(dir));
        Files.writeString(sizeFilePath(dir), "" + storage.m_size);
        storage.close();
    }

    /**
     * Load the {@link ConsoleOutputStorage} from the given directory.
     *
     * @param dir the directory on which {@link #saveConsoleOutput(ConsoleOutputStorage, Path)} was called before
     * @return the {@link ConsoleOutputStorage} that can be used to send the console output to another consumer
     */
    @SuppressWarnings("resource") // ConsoleOutputStorage is closed by the caller
    public static ConsoleOutputStorage openConsoleOutput(final Path dir) {
        final var sizeFilePath = sizeFilePath(dir);
        final var tableFilePath = tableFilePath(dir);

        // Nothing to load in the directory
        if (!Files.exists(sizeFilePath) || !Files.exists(tableFilePath)) {
            return null;
        }

        try {
            return new ConsoleOutputStorage( //
                STORE_FACTORY.createReadStore(tableFilePath), //
                Long.parseLong(Files.readString(sizeFilePath)) //
            );
        } catch (final IOException e) {
            LOGGER.error("Opening the console output failed.", e);
            return null;
        }
    }

    /**
     * A consumer that remembers a bunch of {@link ConsoleText} objects. Note that {@link #finish()} must be called to
     * release resources.
     */
    public static final class ConsoleOutputConsumer implements Consumer<ConsoleText> {

        private final BatchStore m_store;

        private final WriteCursor<WriteAccessRow> m_cursor;

        private long m_size;

        private ConsoleOutputConsumer() throws IOException {
            final var tmpFile = PathUtils.createTempFile("pyscript_console_output", ".arrow");
            m_store = STORE_FACTORY.createStore(TABLE_SCHEMA, new PathBackedFileHandle(tmpFile));
            m_cursor = ColumnarWriteCursorFactory.createWriteCursor(m_store);
            m_size = 0;
        }

        @Override
        public void accept(final ConsoleText t) {
            m_cursor.forward();
            final var access = m_cursor.access();
            ((LongWriteAccess)access.getWriteAccess(0)).setLongValue(System.currentTimeMillis());
            ((StringWriteAccess)access.getWriteAccess(1)).setStringValue(t.text);
            ((BooleanWriteAccess)access.getWriteAccess(2)).setBooleanValue(t.stderr);
            m_size++;
        }

        /**
         * Finish the consumer and transform it into a storage to read from.
         *
         * @return a {@link ConsoleOutputStorage} that can be used to send the consumed output to another consumer
         * @throws IOException if the collected output cannot be flushed to a temporary file
         */
        ConsoleOutputStorage finish() throws IOException {
            try {
                m_cursor.flush();
                m_cursor.close();
            } catch (final IOException e) {
                LOGGER.error("Failed to finish console output storage. The console output will not be available.", e);
            }

            // NB: Delete the temporary file after closing the storage
            return new ConsoleOutputStorage(m_store, m_size) {

                @Override
                public void close() {
                    super.close();
                    m_store.getFileHandle().delete();
                }
            };
        }
    }

    /** A storage that holds the console output. */
    public static class ConsoleOutputStorage implements AutoCloseable {

        private final BatchReadStore m_store;

        private final long m_size;

        private ConsoleOutputStorage(final BatchReadStore store, final long size) {
            m_store = store;
            m_size = size;
        }

        @Override
        public void close() {
            // NB: Don't delete the file. For the case where the file is temporary this method is overwritten
            try {
                m_store.close();
            } catch (final IOException e) {
                LOGGER.error(e);
            }
        }
    }

    /** @return the path to the console.arrow file */
    private static Path tableFilePath(final Path dir) {
        return dir.resolve("console.arrow");
    }

    /** @return the path to the size.txt file */
    private static Path sizeFilePath(final Path dir) {
        return dir.resolve("size.txt");
    }
}
