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
package org.knime.python3.scripting.nodes2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

import org.knime.core.columnar.arrow.PathBackedFileHandle;
import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.util.PathUtils;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.scripting.nodes2.ConsoleOutputUtils.ConsoleOutputStorage;
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
@SuppressWarnings("javadoc") // Suppress warnings about protected visibility in javadoc
final class ConsoleOutputUtils {

    /**
     * The maximum number of rows that we write into one table. A value of 5000 is a good compromise between a long
     * Scrollback and small file sizes.
     */
    static final int MAX_ROWS_PER_TABLE = 5000;

    private ConsoleOutputUtils() {
        // Utility class
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ConsoleOutputUtils.class);

    private static final ColumnStoreFactory STORE_FACTORY = PythonArrowDataUtils.getArrowColumnStoreFactory();

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
     * Load the {@link ConsoleOutputStorage} from the given directory.
     *
     * @param dir the directory on which {@link #saveConsoleOutput(ConsoleOutputStorage, Path)} was called before
     * @return the {@link ConsoleOutputStorage} that can be used to send the console output to another consumer
     */
    public static ConsoleOutputStorage openConsoleOutput(final Path dir) {
        return ConsoleOutputStorage.loadFrom(dir);
    }

    /**
     * A consumer of {@link ConsoleText} that writes to two Arrow Tables in a Ring-Buffer fashion. To release resources
     * the caller must call {@link ConsoleOutputConsumer#finish()} and close the storage.
     */
    public static final class ConsoleOutputConsumer implements Consumer<ConsoleText> {

        private Table m_extraTable;

        private Table m_currentTable;

        public ConsoleOutputConsumer() throws IOException {
            m_currentTable = new Table();
        }

        @Override
        public void accept(final ConsoleText t) {
            try {
                if (m_currentTable.size() >= MAX_ROWS_PER_TABLE) {
                    // Switch to the next table
                    m_currentTable.finish();
                    if (m_extraTable != null) {
                        // NB: Close releases all resources and deletes the file
                        m_extraTable.close();
                    }
                    m_extraTable = m_currentTable;
                    m_currentTable = new Table();

                }
                m_currentTable.write(t);
            } catch (IOException ex) {
                LOGGER.error("Switching to the next table for storing console outputs failed.", ex);
            }
        }

        /**
         * Finish writing to the consumer. Creates a {@link ConsoleOutputStorage} that can be used to save the console
         * logs to disc and retrieve them to a new consumer.
         *
         * @return the {@link ConsoleOutputStorage}
         * @throws IOException if flushing the file failed
         */
        public ConsoleOutputStorage finish() throws IOException {
            // NB: The extra table is either null or already finished
            m_currentTable.finish();
            // NB: The tables are saved in temporary files which are deleted when closing the table
            // ConsoleOutputStorage#saveTo can be used to persist the data
            return new ConsoleOutputStorage(m_extraTable, m_currentTable);
        }
    }

    /** Stores console output in two columnar tables. */
    public static final class ConsoleOutputStorage implements AutoCloseable {

        private final Table m_tableA;

        private final Table m_tableB;

        @SuppressWarnings("resource") // Tables will be closed by ConsoleOutputStorage#close
        private static ConsoleOutputStorage loadFrom(final Path dir) {
            var tableA = Table.loadFrom(dir, "a");
            var tableB = Table.loadFrom(dir, "b");
            if (tableA == null && tableB == null) {
                return null;
            } else {
                return new ConsoleOutputStorage(tableA, tableB);
            }
        }

        private ConsoleOutputStorage(final Table tableA, final Table tableB) {
            m_tableA = tableA;
            m_tableB = tableB;
        }

        /**
         * Send the saved console output to the consumer.
         *
         * @param consumer a consumer that the console output is sent to
         * @throws IOException if reading the console outputs failed
         */
        public void sendConsoleOutputs(final Consumer<ConsoleText> consumer) throws IOException {
            if (m_tableA != null) {
                m_tableA.readAll(consumer);
            }
            if (m_tableB != null) {
                m_tableB.readAll(consumer);
            }
        }

        public void saveTo(final Path dir) throws IOException {
            if (m_tableA != null) {
                m_tableA.saveTo(dir, "a");
            }
            if (m_tableB != null) {
                m_tableB.saveTo(dir, "b");
            }
        }

        @Override
        public void close() {
            if (m_tableA != null) {
                try {
                    m_tableA.close();
                } catch (IOException ex) {
                    LOGGER.warn("Failed to close console output storage.", ex);
                }
            }
            if (m_tableB != null) {
                try {
                    m_tableB.close();
                } catch (IOException ex) {
                    LOGGER.warn("Failed to close console output storage.", ex);
                }
            }
        }
    }

    /** Internal data structure combining a batch store and a size. Used for writing to and reading from. */
    private static final class Table implements AutoCloseable {

        /** load table from an arrow and size file, returns <code>null</code> if the table could not be load */
        @SuppressWarnings("resource") // Store will be closed by Table#close
        static Table loadFrom(final Path dir, final String suffix) {
            final var sizeFilePath = sizeFilePath(dir, suffix);
            final var tableFilePath = tableFilePath(dir, suffix);

            // Nothing to load in the directory
            if (!Files.exists(sizeFilePath) || !Files.exists(tableFilePath)) {
                return null;
            }

            try {
                return new Table( //
                    STORE_FACTORY.createReadStore(tableFilePath), //
                    Integer.parseInt(Files.readString(sizeFilePath)) //
                );
            } catch (IOException ex) {
                LOGGER.error("Opening the console output failed.", ex);
                return null;
            }
        }

        private final boolean m_isTmpFile;

        private final BatchReadStore m_store;

        private ColumnarWriteCursor m_writeCursor;

        private int m_size;

        Table() throws IOException {
            var tmpFile = PathUtils.createTempFile("pyscript_console_output", ".arrow");
            var writeStore = STORE_FACTORY.createStore(TABLE_SCHEMA, new PathBackedFileHandle(tmpFile));
            m_writeCursor = ColumnarWriteCursorFactory.createWriteCursor(writeStore);
            m_store = writeStore;
            m_size = 0;
            m_isTmpFile = true;
        }

        private Table(final BatchReadStore store, final int size) {
            m_store = store;
            m_size = size;
            m_writeCursor = null;
            m_isTmpFile = false;
        }

        int size() {
            return m_size;
        }

        @SuppressWarnings("resource") // We do not want to close the write cursor
        void write(final ConsoleText text) throws IOException {
            Objects.requireNonNull(m_writeCursor, "Writing to a read-only store. This is an implementation error.");
            final var access = m_writeCursor.access();
            ((LongWriteAccess)access.getWriteAccess(0)).setLongValue(System.currentTimeMillis());
            ((StringWriteAccess)access.getWriteAccess(1)).setStringValue(text.text);
            ((BooleanWriteAccess)access.getWriteAccess(2)).setBooleanValue(text.stderr);
            m_writeCursor.commit();
            m_size++;
        }

        void finish() throws IOException {
            if (m_writeCursor != null) {
                m_writeCursor.finish();
                m_writeCursor.close();
                m_writeCursor = null;
            }
        }

        void saveTo(final Path dir, final String suffix) throws IOException {
            Files.move(m_store.getFileHandle().asPath(), tableFilePath(dir, suffix));
            Files.writeString(sizeFilePath(dir, suffix), "" + m_size);
        }

        void readAll(final Consumer<ConsoleText> consumer) throws IOException {
            try (final var cursor = ColumnarCursorFactory.create(m_store, m_size)) {
                while (cursor.forward()) {
                    final ReadAccessRow access = cursor.access();
                    consumer.accept(new ConsoleText( //
                        ((StringReadAccess)access.getAccess(1)).getStringValue(), //
                        ((BooleanReadAccess)access.getAccess(2)).getBooleanValue() //
                    ));
                }
            }
        }

        @Override
        public void close() throws IOException {
            try {
                m_store.close();
            } finally {
                if (m_isTmpFile) {
                    // We only delete the file if it was a temporary file that we created ourselves
                    m_store.getFileHandle().delete();
                }
            }
        }

        /** @return the path to the console.arrow file */
        private static Path tableFilePath(final Path dir, final String suffix) {
            return dir.resolve("console_" + suffix + ".arrow");
        }

        /** @return the path to the console_size.txt file */
        private static Path sizeFilePath(final Path dir, final String suffix) {
            return dir.resolve("console_size_" + suffix + ".txt");
        }
    }
}
