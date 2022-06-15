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
 *   Apr 12, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.knime.core.columnar.store.FileHandle;
import org.knime.python3.DefaultPythonGateway;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonDataSink;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonException;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.SimplePythonCommand;

/**
 * Utilities for Python Arrow data transfer tests.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class TestUtils {

    private static final String PYTHON_EXE_ENV = "PYTHON3_EXEC_PATH";

    private TestUtils() {
        // Static utility class
    }

    /** Create a Python command from the path in the env var PYTHON3_EXEC_PATH */
    private static PythonCommand getPythonCommand() throws IOException {
        final String python3path = System.getenv(PYTHON_EXE_ENV);
        if (python3path != null) {
            return new SimplePythonCommand(python3path);
        }
        throw new IOException(
            "Please set the environment variable '" + PYTHON_EXE_ENV + "' to the path of the Python 3 executable.");
    }

    /**
     * Create a temporary file which is deleted on exit.
     *
     * @return the file
     * @throws IOException if the file could not be created
     */
    public static Path createTmpKNIMEArrowPath() throws IOException {
        final Path path = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow");
        path.toFile().deleteOnExit();
        return path;
    }

    /**
     * Create FileHandle that is backed by a temporary file which is deleted on exit.
     *
     * @return the FileHandle
     * @throws IOException if the temporary file could not be created
     */
    public static FileHandle createTmpKNIMEArrowFileHandle() throws IOException {
        final var path = createTmpKNIMEArrowPath();
        return new FileHandle() {

            @Override
            public void delete() {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public Path asPath() {
                return path;
            }

            @Override
            public File asFile() {
                return path.toFile();
            }

        };
    }

    /**
     * @return a new {@link PythonGateway} for running tests.
     * @throws IOException
     * @throws InterruptedException
     */
    public static PythonGateway<ArrowTestsEntryPoint> openPythonGateway() throws IOException, InterruptedException {
        final PythonCommand command = getPythonCommand();
        final String launcherPath =
            Paths.get(System.getProperty("user.dir"), "src/test/python", "tests_launcher.py").toString();
        final PythonPath pythonPath = (new PythonPathBuilder()) //
            .add(Python3SourceDirectory.getPath()) //
            .add(Python3ArrowSourceDirectory.getPath()) //
            .build();
        final List<PythonExtension> extensions = Collections.singletonList(PythonArrowExtension.INSTANCE);

        return DefaultPythonGateway.create(command.createProcessBuilder(), launcherPath, ArrowTestsEntryPoint.class,
            extensions, pythonPath);
    }

    /**
     * Interface for a {@link SinkCreator} that can create new {@link PythonDataSink}s
     */
    public interface SinkCreator {
        /**
         * @return a new data sink
         * **/
        PythonDataSink createSink();
    }

    /**
     * {@link PythonEntryPoint} for the tests. This interface is implemented on Python and calling a method will execute
     * Python code.
     */
    public interface ArrowTestsEntryPoint extends PythonEntryPoint {

        /**
         * Assert that the data is the expected data for the given type.
         *
         * @param dataType the expected type
         * @param dataSource the data
         */
        void testTypeToPython(String dataType, PythonDataSource dataSource);

        /**
         * Create data for the given type and write it to the data sink.
         *
         * @param dataType the type to write
         * @param dataSink the data sink to write to
         */
        void testTypeFromPython(String dataType, PythonDataSink dataSink);

        /**
         * Create data with the schema 0: int, 1: string, 2: struct<list<int>, float> and write it to the sink.
         *
         * @param dataSink the data sink to write to
         */
        void testExpectedSchema(PythonDataSink dataSink);

        /**
         * Assert that the list of data sources is as expected and write to a list of data sinks.
         *
         * @param dataSources sources of data
         * @param dataSinks sinks to write to
         */
        void testMultipleInputsOutputs(List<? extends PythonDataSource> dataSources,
            List<? extends PythonDataSink> dataSinks);

        /**
         * Write row keys and one other column to the data sink for checking the row keys.
         *
         * @param duplicates "none" if no duplicates should be used, "far" if duplicates should be far away, "close" if
         *            the duplicates should be next to each other
         * @param dataSink sinks to write to
         * @throws PythonException if writing to the dataSink caused an exception because the data sink contains
         *             duplicate keys
         */
        void testRowKeyChecking(String duplicates, PythonDataSink dataSink) throws PythonException;

        /**
         * Write several batches of values into the sink such that we can check that the domain is calculated
         * appropriately.
         *
         * @param scenario The test case scenario, one of "double", "int", "string"
         * @param sink to write to
         */
        void testDomainCalculation(String scenario, PythonDataSink sink);

        /**
         * Test the Python KNIME Table API
         * @param source providing data to Python
         * @param sinkCreator A sink supplier that is called whenever a new data sink is created on the python side
         * @param numRows number of rows in the input table
         * @param numColumns number of columns in the input table
         * @param mode Of copying data from source to sink, "arrow" or "pandas"
         */
        PythonDataSink testKnimeTable(PythonDataSource source, SinkCreator sinkCreator, long numRows, long numColumns, String mode);
    }
}
