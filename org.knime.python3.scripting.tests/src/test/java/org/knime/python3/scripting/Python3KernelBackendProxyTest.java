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
 */

package org.knime.python3.scripting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.util.FileUtil;
import org.knime.python2.kernel.PythonKernelBackendUtils;
import org.knime.python3.DefaultPythonGateway;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowExtension;
import org.knime.python3.testing.Python3ArrowTestUtils;
import org.knime.python3.testing.Python3TestUtils;
import org.knime.python3.views.Python3ViewsSourceDirectory;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class Python3KernelBackendProxyTest {

    // TODO: parts of this class are copied from tests of other python3 plug-ins -- consolidate!

    private static final ColumnarSchema COLUMN_SCHEMA = ColumnarSchema.of(STRING, DOUBLE, INT, LONG, STRING);

    private static final String[] COLUMN_NAMES =
        new String[]{"My double col", "My int col", "My long col", "My string col"};

    private static final int NUM_BATCHES = 3;

    private static final int NUM_ROWS_PER_BATCH = 20;

    private BufferAllocator m_allocator;

    private ArrowColumnStoreFactory m_storeFactory;

    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, 0, m_allocator.getLimit(),
            ArrowCompressionUtil.ARROW_NO_COMPRESSION);
    }

    @After
    public void after() {
        m_allocator.close();
    }

    @Test
    public void testPutTableIntoWorkspace() throws IOException, InterruptedException {
        try (final PythonGateway<Python3KernelBackendProxyTestRunner> gateway = openPythonGateway();
                final ArrowBatchReadStore store = createTestStore()) {
            final PythonArrowDataSource dataSource = PythonArrowDataUtils.createSource(store, COLUMN_NAMES);
            final PythonTestResult testResult = gateway.getEntryPoint().testPutTableIntoWorkspace(dataSource);
            if (!testResult.wasSuccessful()) {
                throw new AssertionError(testResult.getFailureReport());
            }
        }
    }

    @Test
    public void testGetImageFromWorkspace() throws IOException, InterruptedException {
        testGetImageFromWorkspace("png");
        testGetImageFromWorkspace("svg");
    }

    public void testGetImageFromWorkspace(final String type) throws IOException, InterruptedException {
        try (final PythonGateway<Python3KernelBackendProxyTestRunner> gateway = openPythonGateway()) {
            var tempDir = FileUtil.createTempDir("images");
            var imgPath = tempDir.toPath().resolve("image");
            gateway.getEntryPoint().testWriteImageToPath(type, imgPath.toAbsolutePath().toString());
            var imgContainer = PythonKernelBackendUtils.createImage(() -> Files.newInputStream(imgPath));
            if (type.equals("svg")) {
                assertTrue(imgContainer.hasSvgDocument());
            }
            var img = imgContainer.getBufferedImage();
            assertTrue(img.getHeight() > 0);
        }
    }

    @Test
    public void testGetObjectType() throws Exception {
        performEntryPointTest(e -> {
            e.putStringIntoOutputObject(0, "foobar");
            var kernelProxy = e.getKernel();
            assertEquals("str", kernelProxy.getOutputObjectType(0));
        });
    }

    @Test
    public void testGetObjectRepresentation() throws Exception {
        performEntryPointTest(e -> {
            e.putStringIntoOutputObject(0, "foobar");
            var kernelProxy = e.getKernel();
            assertEquals("foobar", kernelProxy.getOutputObjectStringRepresentation(0));
            var veryLongString = Stream.generate(() -> "foobar").limit(500).collect(Collectors.joining());
            e.putStringIntoOutputObject(1, veryLongString);
            var expected = veryLongString.subSequence(0, 996) + "\n...";
            assertEquals(expected, kernelProxy.getOutputObjectStringRepresentation(1));
        });
    }

    @Test
    public void testPutAndGetObject() throws Exception {
        final var file = Files.createTempFile("pickled", "object");
        final var path = file.toAbsolutePath().toString();
        performEntryPointTest(e -> {
            e.putStringIntoOutputObject(0, "foobar");
            e.getKernel().getOutputObject(0, path);
        });
        assertNotEquals(0, Files.size(file));
        performEntryPointTest(e -> {
            var kernel = e.getKernel();
            kernel.setInputObject(1, path);
            assertEquals("foobar", e.getStringFromInputObject(1));
        });
    }

    private static void performEntryPointTest(final Consumer<Python3KernelBackendProxyTestRunner> test) throws IOException, InterruptedException {
        try (var gateway = openPythonGateway()) {
            test.accept(gateway.getEntryPoint());
        }
    }

    private static <E extends PythonEntryPoint> PythonGateway<E> openPythonGateway(final Class<E> entryPointClass)
        throws IOException, InterruptedException {
        final var command = Python3TestUtils.getPythonCommand();
        final var launcherPath =
            Paths.get(System.getProperty("user.dir"), "src/test/python", "knime_kernel_test.py").toString();
        final List<PythonExtension> extensions = Collections.singletonList(PythonArrowExtension.INSTANCE);
        final var pythonPath = (new PythonPathBuilder()) //
            .add(Python3SourceDirectory.getPath()) //
            .add(Python3ArrowSourceDirectory.getPath()) //
            .add(Python3ScriptingSourceDirectory.getPath()) //
            .add(Python3ViewsSourceDirectory.getPath()) //
            .build();
        return DefaultPythonGateway.create(command.createProcessBuilder(), launcherPath, entryPointClass, extensions,
            pythonPath);
    }

    private static PythonGateway<Python3KernelBackendProxyTestRunner> openPythonGateway() throws IOException, InterruptedException {
        return openPythonGateway(Python3KernelBackendProxyTestRunner.class);
    }

    private ArrowBatchReadStore createTestStore() throws IOException {
        final var storePath = Python3ArrowTestUtils.createTmpKNIMEArrowFileHandle();
        try (final ArrowBatchStore store = m_storeFactory.createStore(COLUMN_SCHEMA, storePath)) {
            try (final BatchWriter writer = store.getWriter()) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final WriteBatch batch = writer.create(NUM_ROWS_PER_BATCH);
                    fillBatch(b, batch);
                    final ReadBatch readBatch = batch.close(NUM_ROWS_PER_BATCH);
                    writer.write(readBatch);
                    readBatch.release();
                }
            }
        }
        return m_storeFactory.createReadStore(storePath.asPath());
    }

    private static void fillBatch(final int b, final WriteBatch batch) {
        final StringWriteData rowKey = (StringWriteData)batch.get(0);
        final DoubleWriteData doubleCol = (DoubleWriteData)batch.get(1);
        final IntWriteData intCol = (IntWriteData)batch.get(2);
        final LongWriteData longCol = (LongWriteData)batch.get(3);
        final StringWriteData stringCol = (StringWriteData)batch.get(4);
        for (int r = 0; r < NUM_ROWS_PER_BATCH; r++) {
            final int totalRowIndex = b * NUM_ROWS_PER_BATCH + r;
            rowKey.setString(r, "Row" + totalRowIndex);
            if (totalRowIndex % 13 == 0) {
                doubleCol.setMissing(r);
                intCol.setMissing(r);
                longCol.setMissing(r);
                stringCol.setMissing(r);
            } else {
                doubleCol.setDouble(r, totalRowIndex);
                intCol.setInt(r, totalRowIndex * 2);
                longCol.setLong(r, totalRowIndex * 10l);
                stringCol.setString(r, "This is row " + totalRowIndex);
            }
        }
    }

    public interface Python3KernelBackendProxyTestRunner extends PythonEntryPoint {

        PythonTestResult testPutTableIntoWorkspace(PythonDataSource tableDataSource);

        PythonTestResult testWriteImageToPath(final String imgType, final String path);

        Python3KernelBackendProxy getKernel();

        void putStringIntoOutputObject(final int index, final String testString);

        String getStringFromInputObject(final int index);
    }
}
