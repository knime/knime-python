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
 *   Jun 1, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.python3.PythonPath.PythonPathBuilder;

import py4j.Py4JException;

/**
 * Contains unit tests for the {@link DefaultPythonGateway}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DefaultPythonGatewayTest {

    private static final String PYTHON_EXE_ENV = "PYTHON3_EXEC_PATH";

    /**
     * We observed that Python startup sometimes gets stuck if there is a syntax error in the launch script.
     *
     * @throws InterruptedException
     * @throws IOException
     *
     */
    @Test(expected = ConnectException.class)
    public void testSyntaxErrorInLauncher() throws IOException, InterruptedException {
        try (var gateway = createGateway(DummyEntryPoint.class, "broken_launcher.py")) {
            // do nothing, we are only interested in the startup
        }
    }

    @Test(expected = Py4JException.class)
    public void testSyntaxErrorInImportedExtension() throws IOException, InterruptedException {
        try (var gateway = createGateway(DummyEntryPoint.class, "dummy_launcher.py", extension("broken_extension"))) {
            // just tests startup
        }
    }

    private static PythonExtension extension(final String moduleName) {
        return () -> moduleName;
    }

    /**
     * The {@link Process#getInputStream()} and {@link Process#getErrorStream()} must be consumed otherwise the process
     * may get stuck because it can't write to the output anymore.
     *
     * @throws Exception
     */
    @Test
    public void testChattyLauncher() throws Exception {
        try (var gateway = createGateway(DummyEntryPoint.class, "chatty_launcher.py")) {
        }
    }

    @Test
    public void testSequentialUseOfGateway() throws Exception {
        try (var gateway = createGateway(PrintingEntryPoint.class, "printing_launcher.py")) {
            gateway.getEntryPoint().print("first");
            try (var firstOutput = getOutputReader(gateway)) {
                assertEquals("first", firstOutput.readLine());
            }
            gateway.getEntryPoint().print("second");
            try (var secondOutput = getOutputReader(gateway)) {
                assertEquals("second", secondOutput.readLine());
                gateway.getEntryPoint().print("third");
                assertEquals("third", secondOutput.readLine());
            }
        }
    }

    @Test
    public void testSilentGateway() throws Exception {
        try (var gateway = createGateway(PrintingEntryPoint.class, "printing_launcher.py");
                var output = gateway.getStandardOutputStream()) {
            // just check if we can get the output even though the gateway didn't print anything
        }
    }

    @Test
    public void testInteractingWithGateway() throws Exception {
        var sb = new StringBuilder();
        var executor = Executors.newSingleThreadExecutor();
        try (var gateway = createGateway(PrintingEntryPoint.class, "printing_launcher.py")) {
            gateway.getEntryPoint().print("foo");
            try (var stream = gateway.getStandardOutputStream();
                    var redirector = new AsyncLineRedirector(executor::submit, stream, sb::append, 100)) {
                gateway.getEntryPoint().print("bar");
                gateway.getEntryPoint().print("la");
            }
        }
        assertEquals("foobarla", sb.toString());
    }

    @Test
    public void testConcurrentGatewayCreation() throws Exception {
        var creationStartLatch = new CountDownLatch(1);
        var fail = new AtomicBoolean(false);
        var threads = Stream.generate(() -> new Thread(() -> {
            try {
                creationStartLatch.await();
                try (var gateway = createGateway(PrintingEntryPoint.class, "printing_launcher.py")) {
                    gateway.getEntryPoint().print("foobar");
                }
            } catch (IOException | InterruptedException e) {
                fail.set(true);
            }
        })).limit(10).collect(toList());
        threads.forEach(Thread::start);
        creationStartLatch.countDown();
        for (var thread : threads) {
            thread.join();
        }
        if (fail.get()) {
            fail("One thread failed with an exception.");
        }
    }


    private static BufferedReader getOutputReader(final PythonGateway<?> gateway) {
        return new BufferedReader(new InputStreamReader(gateway.getStandardOutputStream())); //NOSONAR just for testing
    }

    private static <T extends PythonEntryPoint> DefaultPythonGateway<T> createGateway(final Class<T> entryPointClass,
        final String launcherFile, final PythonExtension... extensions) throws IOException, InterruptedException {
        var launcherPath = PythonSourceDirectoryLocator.getPathFor(DefaultPythonGatewayTest.class, "src/test/python")//
            .resolve(launcherFile)//
            .toString();
        final var command = getPythonCommand();
        final PythonPathBuilder builder = PythonPath.builder()//
            .add(Python3SourceDirectory.getPath());
        final var pythonPath = builder.build();
        return new DefaultPythonGateway<>(command.createProcessBuilder(), launcherPath, entryPointClass,
            List.of(extensions), pythonPath);
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

    public interface DummyEntryPoint extends PythonEntryPoint {

    }

    public interface FailingEntryPoint extends PythonEntryPoint {

    }

    public interface PrintingEntryPoint extends PythonEntryPoint {
        void print(String msg);
    }
}
