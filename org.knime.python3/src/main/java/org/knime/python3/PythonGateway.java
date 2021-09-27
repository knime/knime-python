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
 *   May 3, 2021 (benjamin): created
 */
package org.knime.python3;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SystemUtils;

import py4j.ClientServer;
import py4j.ClientServer.ClientServerBuilder;
import py4j.GatewayServer;
import py4j.Py4JException;

/**
 * A gateway to a Python process. Starts a Python process upon construction of an instance and destroys it when
 * {@link #close() closing} the instance. Python functionality can be accessed via a proxy {@link #getEntryPoint()}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @param <T> the class of the entry point
 */
public class PythonGateway<T extends PythonEntryPoint> implements AutoCloseable {

    private final Process m_process;

    private ClientServer m_clientServer;

    private final T m_entryPoint;

    /**
     * Create a {@link PythonGateway} to a new Python process.
     *
     * @param command the command for the Python executable
     * @param launcherPath the Python script to run TODO what should the script do?
     * @param entryPointClass the class of the {@link PythonEntryPoint}
     * @param extensions a collection of extensions which should be imported after the Python process has started
     * @param pythonPath the {@link PythonPath} which defines additional folders from which Python modules can be
     *            imported
     * @throws IOException TODO
     */
    public PythonGateway(final PythonCommand command, final String launcherPath, final Class<T> entryPointClass,
        final Collection<PythonExtension> extensions, final PythonPath pythonPath) throws IOException {

        // Setup Java server
        m_clientServer = new ClientServerBuilder().javaPort(0).build();
        final int javaPort = m_clientServer.getJavaServer().getListeningPort();
        System.out.println("Java listening at: " + javaPort + ".");

        //        GatewayServer.turnAllLoggingOn();
        GatewayServer.turnLoggingOff();

        // Create the process
        final ProcessBuilder pb = command.createProcessBuilder();
        Collections.addAll(pb.command(), "-u", launcherPath, Integer.toString(javaPort));
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        // Set the PYTHONPATH variable to be able to import the Python modules
        pb.environment().put("PYTHONPATH", pythonPath.getPythonPath());

        // Start the Python process and connect to it
        m_process = pb.start();
        // Start listening to stdout and stderror pipes.
        try {
            m_entryPoint = (T)m_clientServer.getPythonServerEntryPoint(new Class[]{entryPointClass});
            waitForConnection(m_entryPoint);
            // Register extensions
            m_entryPoint
                .registerExtensions(extensions.stream().map(e -> e.getPythonModule()).collect(Collectors.toList()));
        } catch (final Throwable th) {
            // TODO: reenable non-blocking
//            final BufferedReader reader = new BufferedReader(new InputStreamReader(m_process.getInputStream()));
//            String message;
//            try {
//                while ((message = reader.readLine()) != null && !Thread.interrupted()) {
//                    System.out.println(message);
//                }
//            } catch (final IOException ignore) {}
//            final BufferedReader reader2 = new BufferedReader(new InputStreamReader(m_process.getErrorStream()));
//            String message2;
//            try {
//                while ((message2 = reader2.readLine()) != null && !Thread.interrupted()) {
//                    System.err.println(message2);
//                }
//            } catch (final IOException ignore) {}
            try {
                close();
            } catch (final Exception ex) {
                th.addSuppressed(ex);
            }
            throw th;
        }
    }

    public InputStream getStandardOutputStream() {
        return m_process.getInputStream();
    }

    public InputStream getStandardErrorStream() {
        return m_process.getErrorStream();
    }

    /**
     * @return the entry point into Python. Calling methods on this object will call Python functions.
     */
    public T getEntryPoint() {
        return m_entryPoint;
    }

    @Override
    public final void close() throws Exception {
        // Mostly copied from PythonKernel.
        // If the original process was a script, we have to kill the actual Python
        // process by PID (see below).
        Integer pid = null;
        try {
            pid = m_entryPoint.getPid();
        } catch (final Exception ex) { // NOSONAR: optional step, catch all types of exceptions and continue.
            System.out.println(ex); // TODO: proper logging
        }

        if (m_clientServer != null) {
            m_clientServer.shutdown();
            // TODO: May require further cleanup. See:
            // https://www.py4j.org/advanced_topics.html#py4j-memory-model
            m_clientServer = null;
        }

        if (m_process != null) {
            m_process.destroy();
            m_process.destroyForcibly();
        }

        if (pid != null) {
            try {
                ProcessBuilder pb;
                if (SystemUtils.IS_OS_WINDOWS) {
                    pb = new ProcessBuilder("taskkill", "/F", "/PID", "" + pid);
                } else {
                    pb = new ProcessBuilder("kill", "-KILL", "" + pid);
                }
                final Process p = pb.start();
                p.waitFor();
            } catch (final InterruptedException ex) {
                // Closing the kernel should not be interrupted.
                Thread.currentThread().interrupt();
            } catch (final Exception ignore) {
                // Ignore.
            }
        }
    }

    // TODO: there must be a better way... Check if py4j sends us some kind of signal for which we can listen/block
    private static void waitForConnection(final PythonEntryPoint entryPoint) throws ConnectException {
        boolean connected = false;
        int numAttempts = 0;
        final int numMaxAttempts = 1000;
        final long tic = System.nanoTime();
        while (!connected) {
            if (numAttempts < numMaxAttempts) {
                try {
                    final int pid = entryPoint.getPid(); // Fails if not yet connected.
                    final long time = Duration.ofNanos(System.nanoTime() - tic).toMillis();
                    System.out.println("Connected to Python process with PID: " + pid + ", after #attempts: " +
                        numAttempts + ". Took ms: " + time + ".");
                    connected = true;
                } catch (final Py4JException ex) {
                    if (!(ex.getCause() instanceof ConnectException)) {
                        throw ex;
                    }
                    numAttempts++;
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException ex1) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } else {
                break;
            }
        }
        if (!connected) {
            // TODO
            throw new ConnectException("Could not connect");
        }
    }
}
