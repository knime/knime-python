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
import java.io.StringWriter;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.knime.core.node.NodeLogger;

import py4j.ClientServer;
import py4j.ClientServer.ClientServerBuilder;
import py4j.Py4JException;

/**
 * Gateway to a Python process. Starts a Python process upon construction of an instance and destroys it when
 * {@link #close() closing} the instance. Python functionality can be accessed via a {@link #getEntryPoint() proxy}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @param <T> the class of the proxy
 */
public final class PythonGateway<T extends PythonEntryPoint> implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonGateway.class);

    /**
     * Copied from {@code PythonKernel2KernelBackend}.
     */
    private static final String CONNECT_TIMEOUT_VM_OPT = "knime.python.connecttimeout";

    /**
     * Set to {@code null} in {@link #close()} just to make sure that any Java instances that have been referenced by
     * the Python side (and therefore by py4j on the Java side) can be garbage collected in a timely manner.
     */
    private /* final */ ClientServer m_clientServer;

    private final Process m_process;

    /**
     * See {@link #m_clientServer}. Set to {@code null} in {@link #close()} to make sure any p4j-related instances can
     * be garbage collected in a timely manner.
     */
    private /* final */ T m_entryPoint;

    /**
     * Python could be started through a start script. When closing the script's process, the actual Python process may
     * not be closed along with it. We have to retrieve the Python process's PID through py4j. We do this as early as
     * possible and remember the PID to make sure we can kill the Python process in {@link #close()} even if it's in an
     * inconsistent state by then where retrieving the PID via py4j would not be possible anymore.
     * <P>
     * Note that this <em>must</em> be a nullable integer to make sure we do not accidentally call (task)kill without a
     * valid PID. Using a primitive integer with its default value of zero would lead to unwanted results, e.g. on
     * Linux, {@code kill -KILL 0} would kill all processes of the current process group!
     */
    private final Integer m_pid;

    /**
     * Creates a {@link PythonGateway} to a new Python process.
     *
     * @param pythonProcessBuilder the builder used to configure and start the Python process
     * @param launcherPath the Python script that bootstraps the py4j-based communication on the Python side (via
     *            {@code knime_gateway.connect_to_knime})
     * @param entryPointClass the class of the {@link PythonEntryPoint proxy}
     * @param extensions a collection of extensions which should be imported after the Python process has started
     * @param pythonPath the {@link PythonPath} which defines additional folders from which Python modules can be
     *            imported
     * @throws IOException If creating the Python process or establishing the connection to it failed.
     */
    public PythonGateway(final ProcessBuilder pythonProcessBuilder, final String launcherPath,
        final Class<T> entryPointClass, final Collection<PythonExtension> extensions, final PythonPath pythonPath)
        throws IOException {
        try {
            m_clientServer = new ClientServerBuilder().javaPort(0).build();
            final int javaPort = m_clientServer.getJavaServer().getListeningPort();

            final var pb = pythonProcessBuilder;
            Collections.addAll(pb.command(), "-u", launcherPath, Integer.toString(javaPort));
            pb.environment().put("PYTHONPATH", pythonPath.getPythonPath());
            m_process = pb.start();

            @SuppressWarnings("unchecked")
            final var casted = (T)m_clientServer.getPythonServerEntryPoint(new Class[]{entryPointClass});
            m_entryPoint = casted;
            m_pid = waitForConnection(m_entryPoint, m_process);

            m_entryPoint.registerExtensions(extensions.stream() //
                .map(PythonExtension::getPythonModule) //
                .collect(Collectors.toList()));
        } catch (final Throwable th) { // NOSONAR We cannot risk leaking the Python process.
            try {
                logPostMortem();
                close();
            } catch (final Exception ex) { // NOSONAR We want to propagate the original error.
                th.addSuppressed(ex);
            }
            throw th;
        }
    }

    // TODO: there must be some better way than a busy wait. Check if py4j sends us some kind of signal once it's ready.
    private static int waitForConnection(final PythonEntryPoint entryPoint, final Process process)
        throws ConnectException {
        final long timeout = getConnectionTimeoutInMillis();
        final long start = System.currentTimeMillis();
        do {
            try {
                final int pid = entryPoint.getPid(); // Fails if not yet connected.
                LOGGER.debug("Connected to Python process with PID: " + pid + " after ms: "
                    + (System.currentTimeMillis() - start)); // TODO: remove once in production!
                return pid;
            } catch (final Py4JException ex) {
                if (!(ex.getCause() instanceof ConnectException)) {
                    throw ex;
                }
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException ex1) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } while (process.isAlive() && (System.currentTimeMillis() - start) <= timeout);
        throw new ConnectException("Could not connect to the Python process.");
    }

    /**
     * Copied from {@code PythonKernel2KernelBackend}.
     */
    private static int getConnectionTimeoutInMillis() {
        final var defaultTimeout = "30000";
        try {
            final String timeout = System.getProperty(CONNECT_TIMEOUT_VM_OPT, defaultTimeout);
            return Integer.parseInt(timeout);
        } catch (final NumberFormatException ex) {
            LOGGER.warn("The VM option -D" + CONNECT_TIMEOUT_VM_OPT
                + " was set to a non-integer value. This is invalid. The timeout therefore defaults to "
                + defaultTimeout + " ms.");
            return Integer.parseInt(defaultTimeout);
        }
    }

    @SuppressWarnings("resource") // Streams will be closed or have been closed along with process.
    private void logPostMortem() throws IOException {
        if (m_process != null) {
            final var stdoutWriter = new StringWriter();
            IOUtils.copy(m_process.getInputStream(), stdoutWriter, StandardCharsets.UTF_8);
            final var stderrWriter = new StringWriter();
            IOUtils.copy(m_process.getErrorStream(), stderrWriter, StandardCharsets.UTF_8);
            final String stdout = stdoutWriter.toString();
            if (!stdout.isBlank()) {
                LOGGER.info(stdout);
            }
            final String stderr = stderrWriter.toString();
            if (!stderr.isBlank()) {
                LOGGER.error(stderr);
            }
        }
    }

    /**
     * @return The Python process's {@code stdout}.
     */
    public InputStream getStandardOutputStream() {
        return m_process.getInputStream();
    }

    /**
     * @return The Python process's {@code stderr}.
     */
    public InputStream getStandardErrorStream() {
        return m_process.getErrorStream();
    }

    /**
     * @return The entry point into Python. Calling methods on this object will call Python functions.
     */
    public T getEntryPoint() {
        return m_entryPoint;
    }

    @Override
    public void close() throws IOException {
        if (m_clientServer != null) {
            m_entryPoint = null;
            m_clientServer.shutdown();
            m_clientServer = null;
        }
        if (m_pid != null) {
            try {
                ProcessBuilder pb;
                if (SystemUtils.IS_OS_WINDOWS) {
                    pb = new ProcessBuilder("taskkill", "/F", "/PID", "" + m_pid);
                } else {
                    pb = new ProcessBuilder("kill", "-KILL", "" + m_pid);
                }
                final Process p = pb.start();
                p.waitFor();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (final IOException ex) {
                LOGGER.debug(ex);
            }
        }
        if (m_process != null) {
            m_process.destroy();
            m_process.destroyForcibly();
        }
    }
}
