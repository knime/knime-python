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
import java.net.ConnectException;
import java.time.Duration;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SystemUtils;

import py4j.ClientServer;
import py4j.Py4JException;

/**
 * A gateway to a Python process. Starts a Python process when created and kills it when closed. Python functionallity
 * can be accessed via {@link #getEntryPoint()}.
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
        // Create the process
        final ProcessBuilder pb = command.createProcessBuilder();
        pb.command().add("-u"); // TODO needed? Use unbuffered stdout, stderr
        pb.command().add(launcherPath); // Path to the python script
        pb.inheritIO(); // TODO we should handle stdin and stdout manually

        // Set the PYTHONPATH variable to be able to import the Python modules
        pb.environment().put("PYTHONPATH", pythonPath.getPythonPath());

        // Start the Python process and connect to it
        m_process = pb.start();
        m_clientServer = new ClientServer(null);
        m_entryPoint = (T)m_clientServer.getPythonServerEntryPoint(new Class[]{entryPointClass});
        waitForConnection(m_entryPoint);

        // Register extensions
        m_entryPoint.registerExtensions(extensions.stream().map(e -> e.getPythonModule()).collect(Collectors.toList()));
    }

    /**
     * @return the entry point into Python. Calling methods on this object will call Python functions.
     */
    public T getEntryPoint() {
        return m_entryPoint;
    }

    @Override
    public void close() {
        // Mostly copied from PythonKernel.
        // If the original process was a script, we have to kill the actual Python
        // process by PID.
        final int pid = m_entryPoint.getPid();

        if (m_clientServer != null) {
            m_clientServer.shutdown();
            // TODO: May require further cleanup. See:
            // https://www.py4j.org/advanced_topics.html#py4j-memory-model
            m_clientServer = null;
        }

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
        if (m_process != null) {
            m_process.destroyForcibly();
            // TODO: Further action required in case the process cannot be destroyed
            // via Java. See PythonKernel#close()
        }
    }

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
                    System.out.println("Connected to Python process with PID: " + pid + ", after attempts: "
                        + numAttempts + ". Took ms: " + time + ".");
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
