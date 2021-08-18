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
 *   Dec 8, 2020 (marcel): created
 */
package org.knime.python2.conda;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.knime.core.node.KNIMEConstants;
import org.knime.core.util.ThreadPool;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.util.PythonUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class CondaExecutionMonitor {

    private final List<String> m_standardOutputErrors = new ArrayList<>();

    private final List<String> m_errorOutputErrors = new ArrayList<>();

    private boolean m_isCanceled;

    void monitorExecution(final Process conda, final boolean hasJsonOutput)
        throws IOException, PythonCanceledExecutionException {
        Future<?> outputListener = null;
        Future<?> errorListener = null;
        try {
            final ThreadPool pool = KNIMEConstants.GLOBAL_THREAD_POOL;
            outputListener = pool.enqueue(() -> parseOutputStream(conda.getInputStream(), hasJsonOutput));
            errorListener = pool.enqueue(() -> parseErrorStream(conda.getErrorStream()));
            final int condaExitCode = awaitTermination(conda, this);
            if (condaExitCode != 0) {
                // Wait for listeners to finish consuming their streams before creating the error message.
                try {
                    outputListener.get();
                    errorListener.get();
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new PythonCanceledExecutionException(ex);
                } catch (final ExecutionException ex) { // NOSONAR Errors are recorded elsewhere. Nothing to add here.
                    // Ignore, use whatever error-related output we have so far.
                }
                final String errorMessage = createErrorMessage(condaExitCode);
                throw new IOException(errorMessage);
            }
        } finally {
            if (outputListener != null) {
                outputListener.cancel(true);
            }
            if (errorListener != null) {
                errorListener.cancel(true);
            }
        }
    }

    private void parseOutputStream(final InputStream standardOutput, final boolean isJsonOutput) {
        try {
            if (isJsonOutput) {
                parseJsonOutput(standardOutput);
            } else {
                parseNonJsonOutput(standardOutput);
            }
        } catch (final IOException ex) {
            if (!isCanceledOrInterrupted()) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    private void parseJsonOutput(final InputStream standardOutput) throws IOException {
        try (final JsonParser parser =
            new JsonFactory(new ObjectMapper()).createParser(new BufferedInputStream(standardOutput))) {
            while (!isCanceledOrInterrupted()) {
                try {
                    final TreeNode json = parser.readValueAsTree();
                    if (json == null) {
                        // EOF
                        break;
                    }
                    final String errorMessage = parseErrorFromJsonOutput(json);
                    if (!errorMessage.isEmpty()) {
                        m_standardOutputErrors.add(errorMessage);
                        handleErrorMessage(errorMessage);
                    } else {
                        handleCustomJsonOutput(json);
                    }
                }
                // No Sonar: Receiving improper output from Conda is expected. Catching an exception here is part of the
                // normal control flow.
                catch (final JsonParseException ex) { // NOSONAR
                    // Ignore and continue; wait for proper output.
                }
            }
        }
    }

    private static String parseErrorFromJsonOutput(final TreeNode json) {
        String errorMessage = "";
        final TreeNode error = json.get("error");
        if (error != null) {
            final TreeNode exceptionName = json.get("exception_name");
            if (exceptionName != null && "ResolvePackageNotFound".equals(((JsonNode)exceptionName).textValue())) {
                errorMessage += "Failed to resolve the following list of packages.\nPlease make sure these "
                    + "packages are available for the local platform or exclude them from the creation process.";
            }
            final TreeNode message = json.get("message");
            if (message != null) {
                errorMessage += ((JsonNode)message).textValue();
            } else {
                errorMessage += ((JsonNode)error).textValue();
            }
            final TreeNode reason = json.get("reason");
            if (reason != null && ((JsonNode)reason).textValue().equals("CONNECTION FAILED")) {
                errorMessage += "\nPlease check your internet connection.";
            }
        }
        return errorMessage;
    }

    /**
     * Asynchronous callback that allows to process error messages (usually just one line at a time) of the monitored
     * Conda command.<br>
     * Exceptions thrown by this callback are discarded.
     *
     * @param error The error message, neither {@code null} nor empty.
     */
    protected void handleErrorMessage(final String error) {
        // Do nothing by default.
    }

    /**
     * Asynchronous callback that allows to process custom JSON output of the monitored Conda command.<br>
     * Exceptions thrown by this callback are discarded.
     *
     * @param json The node that represents the root element of the read JSON output. Not {@code null}.
     */
    void handleCustomJsonOutput(final TreeNode json) {
        // Do nothing by default.
    }

    private void parseNonJsonOutput(final InputStream standardOutput) {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(standardOutput))) {
            String line;
            while (!isCanceledOrInterrupted() && (line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    handleCustomNonJsonOutput(line);
                }
            }
        } catch (final IOException ex) {
            if (!isCanceledOrInterrupted()) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    /**
     * Asynchronous callback that allows to process custom non-JSON output messages (usually just one line at a time) of
     * the monitored Conda command.<br>
     * Exceptions thrown by this callback are discarded.
     *
     * @param output The output message, neither {@code null} nor empty.
     */
    void handleCustomNonJsonOutput(final String output) {
        // Do nothing by default.
    }

    private void parseErrorStream(final InputStream errorOutput) {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(errorOutput))) {
            String line;
            boolean inWarning = false;
            while (!isCanceledOrInterrupted() && (line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    inWarning = inWarning || line.startsWith("==> WARNING: A newer version of conda exists. <==");
                    if (inWarning) {
                        handleWarningMessage(line);
                    } else {
                        m_errorOutputErrors.add(line);
                        handleErrorMessage(line);
                    }
                    inWarning = inWarning && !line.startsWith("$ conda update -n base");
                }
            }
        } catch (final IOException ex) {
            if (!isCanceledOrInterrupted()) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    /**
     * Asynchronous callback that allows to process warning messages (usually just one line at a time) of the monitored
     * Conda command.<br>
     * Exceptions thrown by this callback are discarded.
     *
     * @param warning The warning message, neither {@code null} nor empty.
     */
    protected void handleWarningMessage(final String warning) {
        // Do nothing by default.
    }

    private int awaitTermination(final Process conda, final CondaExecutionMonitor monitor)
        throws IOException, PythonCanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(conda::waitFor, KNIMEConstants.GLOBAL_THREAD_POOL::enqueue,
                new PythonCancelableFromCondaExecutionMonitor(monitor));
        } catch (final PythonCanceledExecutionException ex) {
            handleCanceledExecution(conda);
            throw ex;
        }
    }

    protected void handleCanceledExecution(final Process conda) {
        // Destroy the process by default
        // NOTE: On Windows subprocesses will not be killed
        conda.destroy();
    }

    private String createErrorMessage(final int condaExitCode) {
        String errorMessage = null;
        if (!m_standardOutputErrors.isEmpty()) {
            errorMessage = String.join("\n", m_standardOutputErrors);
        }
        if (!m_errorOutputErrors.isEmpty()) {
            final String detailMessage = String.join("\n", m_errorOutputErrors);
            if (errorMessage == null) {
                errorMessage = "Failed to execute Conda";
                if (detailMessage.contains("CONNECTION FAILED") && detailMessage.contains("SSLError")) {
                    errorMessage += ". Please uninstall and reinstall Conda.\n";
                } else {
                    errorMessage += ":\n";
                }
                errorMessage += detailMessage;
            } else {
                errorMessage += "\nAdditional output: " + detailMessage;
            }
        }
        if (errorMessage == null) {
            errorMessage = "Conda process terminated with error code " + condaExitCode + ".";
        }
        return errorMessage;
    }

    /**
     * Cancels the execution of the monitored conda command.
     */
    public synchronized void cancel() {
        m_isCanceled = true;
    }

    protected synchronized boolean isCanceledOrInterrupted() {
        return m_isCanceled || Thread.currentThread().isInterrupted();
    }

    private static final class PythonCancelableFromCondaExecutionMonitor implements PythonCancelable {

        private final CondaExecutionMonitor m_monitor;

        private PythonCancelableFromCondaExecutionMonitor(final CondaExecutionMonitor monitor) {
            m_monitor = monitor;
        }

        @Override
        public void checkCanceled() throws PythonCanceledExecutionException {
            if (m_monitor.isCanceledOrInterrupted()) {
                throw new PythonCanceledExecutionException();
            }
        }
    }
}
