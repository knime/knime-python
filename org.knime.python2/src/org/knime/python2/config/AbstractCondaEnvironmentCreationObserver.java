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
 *   Feb 26, 2019 (marcel): created
 */
package org.knime.python2.config;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda;
import org.knime.python2.Conda.CondaEnvironmentCreationMonitor;
import org.knime.python2.Conda.CondaEnvironmentIdentifier;
import org.knime.python2.PythonVersion;
import org.knime.python2.kernel.PythonCanceledExecutionException;

/**
 * {@link #startEnvironmentCreation(String, CondaEnvironmentCreationStatus) Initiates}, observes, and
 * {@link #cancelEnvironmentCreation(CondaEnvironmentCreationStatus) cancels} Conda environment creation processes for a
 * specific Conda installation and Python version. Allows clients to subscribe to changes in the status of such creation
 * processes.
 * <P>
 * Note: The current implementation only allows one active creation process at a time.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractCondaEnvironmentCreationObserver {

    private static final String IN_PROGRESS_MESSAGE = "Creating Conda environment...";

    private final CopyOnWriteArrayList<CondaEnvironmentCreationStatusListener> m_listeners =
        new CopyOnWriteArrayList<>();

    private final PythonVersion m_pythonVersion;

    private final SettingsModelString m_condaDirectoryPath;

    private CondaEnvironmentCreationMonitor m_currentCreationMonitor;

    // Not meant for saving/loading. We just want observable values here to communicate with the view:

    private static final String DUMMY_CFG_KEY = "dummy";

    private final SettingsModelBoolean m_environmentCreationEnabled = new SettingsModelBoolean(DUMMY_CFG_KEY, true);

    /**
     * The created instance is {@link #getIsEnvironmentCreationEnabled() disabled by default}.
     *
     * @param environmentPythonVersion The Python version of the Conda environments created by this instance.
     * @param condaDirectoryPath The Conda directory path. Changes in the model are reflected by this instance.
     */
    public AbstractCondaEnvironmentCreationObserver(final PythonVersion environmentPythonVersion,
        final SettingsModelString condaDirectoryPath) {
        m_pythonVersion = environmentPythonVersion;
        m_condaDirectoryPath = condaDirectoryPath;
    }

    /**
     * @return The Python version of the environments created by this instance.
     */
    public PythonVersion getPythonVersion() {
        return m_pythonVersion;
    }

    /**
     * @return The enabled state of this instance. Only enabled instances (i.e., instances whose enabled model returns
     *         {@link SettingsModelBoolean#getBooleanValue() <code>true</code>}) are able to
     *         {@link #startEnvironmentCreation(String, CondaEnvironmentCreationStatus) initiate environment creation}.
     */
    public SettingsModelBoolean getIsEnvironmentCreationEnabled() {
        return m_environmentCreationEnabled;
    }

    /**
     * @param suffix a suffix for the environment name. Can be empty.
     * @return The default environment name for the next environment created by this instance. Returns an empty string
     *         in case calling Conda failed.<br>
     *         Note that this method makes no guarantees about the uniqueness of the returned name if invoked in
     *         parallel to an ongoing environment creation process.
     */
    protected String getDefaultEnvironmentName(final String suffix) {
        try {
            final Conda conda = new Conda(m_condaDirectoryPath.getStringValue());
            final String defaultEnvironmentName;
            if (m_pythonVersion.equals(PythonVersion.PYTHON2)) {
                defaultEnvironmentName = conda.getPython2EnvironmentName(suffix);
            } else if (m_pythonVersion.equals(PythonVersion.PYTHON3)) {
                defaultEnvironmentName = conda.getPython3EnvironmentName(suffix);
            } else {
                throw new IllegalStateException("Python version '" + m_pythonVersion
                    + "' is neither Python 2 nor Python " + "3. This is an implementation error.");
            }
            return defaultEnvironmentName;
        } catch (final Exception ex) {
            return "";
        }
    }

    /**
     * Initiates the a new Conda environment creation process. Only allowed if this instance is
     * {@link #getIsEnvironmentCreationEnabled() enabled}.
     *
     * @param environmentName The name of the environment. Must not already exist in the local Conda installation. May
     *            be {@code null} or empty in which case a unique default name is used.
     * @param status The status object that is will be notified about changes in the state of the initiated creation
     *            process. Can also be used to {@link #cancelEnvironmentCreation(CondaEnvironmentCreationStatus) cancel}
     *            the creation process. A new status object must be used for each new creation process.
     * @param pathToEnvFile path to the environment definition file. If not present the default environment is used.
     */
    protected synchronized void startEnvironmentCreation(final String environmentName,
        final CondaEnvironmentCreationStatus status, final Optional<String> pathToEnvFile) {
        if (!m_environmentCreationEnabled.getBooleanValue()) {
            throw new IllegalStateException("Environment creation is not enabled.");
        }
        if (m_currentCreationMonitor != null) {
            throw new IllegalStateException("Environment generation was tried to be started although one is already in "
                + "progress. This is currently not supported and therefore an implementation error.");
        }
        m_currentCreationMonitor = new StateUpdatingCondaEnvironmentCreationMonitor(status);
        new Thread(() -> {
            try {
                onEnvironmentCreationStarting(status);
                final Conda conda = new Conda(m_condaDirectoryPath.getStringValue());
                final CondaEnvironmentIdentifier createdEnvironment;
                if (pathToEnvFile.isPresent()) {
                    createdEnvironment = conda.createEnvironmentFromFile(m_pythonVersion, pathToEnvFile.get(),
                        environmentName, m_currentCreationMonitor);
                } else if (m_pythonVersion.equals(PythonVersion.PYTHON2)) {
                    createdEnvironment =
                        conda.createDefaultPython2Environment(environmentName, m_currentCreationMonitor);
                } else if (m_pythonVersion.equals(PythonVersion.PYTHON3)) {
                    createdEnvironment =
                        conda.createDefaultPython3Environment(environmentName, m_currentCreationMonitor);
                } else {
                    throw new IllegalStateException("Python version '" + m_pythonVersion
                        + "' is neither Python 2 nor Python " + "3. This is an implementation error.");
                }
                onEnvironmentCreationFinished(status, createdEnvironment);
            } catch (final PythonCanceledExecutionException ex) {
                onEnvironmentCreationCanceled(status);
            } catch (final Exception ex) {
                NodeLogger.getLogger(AbstractCondaEnvironmentCreationObserver.class).debug(ex.getMessage(), ex);
                onEnvironmentCreationFailed(status, ex.getMessage());
            } finally {
                synchronized (AbstractCondaEnvironmentCreationObserver.this) {
                    m_currentCreationMonitor = null;
                }
            }
        }).start();
    }

    /**
     * Cancels the active new Conda environment creation process.
     * <P>
     * Note: Currently unused since the observer only allows one active creation process at a time.
     *
     * @param status The status of the creation process to cancel.
     */
    public synchronized void cancelEnvironmentCreation(final CondaEnvironmentCreationStatus status) {
        if (m_currentCreationMonitor != null) {
            m_currentCreationMonitor.cancel();
            status.m_statusMessage.setStringValue("Canceling environment creation...");
        }
    }

    private void onEnvironmentCreationStarting(final CondaEnvironmentCreationStatus status) {
        status.m_statusMessage.setStringValue(IN_PROGRESS_MESSAGE);
        for (final CondaEnvironmentCreationStatusListener listener : m_listeners) {
            listener.condaEnvironmentCreationStarting(status);
        }
    }

    private void onEnvironmentCreationFinished(final CondaEnvironmentCreationStatus status,
        final CondaEnvironmentIdentifier createdEnvironment) {
        status.m_statusMessage.setStringValue(
            "Environment creation finished.\nNew environment's name: '" + createdEnvironment.getName() + "'.");
        for (final CondaEnvironmentCreationStatusListener listener : m_listeners) {
            listener.condaEnvironmentCreationFinished(status, createdEnvironment);
        }
    }

    private void onEnvironmentCreationCanceled(final CondaEnvironmentCreationStatus status) {
        status.m_statusMessage.setStringValue("Environment creation was canceled.");
        for (final CondaEnvironmentCreationStatusListener listener : m_listeners) {
            listener.condaEnvironmentCreationCanceled(status);
        }
    }

    private void onEnvironmentCreationFailed(final CondaEnvironmentCreationStatus status, final String errorMessage) {
        status.m_statusMessage.setStringValue("Environment creation failed: " + errorMessage);
        for (final CondaEnvironmentCreationStatusListener listener : m_listeners) {
            listener.condaEnvironmentCreationFailed(status, errorMessage);
        }
    }

    /**
     * @param listener A listener which will be notified about changes in the status of the any environment creation
     *            process started by this instance.
     * @param prepend {@code true} if {@code listener} shall be prepended to the list of listeners, {@code false} if it
     *            shall be appended.
     */
    public void addEnvironmentCreationStatusListener(final CondaEnvironmentCreationStatusListener listener,
        final boolean prepend) {
        if (!m_listeners.contains(listener)) {
            if (prepend) {
                m_listeners.add(0, listener);
            } else {
                m_listeners.add(listener);
            }
        }
    }

    /**
     * @param listener The listener to remove.
     * @return {@code true} if the listener was present before removal.
     */
    public boolean removeEnvironmentCreationStatusListener(final CondaEnvironmentCreationStatusListener listener) {
        return m_listeners.remove(listener);
    }

    /**
     * Listener which will be notified about changes in the status of all environment creations initiated by the
     * enclosing observer.
     */
    public interface CondaEnvironmentCreationStatusListener {

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param status The status of the corresponding creation process.
         */
        void condaEnvironmentCreationStarting(CondaEnvironmentCreationStatus status);

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param status The status of the corresponding creation process.
         * @param createdEnvironment A description of the created environment.
         */
        void condaEnvironmentCreationFinished(CondaEnvironmentCreationStatus status,
            CondaEnvironmentIdentifier createdEnvironment);

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param status The status of the corresponding creation process.
         */
        void condaEnvironmentCreationCanceled(CondaEnvironmentCreationStatus status);

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param status The status of the corresponding creation process.
         * @param errorMessage The message of the error that made environment creation fail.
         */
        void condaEnvironmentCreationFailed(CondaEnvironmentCreationStatus status, String errorMessage);
    }

    /**
     * Encapsulates the state of a single Conda environment creation process.
     */
    public static class CondaEnvironmentCreationStatus {

        private static final String DEFAULT_STRING_VALUE = "";

        private static final int DEFAULT_INT_VALUE = 0;

        // Not meant for saving/loading. We just want observable values here to communicate with the view:

        private final SettingsModelString m_statusMessage =
            new SettingsModelString(DUMMY_CFG_KEY, DEFAULT_STRING_VALUE);

        private final SettingsModelInteger m_progress = new SettingsModelInteger(DUMMY_CFG_KEY, DEFAULT_INT_VALUE);

        private final SettingsModelString m_errorLog = new SettingsModelString(DUMMY_CFG_KEY, DEFAULT_STRING_VALUE);

        /**
         * @return The status message of the current environment creation process.
         */
        public SettingsModelString getStatusMessage() {
            return m_statusMessage;
        }

        /**
         * @return The package download progress of the current environment creation process.
         */
        public SettingsModelInteger getProgress() {
            return m_progress;
        }

        /**
         * @return The error log of the current environment creation process.
         */
        public SettingsModelString getErrorLog() {
            return m_errorLog;
        }
    }

    private static final class StateUpdatingCondaEnvironmentCreationMonitor extends CondaEnvironmentCreationMonitor {

        private final CondaEnvironmentCreationStatus m_status;

        private StateUpdatingCondaEnvironmentCreationMonitor(final CondaEnvironmentCreationStatus status) {
            m_status = status;

        }

        @Override
        protected void handlePackageDownloadProgress(final String currentPackage, final boolean packageFinished,
            final double progress) {
            if (!packageFinished) {
                m_status.m_statusMessage.setStringValue("Downloading package '" + currentPackage + "'...");
            } else {
                m_status.m_statusMessage.setStringValue(IN_PROGRESS_MESSAGE);
            }
            m_status.m_progress.setIntValue((int)(progress * 100));
        }

        @Override
        protected void handleWarningMessage(final String warning) {
            handleErrorMessage(warning);
        }

        @Override
        protected void handleErrorMessage(final String message) {
            m_status.m_errorLog.setStringValue(m_status.m_errorLog.getStringValue() + message + "\n");
        }
    }
}
