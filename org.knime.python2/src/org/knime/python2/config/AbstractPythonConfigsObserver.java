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
 *   Jan 25, 2019 (marcel): created
 */
package org.knime.python2.config;

import java.util.concurrent.CopyOnWriteArrayList;

import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonVersion;

/**
 * Handles the listeners for a config observer.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractPythonConfigsObserver {

    private CopyOnWriteArrayList<PythonConfigsInstallationTestStatusChangeListener> m_listeners =
        new CopyOnWriteArrayList<>();

    /**
     * Notifies the listeners that the conda installation test is starting.
     */
    protected synchronized void onCondaInstallationTestStarting() {
        for (final PythonConfigsInstallationTestStatusChangeListener listener : m_listeners) {
            listener.condaInstallationTestStarting();
        }
    }

    /**
     * Notifies the listeners that the conda installation test has finished.
     *
     * @param errorMessage an error that occurred during the installation test
     */
    protected synchronized void onCondaInstallationTestFinished(final String errorMessage) {
        for (final PythonConfigsInstallationTestStatusChangeListener listener : m_listeners) {
            listener.condaInstallationTestFinished(errorMessage);
        }
    }

    /**
     * Notifies the listeners that an environment installation test is starting.
     *
     * @param environmentType the type of the environment (conda v. manual)
     * @param pythonVersion the python version (2 v. 3)
     */
    protected synchronized void onEnvironmentInstallationTestStarting(final PythonEnvironmentType environmentType,
        final PythonVersion pythonVersion) {
        for (final PythonConfigsInstallationTestStatusChangeListener listener : m_listeners) {
            listener.environmentInstallationTestStarting(environmentType, pythonVersion);
        }
    }

    /**
     * Notifies the listeners that an environment installation test has finished.
     *
     * @param environmentType the type of the environment (conda v. manual)
     * @param pythonVersion the python version (2 v. 3)
     * @param testResult the test result
     */
    protected synchronized void onEnvironmentInstallationTestFinished(final PythonEnvironmentType environmentType,
        final PythonVersion pythonVersion, final PythonKernelTestResult testResult) {
        for (final PythonConfigsInstallationTestStatusChangeListener listener : m_listeners) {
            listener.environmentInstallationTestFinished(environmentType, pythonVersion, testResult);
        }
    }

    /**
     * @param listener A listener which will be notified about changes in the status of any installation test initiated
     *            by this instance.
     */
    public void addConfigsTestStatusListener(final PythonConfigsInstallationTestStatusChangeListener listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    /**
     * @param listener The listener to remove.
     * @return {@code true} if the listener was present before removal.
     */
    public boolean removeConfigsTestStatusListener(final PythonConfigsInstallationTestStatusChangeListener listener) {
        return m_listeners.remove(listener);
    }

    /**
     * Listener which will be notified about changes in the status of installation tests initiated by the enclosing
     * class.
     */
    public static interface PythonConfigsInstallationTestStatusChangeListener {

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         */
        void condaInstallationTestStarting();

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param errorMessage Error messages that occurred during the installation test. Empty if the installation test
         *            was successful, i.e., conda is properly installed.
         */
        void condaInstallationTestFinished(String errorMessage);

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param environmentType The environment type of the environment whose installation test is about to start.
         * @param pythonVersion The Python version of the environment.
         */
        void environmentInstallationTestStarting(PythonEnvironmentType environmentType, PythonVersion pythonVersion);

        /**
         * Called asynchronously, that is, possibly not in a UI thread.
         *
         * @param environmentType The environment type of the environment whose installation test has finished.
         * @param pythonVersion The Python version of the environment.
         * @param testResult The result of the installation test.
         */
        void environmentInstallationTestFinished(PythonEnvironmentType environmentType, PythonVersion pythonVersion,
            PythonKernelTestResult testResult);
    }
}
