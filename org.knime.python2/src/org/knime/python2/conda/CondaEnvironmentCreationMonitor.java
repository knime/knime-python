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

import java.io.IOException;

import org.apache.commons.lang.SystemUtils;
import org.knime.core.node.NodeLogger;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Allows to monitor the progress of a Conda environment creation command. Conda only reports progress for package
 * downloads, at the moment.
 */
public abstract class CondaEnvironmentCreationMonitor extends CondaExecutionMonitor {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CondaEnvironmentCreationMonitor.class);

    @Override
    void handleCustomJsonOutput(final TreeNode json) {
        final TreeNode fetch = json.get("fetch");
        if (fetch != null) {
            final String packageNameValue = ((JsonNode)fetch).textValue().split(" ")[0];
            final boolean finishedValue = ((JsonNode)json.get("finished")).booleanValue();
            final double maxvalValue = ((JsonNode)json.get("maxval")).doubleValue();
            final double progressValue = ((JsonNode)json.get("progress")).doubleValue();
            handlePackageDownloadProgress(packageNameValue, finishedValue, progressValue / maxvalValue);
        }
    }

    /**
     * Asynchronous callback that allows to process progress in the download of a Python package.<br>
     * Exceptions thrown by this callback are discarded.
     *
     * @param currentPackage The package for which progress is reported.
     * @param packageFinished {@code true} if downloading the current package is finished, {@code false} otherwise.
     *            Should be accompanied by a {@code progress} value of 1.
     * @param progress The progress as a fraction in [0, 1].
     */
    protected abstract void handlePackageDownloadProgress(String currentPackage, boolean packageFinished,
        double progress);

    @Override
    protected void handleCanceledExecution(final Process conda) {
        try {
            if (SystemUtils.IS_OS_WINDOWS) {
                boolean interrupted = Thread.interrupted();
                final long pid = conda.pid();
                try {
                    // Call `taskkill /F /T /PID <pid>` to kill the process and all its children
                    new ProcessBuilder("taskkill", "/F", "/T", "/PID", "" + pid).start().waitFor();
                } catch (final InterruptedException ex) { // NOSONAR: Re-interrupted later
                    LOGGER.warn("Killing the conda process was interrupted.", ex);
                    interrupted = true;
                } catch (final IOException ex) {
                    LOGGER.warn("Killing the conda process failed.", ex);
                }
                // Re-interrupt the thread
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            super.handleCanceledExecution(conda);
        }
    }
}