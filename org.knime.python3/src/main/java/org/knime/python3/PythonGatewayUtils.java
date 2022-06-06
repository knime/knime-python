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
 *   Jun 5, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.knime.core.util.ThreadUtils;
import org.knime.python3.utils.AutoCloser;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Utility functions for handling PythonGateways.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonGatewayUtils {

    private static final ExecutorService OUTPUT_RETRIEVER_EXECUTOR =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-output-redirector").build());

    /**
     * Redirects the standard output and standard error of a PythonGateway to the provided consumers.
     * The redirections happens asynchronously and is stopped once the returned AutoCloseable is closed.
     *
     * @param gateway whose output to redirect
     * @param stdOutConsumer consumer for the standard output. Must not block!
     * @param stdErrConsumer consumer for the standard error. Must not block!
     * @param closeTimeoutInMs time to wait for when closing the redirector
     * @return an AutoCloseable that when closed stops the redirection after it reads all available data
     */
    // the streams of the gateway are not closeable (they are closed by the Python process when it is shut down)
    // the redirectors are closed by the returned AutoCloseable
    @SuppressWarnings("resource")
    public static AutoCloseable redirectGatewayOutput(final PythonGateway<?> gateway,
        final Consumer<String> stdOutConsumer, final Consumer<String> stdErrConsumer, final long closeTimeoutInMs) {
        var stdOutRetriever = new AsyncLineRedirector(PythonGatewayUtils::submit,
            gateway.getStandardOutputStream(), stdOutConsumer, closeTimeoutInMs);
        var stdErrRetriever = new AsyncLineRedirector(PythonGatewayUtils::submit,
            gateway.getStandardErrorStream(), stdErrConsumer, closeTimeoutInMs);
        return new AutoCloser(stdOutRetriever, stdErrRetriever);
    }

    private static Future<?> submit(final Runnable runnable) {
        // A PythonGateway may also be created outside of a NodeContext (e.g. when the NodeRepository is loaded)
        // therefore we don't log the UnnecessaryCallExceptions that ThreadUtils usually spams the knime.log with
        var runnableWithContext = ThreadUtils.runnableWithContext(runnable, false);
        return OUTPUT_RETRIEVER_EXECUTOR.submit(runnableWithContext);
    }

    private PythonGatewayUtils() {

    }
}
