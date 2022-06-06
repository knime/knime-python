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
 *   Jun 3, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.knime.core.node.NodeLogger;

/**
 * Asynchronously reads lines from an InputStream and writes it to a Consumer provided in the constructor.
 * Closing an instance will read all currently available values from the InputStream and then stop.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class AsyncLineRedirector implements Closeable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AsyncLineRedirector.class);

    private final Future<?> m_future;

    private final Consumer<String> m_lineConsumer;

    private final BufferedReader m_lineReader;

    private final StoppableInputStream m_stoppableStream;

    private final CountDownLatch m_closeLatch = new CountDownLatch(1);

    private final long m_closeTimeoutInMs;

    AsyncLineRedirector(final Function<Runnable, Future<?>> executor, final InputStream stream, final Consumer<String> lineConsumer, final long closeTimeoutInMs) {
        m_lineConsumer = lineConsumer;
        m_stoppableStream = new StoppableInputStream(stream, 100);
        // we use the system default because that's also what the process uses
        m_lineReader = new BufferedReader(new InputStreamReader(m_stoppableStream)); // NOSONAR
        m_closeTimeoutInMs = closeTimeoutInMs;
        m_future = executor.apply(this::readLines);
    }

    private void readLines() {
        String line;
        try {
            while ((line = m_lineReader.readLine()) != null) {
                m_lineConsumer.accept(line);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        } finally {
            m_closeLatch.countDown();
        }
    }

    @Override
    public void close() throws IOException {
        m_stoppableStream.stop();
        try {
            if (!m_closeLatch.await(m_closeTimeoutInMs, TimeUnit.MILLISECONDS)) {
                LOGGER.debugWithFormat("The thread was not stopped within %s ms.", m_closeTimeoutInMs);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while waiting for thread to stop.");
        }
        m_future.cancel(true);
        m_lineReader.close();
    }

}