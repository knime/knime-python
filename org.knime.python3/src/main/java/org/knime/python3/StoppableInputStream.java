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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link InputStream} that can be stopped while the underlying stream is not at the end of file yet.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// read(byte[],int,int) is not overwritten because we would have to check availability before each read anyway
// and therefore the implementation would be the same as the one in InputStream
final class StoppableInputStream extends InputStream {//NOSONAR

    private final CountDownLatch m_stopper = new CountDownLatch(1);

    private final AtomicBoolean m_isRunning = new AtomicBoolean(true);

    private final InputStream m_in;

    private final long m_waitTimeInMs;

    protected StoppableInputStream(final InputStream in, final long waitTimeInMs) {
        m_in = in;
        m_waitTimeInMs = waitTimeInMs;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        boolean inputAvailable;
        while ((inputAvailable = isInputAvailable()) || m_isRunning.get()) {
            if (inputAvailable) {
                return m_in.read(b, off, len);
            } else {
                waitForStop();
            }
        }
        return -1;
    }

    @Override
    public int read() throws IOException {
        boolean inputAvailable;
        while ((inputAvailable = isInputAvailable()) || m_isRunning.get()) {
            if (inputAvailable) {
                return m_in.read();
            } else {
                waitForStop();
            }
        }
        return -1;
    }

    private boolean isInputAvailable() throws IOException {
        return m_in.available() > 0;
    }

    private void waitForStop() {
        try {
            m_stopper.await(m_waitTimeInMs, TimeUnit.MILLISECONDS);//NOSONAR
        } catch (InterruptedException ex) {
            stop();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int available() throws IOException {
        return m_in.available();
    }

    /**
     * Sends the stop signal and immediately returns.<br>
     * This means that any blocked {@link #read()} will return EOF and new {@link #read()} calls will no longer wait
     * i.e. they will return EOF if {@link #available()} returns 0.
     * Note that this method does not guarantee that the consumer of this stream will have processed all data. Ensuring
     * that requires external synchronization.
     */
    void stop() {
        m_isRunning.set(false);
        m_stopper.countDown();
    }

    /**
     * Closing this stream has no effect. It especially doesn't close the delegate stream.
     */
    @Override
    public void close() throws IOException {
        m_in.close();
    }

}
