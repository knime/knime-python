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
 *   Jun 4, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class StoppableInputStreamTest {

    private PipedOutputStream m_pipeOut;

    private PipedInputStream m_pipeIn;

    private StoppableInputStream m_stoppable;

    private PrintStream m_printer;

    @Before
    public void setup() throws IOException {
        m_pipeOut = new PipedOutputStream();
        m_pipeIn = new PipedInputStream(m_pipeOut);
        m_stoppable = new StoppableInputStream(m_pipeIn, 100);
        m_printer = new PrintStream(m_pipeOut);//NOSONAR only for testing
    }

    @After
    public void shutdown() throws IOException {
        m_printer.close();
        m_pipeIn.close();
        m_pipeOut.close();
    }

    @Test
    public void testStopReadingAllLines() throws Exception {
        testBufferedReader(r -> stopBlockedBufferedReader(r, () -> {
            m_printer.println("foo");
            m_printer.println("bar");
            m_printer.println("la");
            return "foobarla";
        }));
    }

    @Test
    public void testStopReadingAllLinesWithNoFinalLineBreak() throws Exception {
        testBufferedReader(r -> stopBlockedBufferedReader(r, () -> {
            m_printer.println("foo");
            m_printer.println("bar");
            m_printer.print("la");
            return "foobarla";
        }));
    }

    private void stopBlockedBufferedReader(final BufferedReader reader, final Supplier<String> printAndReturnExpected)
        throws InterruptedException {
        var sb = new StringBuilder();
        var stopLatch = new CountDownLatch(1);
        var consumer = new Thread(() -> {
            readAllLines(reader, sb::append);
            stopLatch.countDown();
        }, "test-consumer");
        consumer.start();
        var expected = printAndReturnExpected.get();
        // wait until the consumer is blocked
        Thread.sleep(1000);
        m_stoppable.stop();
        stopLatch.await();
        assertEquals(expected, sb.toString());
    }

    /**
     * This is the typical pattern used for reading process output.
     *
     * @param reader to read lines from
     * @param sink to write lines to
     */
    private static void readAllLines(final BufferedReader reader, final Consumer<String> sink) {
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                sink.accept(line);
            }
        } catch (IOException e) {
            fail();
        }
    }

    private interface ReadTest<T> {
        void test(T testObject) throws Exception;
    }

    private void testBufferedReader(final ReadTest<BufferedReader> test) throws Exception {
        try (var reader = new BufferedReader(new InputStreamReader(m_stoppable))) { //NOSONAR only for testing
            test.test(reader);
        }
    }
}
