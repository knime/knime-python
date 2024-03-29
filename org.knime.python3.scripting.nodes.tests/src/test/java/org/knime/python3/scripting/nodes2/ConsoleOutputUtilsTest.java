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
 *   Sep 21, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.util.PathUtils;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

/**
 * Unit tests for the ConsoleOutputUtils.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ConsoleOutputUtilsTest {

    private static final ConsoleText[] SIMPLE_TEST_TEXTS = new ConsoleText[]{ //
        new ConsoleText("something to stdout", false), //
        new ConsoleText("something to stderr", true), //
        new ConsoleText("more to stdout", false), //
        new ConsoleText("even more to stdout", false), //
        new ConsoleText("end with stderr", true), //
    };

    @Test
    public void testConsumingEmptyOutput() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        var emptyArray = new ConsoleText[0];
        sendToConsumer(emptyArray, consumer);
        try (var storage = consumer.finish()) {
            storage.sendConsoleOutputs((text) -> {
                throw new AssertionError("Got text " + text + " but expected none.");
            });
        }

    }

    /**
     * Simple test of consuming and re-sending console output.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("static-method")
    public void testConsumingOutput() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        sendToConsumer(SIMPLE_TEST_TEXTS, consumer);
        try (var storage = consumer.finish()) {
            final TestConsumer testConsumer = new TestConsumer(SIMPLE_TEST_TEXTS);
            storage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", SIMPLE_TEST_TEXTS.length, testConsumer.m_nextIdx);
        }
    }

    /**
     * Test saving and loading a console output storage.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("static-method")
    public void testSavingAndLoadingOutput() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        sendToConsumer(SIMPLE_TEST_TEXTS, consumer);

        // Save to a temporary folder
        final Path tmpPath = PathUtils.createTempDir("tmp_output_storage");
        try (var storage = consumer.finish()) {
            storage.saveTo(tmpPath);
        }

        // Open from the temporary folder and check the values
        try (var loadedStorage = ConsoleOutputUtils.openConsoleOutput(tmpPath)) {
            final TestConsumer testConsumer = new TestConsumer(SIMPLE_TEST_TEXTS);
            loadedStorage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", SIMPLE_TEST_TEXTS.length, testConsumer.m_nextIdx);
        }

        // Cleanup
        PathUtils.deleteDirectoryIfExists(tmpPath);
    }

    /**
     * Test saving and loading a console output storage with overflow.
     *
     * @throws IOException
     */
    @Test
    @SuppressWarnings("static-method")
    public void testSavingAndLoadingOverflowOutput() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        var texts = createTestTexts(ConsoleOutputUtils.MAX_ROWS_PER_TABLE * 2 + 100);
        sendToConsumer(texts, consumer);

        // Save to a temporary folder
        final Path tmpPath = PathUtils.createTempDir("tmp_output_storage");
        try (var storage = consumer.finish()) {
            storage.saveTo(tmpPath);
        }

        var expectedTexts =
            Arrays.stream(texts).skip(ConsoleOutputUtils.MAX_ROWS_PER_TABLE).toArray(ConsoleText[]::new);

        // Open from the temporary folder and check the values
        try (var loadedStorage = ConsoleOutputUtils.openConsoleOutput(tmpPath)) {
            final TestConsumer testConsumer = new TestConsumer(expectedTexts);
            loadedStorage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", expectedTexts.length, testConsumer.m_nextIdx);
        }

        // Cleanup
        PathUtils.deleteDirectoryIfExists(tmpPath);
    }

    @Test
    public void testLargeOutputNoOverflow() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        var texts = createTestTexts(ConsoleOutputUtils.MAX_ROWS_PER_TABLE * 2);
        sendToConsumer(texts, consumer);

        try (var storage = consumer.finish()) {
            final TestConsumer testConsumer = new TestConsumer(texts);
            storage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", texts.length, testConsumer.m_nextIdx);
        }
    }

    @Test
    public void testLargeOutputOverflowBy1() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        var texts = createTestTexts(ConsoleOutputUtils.MAX_ROWS_PER_TABLE * 2 + 1);
        sendToConsumer(texts, consumer);

        var expectedTexts =
            Arrays.stream(texts).skip(ConsoleOutputUtils.MAX_ROWS_PER_TABLE).toArray(ConsoleText[]::new);
        try (var storage = consumer.finish()) {
            final TestConsumer testConsumer = new TestConsumer(expectedTexts);
            storage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", expectedTexts.length, testConsumer.m_nextIdx);
        }
    }

    @Test
    public void testLargeOutputOverflowTwice() throws IOException {
        final var consumer = ConsoleOutputUtils.createConsoleConsumer();
        var texts = createTestTexts(ConsoleOutputUtils.MAX_ROWS_PER_TABLE * 3 + 100);
        sendToConsumer(texts, consumer);

        var expectedTexts =
            Arrays.stream(texts).skip(ConsoleOutputUtils.MAX_ROWS_PER_TABLE * 2).toArray(ConsoleText[]::new);
        try (var storage = consumer.finish()) {
            final TestConsumer testConsumer = new TestConsumer(expectedTexts);
            storage.sendConsoleOutputs(testConsumer);
            assertEquals("Expected to have recieved all texts", expectedTexts.length, testConsumer.m_nextIdx);
        }
    }

    /** Send the given values to the consumer */
    private static void sendToConsumer(final ConsoleText[] values, final Consumer<ConsoleText> consumer) {
        for (var v : values) {
            consumer.accept(v);
        }
    }

    private static ConsoleText[] createTestTexts(final int numTexts) {
        return IntStream.range(0, numTexts) //
            .mapToObj(i -> new ConsoleText("" + i, false)) //
            .toArray(ConsoleText[]::new);
    }

    /** Assert that the console text equals the expected text */
    private static void assertEqualsConsoleText(final ConsoleText expected, final ConsoleText actual) {
        assertEquals("text is expected to be equal", expected.text, actual.text);
        assertEquals("stderr flag is expected to be equal", expected.stderr, actual.stderr);
    }

    /** A consumer that compares what it gets with an array of expected results */
    private static final class TestConsumer implements Consumer<ConsoleText> {

        private final ConsoleText[] m_expected;

        private int m_nextIdx;

        public TestConsumer(final ConsoleText[] expected) {
            m_expected = expected;
            m_nextIdx = 0;
        }

        @Override
        public void accept(final ConsoleText t) {
            assertEqualsConsoleText(m_expected[m_nextIdx++], t);
        }
    }
}
