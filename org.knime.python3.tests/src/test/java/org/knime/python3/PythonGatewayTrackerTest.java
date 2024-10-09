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
 *   25 Nov 2022 (chaubold): created
 */
package org.knime.python3;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class PythonGatewayTrackerTest {

    private static PythonGatewayTracker TRACKER = PythonGatewayTracker.INSTANCE;

    private static class DummyEntryPoint implements PythonEntryPoint {

        @Override
        public int getPid() {
            return -1;
        }

        @Override
        public void enableDebugging() {
        }

        @Override
        public void enableDebugging(final boolean enableBreakpoints, final boolean enableDebugLog,
            final boolean debugLogToStderr, final int port) {
        }

        @Override
        public void registerExtensions(final List<String> extensions) {
        }

        @Override
        public void registerPythonValueFactory(final String pythonModule, final String pythonValueFactoryName,
            final String valueFactoryDataType, final String dataSpec, final String dataTraits,
            final String pythonValueTypeName, final boolean isDefaultPythonRepresentation) {
        }

        @Override
        public void registerToPandasColumnConverter(final String pythonModule, final String pythonClassName,
            final String valueFactory) {
        }

        @Override
        public void registerFromPandasColumnConverter(final String pythonModule, final String pythonClassName,
            final String valueTypeName) {
        }

    }

    private static class DummyPythonGateway implements PythonGateway<DummyEntryPoint> {
        private boolean m_isClosed = false;

        @Override
        public DummyEntryPoint getEntryPoint() {
            return null;
        }

        @Override
        public InputStream getStandardOutputStream() {
            return null;
        }

        @Override
        public InputStream getStandardErrorStream() {
            return null;
        }

        @Override
        public void close() throws IOException {
            m_isClosed = true;
        }

        public boolean isClosed() {
            return m_isClosed;
        }

        @Override
        public String getTerminationReason() {
            return null;
        }

    }

    @SuppressWarnings("resource")
    @Test
    public void testDelegatingClose() throws IOException {
        final var gateway = new DummyPythonGateway();
        assertFalse(gateway.isClosed());
        final var trackedGateway = TRACKER.createTrackedGateway(gateway);
        assertFalse(gateway.isClosed());
        trackedGateway.close();
        assertTrue(gateway.isClosed());
    }

    @SuppressWarnings("resource")
    @Test
    public void testTrackerCloses() throws IOException {
        final var gateway = new DummyPythonGateway();
        TRACKER.createTrackedGateway(gateway);
        TRACKER.clear();
        assertTrue(gateway.isClosed());
    }

    @SuppressWarnings("resource")
    @Test
    public void testCreateTrackerAfterClose() throws IOException {
        final var gateway1 = new DummyPythonGateway();
        final var gateway2 = new DummyPythonGateway();
        TRACKER.createTrackedGateway(gateway1);
        assertFalse(gateway1.isClosed());
        assertFalse(gateway2.isClosed());

        TRACKER.clear();
        assertTrue(gateway1.isClosed());
        assertFalse(gateway2.isClosed());

        TRACKER.createTrackedGateway(gateway2);
        assertTrue(gateway1.isClosed());
        assertFalse(gateway2.isClosed());

        TRACKER.clear();
        assertTrue(gateway1.isClosed());
        assertTrue(gateway2.isClosed());
    }

    @SuppressWarnings("resource")
    @Test
    public void testCloseGatewayAfterTrackerClose() throws IOException {
        final var gateway = new DummyPythonGateway();
        final var trackedGateway = TRACKER.createTrackedGateway(gateway);
        TRACKER.clear();
        trackedGateway.close();
        assertTrue(gateway.isClosed());
    }

    @SuppressWarnings("resource")
    @Test
    public void testTrackerClosesOnGateClose() throws IOException {
        final var gateway = new DummyPythonGateway();
        TRACKER.createTrackedGateway(gateway);
        TRACKER.onPythonGatewayCreationGateClose();
        assertTrue(gateway.isClosed());
    }
}
