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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.knime.core.node.NodeLogger;
import org.knime.python3.PythonGatewayCreationGate.PythonGatewayCreationGateListener;

import com.google.common.cache.CacheBuilder;

/**
 * Tracks {@link PythonGateway}s created by {@link PythonGatewayFactory}s in order to be able to close them in case all
 * running Python processes need to be cleaned up (e.g. during environment installation).
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonGatewayTracker implements Closeable, PythonGatewayCreationGateListener {

    /**
     * The public instance of the GatewayTracker. There can be only one!
     */
    public static final PythonGatewayTracker INSTANCE = new PythonGatewayTracker();

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonGatewayTracker.class);

    private final Set<TrackedPythonGateway<?>> m_openGateways;

    private PythonGatewayTracker() {
        m_openGateways = gatewaySet();
    }

    /**
     * Create a {@link PythonGateway} that is tracked and thus can be closed if needed.
     *
     * @param <EP> type of {@link PythonEntryPoint} used for this {@link PythonGateway}
     * @param gateway The {@link PythonGateway} to track
     * @return The tracked {@link PythonGateway}
     */
    public <EP extends PythonEntryPoint> PythonGateway<EP> createTrackedGateway(final PythonGateway<EP> gateway) {
        return new TrackedPythonGateway<>(gateway, this);
    }

    @Override
    public void onPythonKernelCreationGateOpen() {
        // Nothing to do
    }

    @Override
    public void onPythonKernelCreationGateClose() {
        try {
            close();
        } catch (IOException ex) {
            LOGGER.warn("Error when forcefully terminating Python process", ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (m_openGateways.isEmpty()) {
            return;
        }

        LOGGER.error("Found running Python processes. Aborting them to allow installation process. "
            + "If this leads to failures in node execution, please restart those nodes once the installation has finished");

        for (var gateway : m_openGateways) {
            // not using gateway.close() because that would modify m_openGateways during iteration
            gateway.m_delegate.close();
        }
        m_openGateways.clear();
    }

    private static class TrackedPythonGateway<EP extends PythonEntryPoint> implements PythonGateway<EP> {
        private final PythonGateway<EP> m_delegate;

        private final PythonGatewayTracker m_tracker;

        public TrackedPythonGateway(final PythonGateway<EP> delegate, final PythonGatewayTracker tracker) {
            m_delegate = delegate;
            m_tracker = tracker;
            m_tracker.add(this);
        }

        @Override
        public EP getEntryPoint() {
            return m_delegate.getEntryPoint();
        }

        @Override
        public InputStream getStandardOutputStream() {
            return m_delegate.getStandardOutputStream();
        }

        @Override
        public InputStream getStandardErrorStream() {
            return m_delegate.getStandardErrorStream();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
            m_tracker.remove(this);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<TrackedPythonGateway<?>> gatewaySet() {
        @SuppressWarnings("rawtypes")
        Map map = CacheBuilder.newBuilder().weakKeys().build().asMap();
        return Collections.newSetFromMap(map);
    }

    private <EP extends PythonEntryPoint> void remove(final TrackedPythonGateway<EP> gateway) {
        m_openGateways.remove(gateway);
    }

    private <EP extends PythonEntryPoint> void add(final TrackedPythonGateway<EP> gateway) {
        m_openGateways.add(gateway);
    }
}
