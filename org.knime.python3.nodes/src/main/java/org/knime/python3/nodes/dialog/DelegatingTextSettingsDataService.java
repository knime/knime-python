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
 *   Feb 17, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.dialog;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.TextNodeSettingsService;
import org.knime.python3.nodes.JsonNodeSettings;
import org.knime.python3.nodes.proxy.CloseableNodeDialogProxy;
import org.knime.python3.nodes.proxy.NodeDialogProxy;

/**
 * Delegates the {@link TextSettingsDataService#getInitialData(Map, PortObjectSpec[])} method to a
 * {@link NodeDialogProxy} that can e.g. be implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingTextSettingsDataService implements TextNodeSettingsService {

    private final Supplier<CloseableNodeDialogProxy> m_proxyProvider;

    private String m_schema;

    /**
     * Constructor.
     *
     * @param proxyProvider provides proxy objects
     */
    public DelegatingTextSettingsDataService(final Supplier<CloseableNodeDialogProxy> proxyProvider) {
        m_proxyProvider = proxyProvider;
    }

    @Override
    public String fromNodeSettings(final Map<SettingsType, NodeSettingsRO> settings, final PortObjectSpec[] specs) {
        try (var proxy = m_proxyProvider.get()) {
            // TODO support model and view settings?
            if (m_schema == null) {
                m_schema = proxy.getSchema();
            }
            // FIXME: Ask Martin if this is desired behavior
            var specsWithoutFlowVars = Stream.of(specs).skip(1).toArray(PortObjectSpec[]::new);
            var parameters = new JsonNodeSettings(settings.get(SettingsType.MODEL), m_schema);
            var rep = proxy.getDialogRepresentation(parameters.getParameters(), parameters.getCreationVersion(),
                specsWithoutFlowVars);
            return rep;
        }
    }

    @Override
    public void toNodeSettings(final String textSettings, final Map<SettingsType, NodeSettingsWO> settings) {
        if (m_schema == null) {
            try (var proxy = m_proxyProvider.get()) {
                m_schema = proxy.getSchema();
            }
        }
        // TODO support model and view settings?
        var jsonSettings = new JsonNodeSettings(textSettings, m_schema);
        jsonSettings.saveTo(settings.get(SettingsType.MODEL));
    }

    @Override
    public void getDefaultNodeSettings(final Map<SettingsType, NodeSettingsWO> settings, final PortObjectSpec[] specs) {
        try (var proxy = m_proxyProvider.get()) {
            var parameters = proxy.getParameters();
            if (m_schema == null) {
                m_schema = proxy.getSchema();
            }
            var jsonSettings = new JsonNodeSettings(parameters, m_schema);
            jsonSettings.saveTo(settings.get(SettingsType.MODEL));
        }
    }

}
