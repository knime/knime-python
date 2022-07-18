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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.JsonNodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.python3.nodes.proxy.NodeDialogProxy;
import org.knime.python3.nodes.settings.JsonNodeSettings;

/**
 * Delegates methods to a proxy object that can e.g. be implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingJsonSettingsDataService implements JsonNodeSettingsService<String> {

    private final Supplier<NodeDialogProxy> m_proxyProvider;

    private JsonNodeSettings m_lastSettings;

    /**
     * Constructor.
     *
     * @param proxyProvider provides proxy objects
     */
    public DelegatingJsonSettingsDataService(final Supplier<NodeDialogProxy> proxyProvider) {
        m_proxyProvider = proxyProvider;
    }

    @Override
    public String fromNodeSettingsToObject(final Map<SettingsType, NodeSettingsRO> settings, final PortObjectSpec[] specs) {
        try (var proxy = m_proxyProvider.get()) {
            var specsWithoutFlowVars = Stream.of(specs).skip(1).toArray(PortObjectSpec[]::new);
            m_lastSettings = proxy.getSettings();
            var jsonSettings = loadSettings(settings.get(SettingsType.MODEL));
            return proxy.getDialogRepresentation(jsonSettings, specsWithoutFlowVars);
        }
    }

    private JsonNodeSettings loadSettings(final NodeSettingsRO settings) {
        try {
            return m_lastSettings.createFromSettings(settings);
        } catch (InvalidSettingsException ex) {
            throw new IllegalArgumentException("The provided settings are invalid.", ex);
        }
    }

    @Override
    public void toNodeSettingsFromObject(final String jsonSettings, final Map<SettingsType, NodeSettingsWO> settings) {
        m_lastSettings.createFromJson(jsonSettings).saveTo(settings.get(SettingsType.MODEL));
    }


    @Override
    public void getDefaultNodeSettings(final Map<SettingsType, NodeSettingsWO> settings, final PortObjectSpec[] specs) {
        try (var proxy = m_proxyProvider.get()) {
            proxy.getSettings().saveTo(settings.get(SettingsType.MODEL));
        }
    }

    @Override
    public String fromJson(final String json) {
        return json;
    }

    @Override
    public String toJson(final String obj) {
        return obj;
    }
}
