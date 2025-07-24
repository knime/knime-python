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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.dataservice.DefaultDialogDataConverter;
import org.knime.core.webui.node.dialog.defaultdialog.jsonforms.JsonFormsConsts;
import org.knime.core.webui.node.dialog.defaultdialog.jsonforms.JsonFormsDataUtil;
import org.knime.core.webui.node.dialog.defaultdialog.jsonforms.JsonNodeSettingsMapperUtil;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.python3.nodes.proxy.NodeDialogProxy;
import org.knime.python3.nodes.settings.JsonNodeSettingsSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Delegates methods to a proxy object that can e.g. be implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class DelegatingJsonSettingsDataService implements NodeSettingsService, DefaultDialogDataConverter {

    private final Supplier<NodeDialogProxy> m_proxyProvider;

    private JsonNodeSettingsSchema m_lastSettingsSchema;

    private String m_extensionVersion;

    /**
     * Constructor.
     *
     * @param proxyProvider provides proxy objects
     * @param extensionVersion the version of the extension
     */
    public DelegatingJsonSettingsDataService(final Supplier<NodeDialogProxy> proxyProvider,
        final String extensionVersion) {
        m_proxyProvider = proxyProvider;
        m_extensionVersion = extensionVersion;
    }

    @Override
    public String fromNodeSettings(final Map<SettingsType, NodeAndVariableSettingsRO> settings,
        final PortObjectSpec[] specs) {
        try (var proxy = m_proxyProvider.get()) {

            // this is assigned here to accommodate changing the set of parameters during development
            var modelSettings = settings.get(SettingsType.MODEL);
            // load the old settings with the old schema
            var jsonSettings = proxy.getSettingsSchema(JsonNodeSettingsSchema.readVersion(modelSettings)).createFromSettings(modelSettings);
            // keep the current settings schema for saving the settings when the dialog is closed
            m_lastSettingsSchema = proxy.getSettingsSchema(m_extensionVersion);
            // the Python side needs the version of the settings to properly load the settings
            // the schema extraction then uses the current version of the extension which is already known on the Python side
            return proxy.getDialogRepresentation(jsonSettings, specs, jsonSettings.getCreationVersion());
        }
    }

    @Override
    public void toNodeSettings(final String jsonSettings,
        final Map<SettingsType, NodeAndVariableSettingsRO> previousSettings,
        final Map<SettingsType, NodeAndVariableSettingsWO> settings) {
        // the jsonSettings received from the frontend are wrapped into a 'data' object
        var unwrapped = JsonNodeSettingsMapperUtil.getNestedJsonObject(jsonSettings, JsonFormsConsts.FIELD_NAME_DATA);
        m_lastSettingsSchema.createFromJson(unwrapped).saveTo(settings.get(SettingsType.MODEL));
    }

    @Override
    public NodeSettings dataJsonToNodeSettings(final JsonNode dataJson, final SettingsType type) {
        var jsonSettings = m_lastSettingsSchema.createFromJson(dataJson.toString());
        var settings = new NodeSettings(type.getConfigKey());
        jsonSettings.saveTo(settings);
        return settings;
    }

    @Override
    public JsonNode nodeSettingsToDataJson(final SettingsType type, final NodeSettingsRO nodeSettings,
        final NodeParametersInput context) throws InvalidSettingsException {
        var jsonSettings = m_lastSettingsSchema.createFromSettings(nodeSettings);
        var params = jsonSettings.getParameters();

        try {
            return JsonFormsDataUtil.getMapper().readTree(params);
        } catch (JsonProcessingException ex) {
            // NB: This cannot happen because params is valid JSON
            throw new IllegalStateException("failed to parse json parameters", ex);
        }
    }
}
