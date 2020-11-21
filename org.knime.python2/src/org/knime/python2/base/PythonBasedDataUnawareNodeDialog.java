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
 *   Nov 20, 2020 (marcel): created
 */
package org.knime.python2.base;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python2.PythonCommand;
import org.knime.python2.config.PythonCommandFlowVariableConfig;
import org.knime.python2.config.PythonCommandFlowVariableModel;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class PythonBasedDataUnawareNodeDialog extends NodeDialogPane {

    private final Map<PythonCommandFlowVariableConfig, Supplier<PythonCommand>> m_configs = new LinkedHashMap<>(1);

    private final List<PythonCommandFlowVariableModel> m_models = new ArrayList<>(1);

    /**
     * May only be constructed during construction of the node dialog.
     */
    protected final void addPythonCommandConfig(final PythonCommandFlowVariableConfig config,
        final Supplier<PythonCommand> fallback) {
        m_configs.put(config, fallback);
        final PythonCommandFlowVariableModel model = new PythonCommandFlowVariableModel(this, config);
        m_models.add(model);
    }

    protected final PythonCommand getConfiguredPythonCommand(final PythonCommandFlowVariableConfig config) {
        return config.getCommand().orElseGet(m_configs.get(config));
    }

    @Override
    protected final void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        for (final PythonCommandFlowVariableModel model : m_models) {
            model.loadSettingsFrom(settings);
        }
        loadSettingsFromDerived(settings, specs);
    }

    protected abstract void loadSettingsFromDerived(NodeSettingsRO settings, PortObjectSpec[] specs)
        throws NotConfigurableException;

    @Override
    protected final void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsToDerived(settings);
        for (final PythonCommandFlowVariableModel model : m_models) {
            model.saveSettingsTo(settings);
        }
    }

    protected abstract void saveSettingsToDerived(NodeSettingsWO settings) throws InvalidSettingsException;

    @Override
    public final void onOpen() {
        m_models.forEach(PythonCommandFlowVariableModel::onDialogOpen);
        onOpenDerived();
    }

    public void onOpenDerived() {
        // Do nothing by default.
    }
}
