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
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python2.config.PythonCommandConfig;
import org.knime.python2.config.PythonExecutableSelectionPanel;
import org.knime.python2.config.PythonFixedVersionExecutableSelectionPanel;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class PythonBasedDataUnawareNodeDialog extends NodeDialogPane {

    private final List<PythonExecutableSelectionPanel> m_executableSelectionTabs = new ArrayList<>(1);

    protected final void addDefaultPythonExecutableSelectionTab(final PythonCommandConfig config) {
        addPythonExecutableSelectionTab(PythonExecutableSelectionPanel.DEFAULT_TAB_NAME, config);
    }

    protected final void addPythonExecutableSelectionTab(final String tabName, final PythonCommandConfig config) {
        final PythonExecutableSelectionPanel selectionTab =
            new PythonFixedVersionExecutableSelectionPanel(this, config);
        selectionTab.addChangeListener(e -> onPythonCommandChanged(config));
        m_executableSelectionTabs.add(selectionTab);
        addTab(tabName, selectionTab);
    }

    protected abstract void onPythonCommandChanged(PythonCommandConfig config);

    @Override
    protected final void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        for (final PythonExecutableSelectionPanel tab : m_executableSelectionTabs) {
            tab.loadSettingsFrom(settings);
        }
        loadSettingsFromDerived(settings, specs);
    }

    protected abstract void loadSettingsFromDerived(NodeSettingsRO settings, PortObjectSpec[] specs)
        throws NotConfigurableException;

    @Override
    protected final void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsToDerived(settings);
        for (final PythonExecutableSelectionPanel tab : m_executableSelectionTabs) {
            tab.saveSettingsTo(settings);
        }
    }

    protected abstract void saveSettingsToDerived(NodeSettingsWO settings) throws InvalidSettingsException;
}
