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
 *   Dec 1, 2020 (marcel): created
 */
package org.knime.python2.config;

import java.awt.CardLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeListener;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public final class PythonVersionAndExecutableSelectionPanel extends PythonExecutableSelectionPanel {

    private final PythonVersionAndCommandConfig m_config; // NOSONAR Not intended for serialization.

    private final PythonVersionSelectionPanel m_pythonVersionSelectionPanel;

    private final JPanel m_pythonEnvironmentSelectionsWrapper;

    private final PythonFixedVersionExecutableSelectionPanel m_python2ExecutableSelectionPanel;

    private final PythonFixedVersionExecutableSelectionPanel m_python3ExecutableSelectionPanel;

    // Sonar: not intended for serialization.
    private final List<ChangeListener> m_listeners = new CopyOnWriteArrayList<>(); // NOSONAR

    /**
     * @param dialog The hosting node dialog. Needed to create flow variable models for the Python-command configs
     *            exposed to the user by this instance.
     * @param config The configuration exposed to the user, and accordingly manipulated, by this instance.
     */
    public PythonVersionAndExecutableSelectionPanel(final NodeDialogPane dialog,
        final PythonVersionAndCommandConfig config) {
        setLayout(new GridBagLayout());

        m_config = config;

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;

        m_pythonVersionSelectionPanel = new PythonVersionSelectionPanel(m_config.getPythonVersionConfig());
        add(m_pythonVersionSelectionPanel, gbc);

        m_pythonEnvironmentSelectionsWrapper = new JPanel(new CardLayout());
        m_python2ExecutableSelectionPanel =
            new PythonFixedVersionExecutableSelectionPanel(dialog, m_config.getPython2CommandConfig());
        m_pythonEnvironmentSelectionsWrapper.add(m_python2ExecutableSelectionPanel, PythonVersion.PYTHON2.getId());
        m_python3ExecutableSelectionPanel =
            new PythonFixedVersionExecutableSelectionPanel(dialog, m_config.getPython3CommandConfig());
        m_pythonEnvironmentSelectionsWrapper.add(m_python3ExecutableSelectionPanel, PythonVersion.PYTHON3.getId());
        updateShownSelectionPanel();
        gbc.gridy++;
        add(m_pythonEnvironmentSelectionsWrapper, gbc);

        gbc.gridy++;
        gbc.weighty = 1;
        add(new JLabel(""), gbc);

        m_pythonVersionSelectionPanel.addChangeListener(e -> {
            updateShownSelectionPanel();
            notifyListenersIfCurrentPythonVersion(getPythonVersion());
        });
        m_python2ExecutableSelectionPanel
            .addChangeListener(e -> notifyListenersIfCurrentPythonVersion(PythonVersion.PYTHON2));
        m_python3ExecutableSelectionPanel
            .addChangeListener(e -> notifyListenersIfCurrentPythonVersion(PythonVersion.PYTHON3));
    }

    @Override
    public PythonVersion getPythonVersion() {
        return m_config.getPythonVersionConfig().getPythonVersion();
    }

    @Override
    public PythonCommand getPythonCommand() {
        return (getPythonVersion() == PythonVersion.PYTHON2 //
            ? m_config.getPython2CommandConfig() //
            : m_config.getPython3CommandConfig()).getCommand();
    }

    /**
     * @return The underlying configuration that is exposed to the user, and accordingly manipulated, by this instance.
     */
    public PythonVersionAndCommandConfig getConfig() {
        return m_config;
    }

    @Override
    public void addChangeListener(final ChangeListener listener) {
        m_listeners.add(listener); // NOSONAR Not performance critical.
    }

    @Override
    public void removeChangeListener(final ChangeListener listener) {
        m_listeners.remove(listener); // NOSONAR Not performance critical.
    }

    private void notifyListenersIfCurrentPythonVersion(final PythonVersion pythonVersion) {
        if (pythonVersion == getPythonVersion()) {
            m_listeners.forEach(l -> l.stateChanged(null));
        }
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        m_pythonVersionSelectionPanel.loadSettingsFrom(settings);
        m_python2ExecutableSelectionPanel.loadSettingsFrom(settings);
        m_python3ExecutableSelectionPanel.loadSettingsFrom(settings);
        updateShownSelectionPanel();
    }

    private void updateShownSelectionPanel() {
        ((CardLayout)m_pythonEnvironmentSelectionsWrapper.getLayout()).show(m_pythonEnvironmentSelectionsWrapper,
            m_config.getPythonVersionConfig().getPythonVersion().getId());
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_pythonVersionSelectionPanel.saveSettingsTo(settings);
        m_python2ExecutableSelectionPanel.saveSettingsTo(settings);
        m_python3ExecutableSelectionPanel.saveSettingsTo(settings);
    }
}
