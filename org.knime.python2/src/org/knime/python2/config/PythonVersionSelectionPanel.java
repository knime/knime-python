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
 *   Jun 7, 2021 (marcel): created
 */
package org.knime.python2.config;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.text.JTextComponent;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.python2.PythonVersion;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public final class PythonVersionSelectionPanel extends JPanel {

    private final PythonVersionNodeConfig m_config; // NOSONAR Not intended for serialization.

    private final JRadioButton m_python2Button = new JRadioButton("Python 2");

    private final JRadioButton m_python3Button = new JRadioButton("Python 3");

    private final JTextComponent m_python3BackendInfoText =
        PythonKernelBackendSelectionPanel.createBackendInfoText("The new table API only supports Python 3.");

    // Sonar: Not intended for serialization.
    private final List<Consumer<PythonVersion>> m_listeners = new CopyOnWriteArrayList<>(); // NOSONAR

    /**
     * @param config The configuration exposed to the user, and accordingly manipulated, by this instance.
     * @param optionsPanel The dialog's options panel to whose back end change events the Python version selection in
     *            this panel subscribes. (The new back end does not support Python 2.)
     */
    public PythonVersionSelectionPanel(final PythonVersionNodeConfig config,
        final PythonSourceCodeOptionsPanel optionsPanel) {
        super(new GridBagLayout());

        m_config = config;

        final GridBagConstraints gbc = createDefaultGbc();
        final JPanel versionPanel = createVersionPanel();
        add(versionPanel, gbc);

        updateView();

        m_python2Button.addActionListener(e -> updateConfigAndNotifyListeners(PythonVersion.PYTHON2));
        m_python3Button.addActionListener(e -> updateConfigAndNotifyListeners(PythonVersion.PYTHON3));

        optionsPanel.addKernelBackendChangeListener(backendType -> {
            final boolean isPython2Backend = backendType == PythonKernelBackendType.PYTHON2;
            PythonSourceCodeOptionsPanel.setEnabledRecursively(versionPanel, isPython2Backend);
            m_python3BackendInfoText.setEnabled(!isPython2Backend);
            m_python3BackendInfoText.setVisible(!isPython2Backend);
        });
    }

    private static GridBagConstraints createDefaultGbc() {
        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        return gbc;
    }

    private JPanel createVersionPanel() {
        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Use Python version"));
        final var gbc = createDefaultGbc();

        final var pythonVersionButtonGroup = new ButtonGroup();
        pythonVersionButtonGroup.add(m_python2Button);
        panel.add(m_python2Button, gbc);
        pythonVersionButtonGroup.add(m_python3Button);
        gbc.gridx++;
        panel.add(m_python3Button, gbc);
        m_python3BackendInfoText.setVisible(false);
        gbc.gridx--;
        gbc.gridy++;
        gbc.gridwidth = 2;
        panel.add(m_python3BackendInfoText, gbc);
        return panel;
    }

    /**
     * The listener will be notified if the Python version selected via this panel has changed.
     *
     * @param listener The listener.
     */
    public void addChangeListener(final Consumer<PythonVersion> listener) {
        m_listeners.add(listener); // NOSONAR Not performance critical.
    }

    /**
     * Removes the given listener.
     *
     * @param listener The listener.
     */
    public void removeChangeListener(final Consumer<PythonVersion> listener) {
        m_listeners.remove(listener); // NOSONAR Not performance critical.
    }

    private void updateConfigAndNotifyListeners(final PythonVersion pythonVersion) {
        final PythonVersion oldPythonVersion = m_config.getPythonVersion();
        m_config.setPythonVersion(pythonVersion);
        if (pythonVersion != oldPythonVersion) {
            m_listeners.forEach(l -> l.accept(pythonVersion));
        }
    }

    private void updateView() {
        final PythonVersion pythonVersion = m_config.getPythonVersion();
        m_python2Button.setSelected(pythonVersion == PythonVersion.PYTHON2);
        m_python3Button.setSelected(pythonVersion == PythonVersion.PYTHON3);
    }

    /**
     * Load this panel's configuration from the given settings.
     *
     * @param settings The settings.
     * @throws NotConfigurableException If loading the configuration failed.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_config.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
        updateView();
    }

    /**
     * Save this panel's configuration to the given settings.
     *
     * @param settings The setting.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.saveSettingsTo(settings);
    }
}
