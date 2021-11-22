/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.generic;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntConsumer;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

/**
 * A panel to configure the various options of a scripting node. Intended to be used as a base class for more specific
 * options panels.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 *
 * @param <C> The associated {@link SourceCodeConfig}.
 */
@SuppressWarnings("serial") // Not intended for serialization.
public class SourceCodeOptionsPanel<C extends SourceCodeConfig> extends JPanel {

    private final JLabel m_rowLimitLabel = new JLabel("Row limit (dialog)");

    private final JSpinner m_rowLimit =
        new JSpinner(new SpinnerNumberModel(SourceCodeConfig.DEFAULT_ROW_LIMIT, 0, Integer.MAX_VALUE, 100));

    // Not intended for serialization.
    private final CopyOnWriteArrayList<IntConsumer> m_listeners = new CopyOnWriteArrayList<>(); // NOSONAR

    /**
     * Creates a scripting node's options panel.
     */
    public SourceCodeOptionsPanel() {
        super(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        add(m_rowLimitLabel, gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        add(m_rowLimit, gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 2;
        final JPanel additionalOptionsPanel = getAdditionalOptionsPanel();
        if (additionalOptionsPanel != null) {
            add(additionalOptionsPanel, gbc);
            gbc.gridy++;
        }
        gbc.weighty = Double.MIN_NORMAL;
        add(new JLabel(), gbc);

        m_rowLimit.addChangeListener(e -> onRowLimitChanged());
    }

    /**
     * Made final because the method is called from this class's constructor.
     * <P>
     * {@inheritDoc}
     */
    @Override
    public final void add(final Component comp, final Object constraints) {
        super.add(comp, constraints);
    }

    public int getRowLimit() {
        return (int)m_rowLimit.getValue();
    }

    public void addRowLimitChangeListener(final IntConsumer listener) {
        m_listeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    public void removeRowLimitChangeListener(final IntConsumer listener) {
        m_listeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onRowLimitChanged() {
        final int rowLimit = getRowLimit();
        for (IntConsumer listener : m_listeners) {
            listener.accept(rowLimit);
        }
    }

    /**
     * Load the panel's settings from the given config.
     *
     * @param config The config.
     */
    public void loadSettingsFrom(final C config) {
        m_rowLimit.setValue(config.getRowLimit());
    }

    /**
     * Save the panel's current settings into the given config.
     *
     * @param config The config.
     */
    public void saveSettingsTo(final C config) {
        config.setRowLimit(getRowLimit());
    }

    /**
     * @return The default implementation returns {@code null}.
     */
    protected JPanel getAdditionalOptionsPanel() {
        return null;
    }
}
