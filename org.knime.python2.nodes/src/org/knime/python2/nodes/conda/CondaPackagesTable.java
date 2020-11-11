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
 *   Nov 5, 2020 (marcel): created
 */
package org.knime.python2.nodes.conda;

import static org.knime.python2.nodes.conda.CondaEnvironmentPropagationNodeDialog.invokeOnEDT;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.function.Supplier;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda;
import org.knime.python2.CondaPackageSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaPackagesTable {

    private static final String UNINITIALIZED = "uninitialized";

    private static final String REFRESHING = "refreshing";

    private static final String POPULATED = "populated";

    private static final String[] COLUMN_NAMES = new String[]{"Include?", "Name", "Version", "Build", "Channel"};

    private static final String ERROR = "error";

    private final CondaPackagesConfig m_config;

    private final SettingsModelString m_environmentNameModel;

    private final Supplier<Conda> m_conda;

    private final JPanel m_panel = new JPanel(new CardLayout());

    private final JLabel m_refreshingLabel = new JLabel("Collecting packages...", SwingConstants.CENTER);

    @SuppressWarnings("serial") // Not intended for serialization.
    private final DefaultTableModel m_model = new DefaultTableModel(COLUMN_NAMES, 0) {

        @Override
        public boolean isCellEditable(final int row, final int column) {
            // Only allow to edit (toggle) checkbox, nothing else.
            return column == 0;
        }
    };

    @SuppressWarnings("serial") // Not intended for serialization.
    private final JTable m_table = new JTable(m_model) {

        @Override
        public Class<?> getColumnClass(final int column) {
            if (column == 0) {
                return Boolean.class;
            } else {
                return String.class;
            }
        }
    };

    private final JLabel m_errorLabel = new JLabel("", SwingConstants.CENTER);

    private volatile boolean m_allowsRefresh = false;

    public CondaPackagesTable(final CondaPackagesConfig config, final SettingsModelString environmentNameModel,
        final Supplier<Conda> conda) {
        m_config = config;
        m_environmentNameModel = environmentNameModel;
        m_conda = conda;

        m_panel.add(new JPanel(), UNINITIALIZED);
        m_panel.add(m_refreshingLabel, REFRESHING);
        m_panel.add(createTablePanel(), POPULATED);
        m_errorLabel.setForeground(Color.RED);
        m_panel.add(m_errorLabel, ERROR);
    }

    private JPanel createTablePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.ipadx = 0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 3;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        final JScrollPane pane = new JScrollPane(m_table);
        final Dimension panePreferredSize = pane.getPreferredSize();
        panePreferredSize.width += 100;
        pane.setPreferredSize(panePreferredSize);
        panel.add(pane, gbc);
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(5, 0, 5, 3);
        final JButton includeAll = new JButton("Include all");
        includeAll.addActionListener(e -> includeAllPackages());
        panel.add(includeAll, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(5, 3, 5, 3);
        final JButton excludeAll = new JButton("Exclude all");
        excludeAll.addActionListener(e -> excludeAllPackages());
        panel.add(excludeAll, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(5, 3, 5, 0);
        final JButton includeExplicit = new JButton("Include only explicitly installed");
        includeExplicit.addActionListener(e -> includeExplicitPackages());
        panel.add(includeExplicit, gbc);
        return panel;
    }

    public JComponent getComponent() {
        return m_panel;
    }

    public void setToUninitializedView() {
        setState(UNINITIALIZED);
    }

    private void setState(final String state) {
        m_allowsRefresh = POPULATED.equals(state) || ERROR.equals(state);
        ((CardLayout)m_panel.getLayout()).show(m_panel, state);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_config.loadSettingsFrom(settings);
        } catch (InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
    }

    public void initializePackages() {
        refreshPackages(true);
    }

    public boolean allowsRefresh() {
        return m_allowsRefresh;
    }

    public void refreshPackages() {
        if (m_allowsRefresh) {
            refreshPackages(false);
        }
    }

    private void refreshPackages(final boolean keepCurrentSelection) {
        try {
            invokeOnEDT(() -> setState(REFRESHING));
            final String environmentName = m_environmentNameModel.getStringValue();
            final List<CondaPackageSpec> packages = m_conda.get().getPackages(environmentName);
            invokeOnEDT(() -> setPackages(packages, keepCurrentSelection ? m_config.getPackages() : packages));
            invokeOnEDT(() -> setState(POPULATED));
        } catch (final IOException ex) {
            setToErrorView(ex);
        }
    }

    private void setPackages(final List<CondaPackageSpec> packages, final List<CondaPackageSpec> selectedPackages) {
        final Set<CondaPackageSpec> selectedPackagesSet = new HashSet<>(selectedPackages);
        final Object[][] dataVector = new Object[packages.size()][];
        for (int i = 0; i < packages.size(); i++) {
            final CondaPackageSpec pkg = packages.get(i);
            final Object[] entry = new Object[5];
            entry[0] = selectedPackagesSet.contains(pkg);
            entry[1] = pkg.getName();
            entry[2] = pkg.getVersion();
            entry[3] = pkg.getBuild();
            entry[4] = pkg.getChannel();
            dataVector[i] = entry;
        }
        m_model.setDataVector(dataVector, COLUMN_NAMES);
    }

    private void includeAllPackages() {
        @SuppressWarnings("unchecked")
        final Vector<Vector<Object>> dataVector = m_model.getDataVector();
        for (final Vector<Object> entry : dataVector) {
            entry.set(0, Boolean.TRUE);
        }
        m_model.fireTableDataChanged();
    }

    private void excludeAllPackages() {
        @SuppressWarnings("unchecked")
        final Vector<Vector<Object>> dataVector = m_model.getDataVector();
        for (final Vector<Object> entry : dataVector) {
            entry.set(0, Boolean.FALSE);
        }
        m_model.fireTableDataChanged();
    }

    private void includeExplicitPackages() {
        final Window dialog = SwingUtilities.getWindowAncestor(m_panel);
        dialog.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        try {
            final Set<String> explicitPackages =
                new HashSet<>(m_conda.get().getPackageNamesFromHistory(m_environmentNameModel.getStringValue()));
            @SuppressWarnings("unchecked")
            final Vector<Vector<Object>> dataVector = m_model.getDataVector();
            for (final Vector<Object> entry : dataVector) {
                final String name = (String)entry.get(1);
                entry.set(0, explicitPackages.contains(name));
            }
            m_model.fireTableDataChanged();
        } catch (final IOException ex) {
            setToErrorView(ex);
        } finally {
            dialog.setCursor(Cursor.getDefaultCursor());
        }
    }

    private void setToErrorView(final Exception ex) {
        invokeOnEDT(() -> {
            setPackages(Collections.emptyList(), Collections.emptyList());
            m_errorLabel.setText(ex.getMessage());
            setState(ERROR);
        });
        NodeLogger.getLogger(CondaEnvironmentPropagationNodeDialog.class).error(ex.getMessage(), ex);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.setPackages(getSelectedPackages());
        m_config.saveSettingsTo(settings);
    }

    private List<CondaPackageSpec> getSelectedPackages() {
        @SuppressWarnings("unchecked")
        final Vector<Vector<?>> dataVector = m_model.getDataVector();
        final List<CondaPackageSpec> packages = new ArrayList<>(dataVector.size());
        for (final Vector<?> entry : dataVector) {
            if ((boolean)entry.get(0)) {
                final String name = (String)entry.get(1);
                final String version = (String)entry.get(2);
                final String build = (String)entry.get(3);
                final String channel = (String)entry.get(4);
                packages.add(new CondaPackageSpec(name, version, build, channel));
            }
        }
        return packages;
    }
}
