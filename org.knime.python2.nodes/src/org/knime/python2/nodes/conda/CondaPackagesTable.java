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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import org.knime.python2.conda.Conda;
import org.knime.python2.conda.CondaPackageSpec;

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

    private /* final **/ JButton m_includeExplicit;

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
        m_includeExplicit = new JButton("Include only explicitly installed");
        m_includeExplicit.addActionListener(e -> includeExplicitPackages());
        panel.add(m_includeExplicit, gbc);
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

    private void refreshPackages(final boolean isInitialRefresh) {
        invokeOnEDT(() -> setState(REFRESHING));
        final Conda conda = m_conda.get();
        final String environmentName = m_environmentNameModel.getStringValue();
        List<CondaPackageSpec> included;
        List<CondaPackageSpec> excluded;
        try {
            final List<CondaPackageSpec> packages = conda.getPackages(environmentName);
            if (isInitialRefresh) {
                final Set<CondaPackageSpec> includedSet = new HashSet<>(m_config.getIncludedPackages());
                included = new ArrayList<>();
                excluded = new ArrayList<>();
                for (final CondaPackageSpec pkg : packages) {
                    (includedSet.contains(pkg) ? included : excluded).add(pkg);
                }
            } else {
                included = packages;
                excluded = Collections.emptyList();
            }
        } catch (final IOException ex) {
            if (isInitialRefresh) {
                // Retain the currently configured packages if the configured environment is not present on the local
                // machine. This way, the dialog can be opened on the target machine before the environment has been
                // recreated without overriding the current configuration.
                included = m_config.getIncludedPackages();
                excluded = m_config.getExcludedPackages();
            } else {
                setToErrorView(ex);
                return;
            }
        }
        final List<CondaPackageSpec> includedFinal = included;
        final List<CondaPackageSpec> excludedFinal = excluded;
        invokeOnEDT(() -> setPackages(includedFinal, excludedFinal));
        if (isInitialRefresh) {
            m_includeExplicit.setEnabled(conda.isPackageNamesFromHistoryAvailable());
        }
        invokeOnEDT(() -> setState(POPULATED));
    }

    private void setPackages(final List<CondaPackageSpec> included, final List<CondaPackageSpec> excluded) {
        final Object[][] dataVector = new Object[included.size() + excluded.size()][];
        addPackages(dataVector, 0, included, true);
        addPackages(dataVector, included.size(), excluded, false);
        Arrays.sort(dataVector, Comparator.comparing( //
            entry -> entry[1].toString() //
                + entry[2].toString() //
                + entry[3].toString() //
                + entry[4].toString()));
        m_model.setDataVector(dataVector, COLUMN_NAMES);
    }

    private static void addPackages(final Object[][] dataVector, final int startIndex,
        final List<CondaPackageSpec> packages, final boolean included) {
        for (int i = 0; i < packages.size(); i++) {
            final CondaPackageSpec pkg = packages.get(i);
            final Object[] entry = new Object[5];
            entry[0] = included;
            entry[1] = pkg.getName();
            entry[2] = pkg.getVersion();
            entry[3] = pkg.getBuild();
            entry[4] = pkg.getChannel();
            dataVector[startIndex + i] = entry;
        }
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
        final Pair<List<CondaPackageSpec>, List<CondaPackageSpec>> p = getPackages();
        m_config.setPackages(p.getFirst(), p.getSecond());
        m_config.saveSettingsTo(settings);
    }

    private Pair<List<CondaPackageSpec>, List<CondaPackageSpec>> getPackages() {
        @SuppressWarnings("unchecked")
        final Vector<Vector<?>> dataVector = m_model.getDataVector();
        final List<CondaPackageSpec> included = new ArrayList<>();
        final List<CondaPackageSpec> excluded = new ArrayList<>();
        for (final Vector<?> entry : dataVector) {
            final String name = (String)entry.get(1);
            final String version = (String)entry.get(2);
            final String build = (String)entry.get(3);
            final String channel = (String)entry.get(4);
            final CondaPackageSpec pkg = new CondaPackageSpec(name, version, build, channel);
            ((boolean)entry.get(0) ? included : excluded).add(pkg);
        }
        return new Pair<>(included, excluded);
    }
}
