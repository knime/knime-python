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
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.Pair;
import org.knime.core.util.Version;
import org.knime.python2.conda.Conda;
import org.knime.python2.conda.CondaPackageSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaPackagesTable {

    private static final String UNINITIALIZED = "uninitialized";

    private static final String REFRESHING = "refreshing";

    private static final String POPULATED = "populated";

    private static final String ERROR = "error";

    private final CondaPackagesConfig m_config;

    private final SettingsModelString m_environmentNameModel;

    private final Supplier<Conda> m_conda;

    private final JPanel m_panel = new JPanel(new CardLayout());

    private final JLabel m_refreshingLabel = new JLabel("Collecting packages...", SwingConstants.CENTER);

    private final TableModel m_model = new TableModel();

    @SuppressWarnings("serial") // Not intended for serialization.
    private final JTable m_table = new JTable(m_model) {

        @Override
        public Component prepareRenderer(final TableCellRenderer renderer, final int row, final int column) {
            final JComponent c = (JComponent)super.prepareRenderer(renderer, row, column);
            final TableModel.Entry entry = m_model.getEntryAt(row);
            if (entry.m_unconfigured) {
                c.setBackground(new Color(225, 237, 255));
            } else if (entry.m_removed) {
                c.setBackground(new Color(255, 242, 225));
            } else {
                c.setBackground(null);
            }
            return c;
        }
    };

    private final JTextArea m_removedLabel = new JTextArea();

    private final JTextArea m_unconfiguredLabel = new JTextArea();

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
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 3;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        final JScrollPane pane = new JScrollPane(m_table);
        final Dimension panePreferredSize = pane.getPreferredSize();
        panePreferredSize.width += 125;
        pane.setPreferredSize(panePreferredSize);
        panel.add(pane, gbc);

        gbc.gridy++;
        gbc.weighty = 0;
        gbc.insets = new Insets(3, 0, 2, 0);
        m_removedLabel.setEditable(false);
        m_removedLabel.setLineWrap(true);
        m_removedLabel.setWrapStyleWord(true);
        m_removedLabel.setForeground(new Color(154, 120, 0));
        m_removedLabel.setBackground(m_errorLabel.getBackground());
        m_removedLabel.setFont(m_errorLabel.getFont());
        m_removedLabel.setMinimumSize(new Dimension(panePreferredSize.width,
            m_removedLabel.getFontMetrics(m_removedLabel.getFont()).getHeight()));
        panel.add(m_removedLabel, gbc);

        gbc.gridy++;
        gbc.insets = new Insets(0, 0, 0, 0);
        m_unconfiguredLabel.setEditable(false);
        m_unconfiguredLabel.setLineWrap(true);
        m_unconfiguredLabel.setWrapStyleWord(true);
        m_unconfiguredLabel.setForeground(Color.BLUE);
        m_unconfiguredLabel.setBackground(m_errorLabel.getBackground());
        m_unconfiguredLabel.setFont(m_errorLabel.getFont());
        m_unconfiguredLabel.setMinimumSize(new Dimension(panePreferredSize.width,
            m_unconfiguredLabel.getFontMetrics(m_unconfiguredLabel.getFont()).getHeight() * 2));
        panel.add(m_unconfiguredLabel, gbc);

        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(5, 0, 5, 3);
        final JButton includeAll = new JButton("Include all");
        includeAll.addActionListener(e -> m_model.includeAllPackages());
        panel.add(includeAll, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(5, 3, 5, 3);
        final JButton excludeAll = new JButton("Exclude all");
        excludeAll.addActionListener(e -> m_model.excludeAllPackages());
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
        invokeOnEDT(() -> {
            m_removedLabel.setText("");
            m_removedLabel.setVisible(false);
            m_unconfiguredLabel.setText("");
            m_unconfiguredLabel.setVisible(false);
        });
        final Conda conda = m_conda.get();
        List<CondaPackageSpec> included = new ArrayList<>();
        List<CondaPackageSpec> excluded = new ArrayList<>();
        List<CondaPackageSpec> removed = new ArrayList<>();
        List<CondaPackageSpec> unconfigured = new ArrayList<>();
        try {
            final String environmentName = m_environmentNameModel.getStringValue();
            final List<CondaPackageSpec> packages = conda.getPackages(environmentName);
            if (isInitialRefresh) {
                categorizePackages(packages, m_config, included, excluded, removed, unconfigured);
            } else {
                included = packages;
            }
        } catch (final IOException ex) {
            if (isInitialRefresh) {
                // Retain the currently configured packages if the configured environment is not present on the local
                // machine. This way, the dialog can be opened on the target machine before the environment has been
                // recreated without overriding the current configuration.
                included = m_config.getIncludedPackages();
                excluded = m_config.getExcludedPackages();
                removed = Collections.emptyList();
                unconfigured = Collections.emptyList();
            } else {
                setToErrorView(ex);
                return;
            }
        }
        setPackages(included, excluded, removed, unconfigured);
        if (isInitialRefresh) {
            m_includeExplicit.setEnabled(conda.isPackageNamesFromHistoryAvailable());
        }
        invokeOnEDT(() -> setState(POPULATED));
    }

    private static void categorizePackages(final List<CondaPackageSpec> packages, final CondaPackagesConfig config,
        final List<CondaPackageSpec> included, final List<CondaPackageSpec> excluded,
        final List<CondaPackageSpec> removed, final List<CondaPackageSpec> unconfigured) {
        final Set<CondaPackageSpec> includedSet = new HashSet<>(config.getIncludedPackages());
        final Set<CondaPackageSpec> excludedSet = new HashSet<>(config.getExcludedPackages());
        for (final CondaPackageSpec pkg : packages) {
            if (includedSet.contains(pkg)) {
                included.add(pkg);
            } else if (excludedSet.contains(pkg)) {
                excluded.add(pkg);
            } else {
                unconfigured.add(pkg);
            }
        }
        final Set<CondaPackageSpec> packagesSet = new HashSet<>(packages);
        for (final CondaPackageSpec pkg : includedSet) {
            if (!packagesSet.contains(pkg)) {
                removed.add(pkg);
            }
        }
    }

    private void setPackages(final List<CondaPackageSpec> included, final List<CondaPackageSpec> excluded,
        final List<CondaPackageSpec> removed, final List<CondaPackageSpec> unconfigured) {
        if (!removed.isEmpty()) {
            invokeOnEDT(() -> {
                final int numPackages = removed.size();
                final boolean pl = numPackages > 1;
                m_removedLabel
                    .setText(numPackages + String.format(" %s configured for inclusion %s not present locally.",
                        pl ? "packages" : "package", pl ? "are" : "is"));
                m_removedLabel.setVisible(true);
            });
        }
        if (!unconfigured.isEmpty()) {
            invokeOnEDT(() -> {
                final int numPackages = unconfigured.size();
                final boolean pl = numPackages > 1;
                m_unconfiguredLabel.setText(numPackages
                    + String.format(" %s present locally %s not yet been configured for inclusion or exclusion.",
                        pl ? "packages" : "package", pl ? "have" : "has"));
                m_unconfiguredLabel.setVisible(true);
            });
        }
        invokeOnEDT(() -> m_model.setPackages(included, excluded, removed, unconfigured));
    }

    private void setToErrorView(final Exception ex) {
        invokeOnEDT(() -> {
            m_model.setPackages(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList());
            m_errorLabel.setText(ex.getMessage());
            setState(ERROR);
        });
        NodeLogger.getLogger(CondaEnvironmentPropagationNodeDialog.class).error(ex.getMessage(), ex);
    }

    private void includeExplicitPackages() {
        final Window dialog = SwingUtilities.getWindowAncestor(m_panel);
        dialog.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        try {
            final Set<String> explicitPackages =
                new HashSet<>(m_conda.get().getPackageNamesFromHistory(m_environmentNameModel.getStringValue()));
            m_model.setIncludedPackages(explicitPackages);
        } catch (final IOException ex) {
            setToErrorView(ex);
        } finally {
            dialog.setCursor(Cursor.getDefaultCursor());
        }
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        final Pair<List<CondaPackageSpec>, List<CondaPackageSpec>> p = m_model.getPackages();
        m_config.setPackages(p.getFirst(), p.getSecond());
        m_config.saveSettingsTo(settings);
    }

    @SuppressWarnings("serial") // Not intended for serialization.
    private static final class TableModel extends AbstractTableModel {

        private static final String[] COLUMN_NAMES = new String[]{"Include?", "Name", "Version", "Build", "Channel"};

        private List<Entry> m_packages = Collections.emptyList();

        @Override
        public int getRowCount() {
            return m_packages.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public String getColumnName(final int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public Class<?> getColumnClass(final int column) {
            if (column == 0) {
                return Boolean.class;
            } else {
                return String.class;
            }
        }

        @Override
        public boolean isCellEditable(final int row, final int column) {
            // Only allow to edit (toggle) checkbox, nothing else.
            return column == 0;
        }

        @Override
        public Object getValueAt(final int row, final int column) {
            final Entry entry = m_packages.get(row);
            final CondaPackageSpec pkg = entry.m_package;
            Object value = null;
            if (column == 0) {
                value = entry.m_included;
            } else if (column == 1) {
                value = pkg.getName();
            } else if (column == 2) {
                value = pkg.getVersion();
            } else if (column == 3) {
                value = pkg.getBuild();
            } else if (column == 4) {
                value = pkg.getChannel();
            }
            return value;
        }

        @Override
        public void setValueAt(final Object value, final int row, final int column) {
            if (column == 0) {
                getEntryAt(row).m_included = (boolean)value;
            }
        }

        public Entry getEntryAt(final int row) {
            return m_packages.get(row);
        }

        private void setPackages(final List<CondaPackageSpec> included, final List<CondaPackageSpec> excluded,
            final List<CondaPackageSpec> removed, final List<CondaPackageSpec> unconfigured) {
            final List<Entry> packages =
                new ArrayList<>(included.size() + excluded.size() + unconfigured.size() + removed.size());
            included.forEach(pkg -> packages.add(new Entry(true, pkg)));
            excluded.forEach(pkg -> packages.add(new Entry(false, pkg)));
            unconfigured.forEach(pkg -> {
                final Entry entry = new Entry(false, pkg);
                entry.m_unconfigured = true;
                packages.add(entry);
            });
            removed.forEach(pkg -> {
                final Entry entry = new Entry(true, pkg);
                entry.m_removed = true;
                packages.add(entry);
            });

            packages.sort(Comparator.<Entry, String> comparing(e -> e.m_package.getName()) //
                .thenComparing((e1, e2) -> {
                    final String v1 = e1.m_package.getVersion();
                    final String v2 = e2.m_package.getVersion();
                    try {
                        return new Version(v1).compareTo(new Version(v2));
                    } catch (final Exception ex) {
                        return v1.compareTo(v2);
                    }
                }) //
                .thenComparing(e -> e.m_package.getBuild()) //
                .thenComparing(e -> e.m_package.getChannel()));
            m_packages = packages;
            fireTableDataChanged();
        }

        private void includeAllPackages() {
            for (final Entry entry : m_packages) {
                entry.m_included = true;
            }
            fireTableDataChanged();
        }

        private void excludeAllPackages() {
            for (final Entry entry : m_packages) {
                entry.m_included = false;
            }
            fireTableDataChanged();
        }

        private void setIncludedPackages(final Collection<String> explicitPackages) {
            for (final Entry entry : m_packages) {
                entry.m_included = explicitPackages.contains(entry.m_package.getName());
            }
            fireTableDataChanged();
        }

        private Pair<List<CondaPackageSpec>, List<CondaPackageSpec>> getPackages() {
            final List<CondaPackageSpec> included = new ArrayList<>();
            final List<CondaPackageSpec> excluded = new ArrayList<>();
            for (final Entry entry : m_packages) {
                (entry.m_included ? included : excluded).add(entry.m_package);
            }
            return new Pair<>(included, excluded);
        }

        private static final class Entry {

            private boolean m_included;

            private boolean m_unconfigured;

            private boolean m_removed;

            private CondaPackageSpec m_package;

            public Entry(final boolean included, final CondaPackageSpec pkg) {
                m_included = included;
                m_package = pkg;
            }
        }
    }
}
