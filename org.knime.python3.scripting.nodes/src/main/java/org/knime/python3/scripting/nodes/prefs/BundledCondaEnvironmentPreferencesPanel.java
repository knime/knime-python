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
 *   2 Apr 2022 (Carsten Haubold): created
 */
package org.knime.python3.scripting.nodes.prefs;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

/**
 * The BundledCondaEnvironmentPreferencesPanel displays information about the bundled conda environment.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
final class BundledCondaEnvironmentPreferencesPanel
    extends AbstractPythonConfigPanel<BundledCondaEnvironmentConfig, Composite> {

    private static final String BUNDLED_ENV_DESCRIPTION = """
            KNIME Analytics Platform provides its own Python environment that can be used
            by the Python Script nodes. If you select this option, then all Python Script nodes
            that are configured to use the settings from the preference page will make use of this \
            bundled Python environment.


            This bundled Python environment can not be extended, if you need additional packages for your scripts,
            use the "Conda" option above to change the environment for all Python Script nodes or
            use the Conda Environment Propagation Node to set a conda environment for selected nodes
            """;

    /**
     * Create a panel that displays information about the bundled conda environment.
     *
     * @param config The BundledCondaEnvironmentConfig
     * @param parent The parent {@link Composite} in which the panel will add its UI elements
     */
    public BundledCondaEnvironmentPreferencesPanel(final BundledCondaEnvironmentConfig config, final Composite parent) {
        super(config, parent);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final var panel = new Composite(parent, SWT.NONE);
        panel.setLayout(new GridLayout());

        final var environmentSelectionLabel = new Label(panel, SWT.NONE);
        final var gridData = new GridData();
        environmentSelectionLabel.setLayoutData(gridData);
        environmentSelectionLabel.setText(BUNDLED_ENV_DESCRIPTION);

        return panel;
    }
}
