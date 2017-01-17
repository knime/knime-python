/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
package org.knime.code2.generic;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * Panel containing additinal options to the {@link SourceCodePanel}.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class SourceCodeOptionsPanel extends JPanel {

	private static final long serialVersionUID = 526829042113254402L;

	private JLabel m_rowLimitLabel = new JLabel("Row limit (dialog)");
	private JSpinner m_rowLimit = new JSpinner(new SpinnerNumberModel(SourceCodeConfig.DEFAULT_ROW_LIMIT, 0,
			Integer.MAX_VALUE, 100));
	private SourceCodePanel m_sourceCodePanel;

	/**
	 * Create a source code options panel.
	 * 
	 * @param sourceCodePanel
	 *            The corresponding source code panel
	 */
	public SourceCodeOptionsPanel(final SourceCodePanel sourceCodePanel) {
		m_sourceCodePanel = sourceCodePanel;
		setLayout(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
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
		gbc.weighty = Double.MIN_NORMAL;
		gbc.gridy++;
		gbc.gridwidth = 2;
		add(new JLabel(), gbc);
		m_rowLimit.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				sourceCodePanel.setRowLimit((int) m_rowLimit.getValue());
			}
		});
	}

	/**
	 * Save current settings into the given config.
	 * 
	 * @param config
	 *            The config
	 */
	public void saveSettingsTo(final SourceCodeConfig config) {
		config.setRowLimit((int) m_rowLimit.getValue());
	}

	/**
	 * Load settings from the given config.
	 * 
	 * @param config
	 *            The config
	 */
	public void loadSettingsFrom(final SourceCodeConfig config) {
		m_rowLimit.setValue(config.getRowLimit());
		m_sourceCodePanel.setRowLimit(config.getRowLimit());
	}

}
