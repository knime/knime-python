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
 */

package org.knime.code2.python;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import org.knime.code2.generic.SourceCodeOptionsPanel;

public class PythonSourceCodeOptionsPanel extends SourceCodeOptionsPanel<PythonSourceCodePanel, PythonSourceCodeConfig> {
	
	private static final long serialVersionUID = -5612311503547573497L;
	private ButtonGroup m_pythonVersion;
	private JRadioButton m_python2;
	private JRadioButton m_python3;

	public PythonSourceCodeOptionsPanel(PythonSourceCodePanel sourceCodePanel) {
		super(sourceCodePanel);
	}
	
	@Override
	protected JPanel getAdditionalOptionsPanel() {
		m_pythonVersion = new ButtonGroup();
		m_python2 = new JRadioButton("Python 2");
		m_python3 = new JRadioButton("Python 3");
		m_pythonVersion.add(m_python2);
		m_pythonVersion.add(m_python3);
		ActionListener listener = new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				getSourceCodePanel().setUsePython3(m_python3.isSelected());
			}
		};
		m_python2.addActionListener(listener);
		m_python3.addActionListener(listener);
		JPanel panel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.fill = GridBagConstraints.BOTH;
		gbc.weightx = 0;
		gbc.weighty = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
		panel.add(m_python2, gbc);
		gbc.gridx++;
		panel.add(m_python3, gbc);
		return panel;
	}
	
	@Override
	public void loadSettingsFrom(PythonSourceCodeConfig config) {
		super.loadSettingsFrom(config);
		if (config.getUsePython3()) {
			m_python3.setSelected(true);
		} else {
			m_python2.setSelected(true);
		}
		getSourceCodePanel().setUsePython3(config.getUsePython3());
	}
	
	@Override
	public void saveSettingsTo(PythonSourceCodeConfig config) {
		super.saveSettingsTo(config);
		config.setUsePython3(m_python3.isSelected());
	}

}
