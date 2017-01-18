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
