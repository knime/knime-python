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
package org.knime.code2.generic.templates;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.knime.code2.generic.SourceCodePanel;

//TODO comment

public class SourceCodeTemplatesPanel extends JPanel {

	private static final long serialVersionUID = -9069745257910008324L;

	private SourceCodeTemplateRepository m_repository;
	private SourceCodePanel m_sourceCodePanel;
	private RSyntaxTextArea m_editor;
	private JButton m_create;
	private JButton m_apply;
	private JButton m_remove;
	private JComboBox<String> m_category;
	private DefaultComboBoxModel<String> m_categoryModel;
	private JList<SourceCodeTemplate> m_template;
	private JTextArea m_description;
	private DefaultListModel<SourceCodeTemplate> m_templateModel;

	public SourceCodeTemplatesPanel(final SourceCodePanel sourceCodePanel,
			final String repositoryId) {
		m_sourceCodePanel = sourceCodePanel;
		m_repository = new SourceCodeTemplateRepository(repositoryId);
		m_editor = SourceCodePanel.createEditor(m_sourceCodePanel.getEditor()
				.getSyntaxEditingStyle());
		m_editor.setEditable(false);
		m_create = new JButton("Create from editor...");
		m_apply = new JButton("Apply selected");
		m_remove = new JButton("Remove selected");
		m_categoryModel = new DefaultComboBoxModel<String>();
		m_category = new JComboBox<String>(m_categoryModel);
		m_templateModel = new DefaultListModel<SourceCodeTemplate>();
		m_template = new JList<SourceCodeTemplate>(m_templateModel);
		m_template.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		m_description = new JTextArea();
		m_description.setEditable(false);
		m_description.setLineWrap(true);
		m_description.setWrapStyleWord(true);
		// Layout
		JPanel leftPanel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.fill = GridBagConstraints.BOTH;
		gbc.weightx = 1;
		gbc.weighty = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
		leftPanel.add(m_category, gbc);
		gbc.gridy++;
		gbc.weighty = 0.6;
		leftPanel.add(new JScrollPane(m_template), gbc);
		gbc.gridy++;
		gbc.weighty = 0.4;
		leftPanel.add(new JScrollPane(m_description), gbc);
		JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
		split.setResizeWeight(0.2);
		split.setOneTouchExpandable(true);
		split.setDividerSize(8);
		split.setDividerLocation(200);
		split.setLeftComponent(leftPanel);
		RTextScrollPane editorScrollPane = new RTextScrollPane(m_editor);
		editorScrollPane.setFoldIndicatorEnabled(true);
		split.setRightComponent(editorScrollPane);
		setLayout(new BorderLayout());
		add(split, BorderLayout.CENTER);
		JPanel editorButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
		int maxWidth = Math.max(
				Math.max(m_create.getPreferredSize().width,
						m_apply.getPreferredSize().width),
				m_remove.getPreferredSize().width);
		int maxHeight = Math.max(
				Math.max(m_create.getPreferredSize().height,
						m_apply.getPreferredSize().height),
				m_remove.getPreferredSize().height);
		Dimension dimension = new Dimension(maxWidth, maxHeight);
		m_create.setPreferredSize(dimension);
		m_apply.setPreferredSize(dimension);
		m_remove.setPreferredSize(dimension);
		editorButtons.add(m_create);
		editorButtons.add(m_remove);
		editorButtons.add(m_apply);
		add(editorButtons, BorderLayout.SOUTH);
		// Add listeners
		m_create.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				openCreateDialog();
			}
		});
		m_apply.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				m_sourceCodePanel.getEditor().setText(
						m_template.getSelectedValue().getSourceCode());
			}
		});
		m_remove.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				m_repository.removeTemplate(m_template.getSelectedValue());
				refreshCategories();
			}
		});
		m_category.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				refreshTemplates();
			}
		});
		m_template.addListSelectionListener(new ListSelectionListener() {
			@Override
			public void valueChanged(ListSelectionEvent e) {
				refreshTemplateInfo();
			}
		});
		refreshCategories();
	}

	private void refreshCategories() {
		String selected = (String) m_category.getSelectedItem();
		m_categoryModel.removeAllElements();
		Set<String> categories = m_repository.getCategories();
		for (String category : categories) {
			m_categoryModel.addElement(category);
		}
		if (selected != null && categories.contains(selected)) {
			m_category.setSelectedItem(selected);
		} else {
			m_category.setSelectedIndex(m_categoryModel.getSize() < 1 ? -1 : 0);
		}
		refreshTemplates();
	}

	private void refreshTemplates() {
		SourceCodeTemplate selected = m_template.getSelectedValue();
		m_templateModel.removeAllElements();
		String category = (String) m_category.getSelectedItem();
		Set<SourceCodeTemplate> templates = m_repository
				.getTemplatesForCategory(category);
		for (SourceCodeTemplate template : templates) {
			m_templateModel.addElement(template);
		}
		if (selected != null && templates.contains(selected)) {
			m_template.setSelectedValue(selected, true);
		} else {
			m_template.setSelectedIndex(0);
		}
		refreshTemplateInfo();
	}

	private void refreshTemplateInfo() {
		SourceCodeTemplate template = m_template.getSelectedValue();
		if (template != null) {
			m_description.setText(template.getDescription());
			m_editor.setText(template.getSourceCode());
		} else {
			m_description.setText("");
			m_editor.setText("");
		}
		m_apply.setEnabled(template != null);
		m_remove.setEnabled(template != null && !template.isPredefined());
	}

	private void openCreateDialog() {
		Frame f = null;
		Container c = getParent();
		while (c != null) {
			if (c instanceof Frame) {
				f = (Frame) c;
				break;
			}
			c = c.getParent();
		}
		final JDialog dialog = new JDialog(f);
		final AtomicBoolean apply = new AtomicBoolean(false);
		JComboBox<String> category = new JComboBox<String>(m_repository
				.getCategories().toArray(new String[0]));
		category.setEditable(true);
		JTextField title = new JTextField();
		title.setColumns(30);
		JTextArea description = new JTextArea();
		description.setBorder(new EtchedBorder());
		description.setColumns(30);
		description.setRows(10);
		description.setLineWrap(true);
		description.setWrapStyleWord(true);
		JButton ok = new JButton("OK");
		JButton cancel = new JButton("Cancel");
		Insets buttonMargin = new Insets(0, 0, 0, 0);
		ok.setMargin(buttonMargin);
		cancel.setMargin(buttonMargin);
		dialog.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(final KeyEvent e) {
				if (e.getKeyChar() == KeyEvent.VK_ESCAPE) {
					dialog.setVisible(false);
				}
			}
		});
		title.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(final KeyEvent e) {
				if (e.getKeyChar() == '\n') {
					apply.set(true);
					dialog.setVisible(false);
				} else if (e.getKeyChar() == KeyEvent.VK_ESCAPE) {
					dialog.setVisible(false);
				}
			}
		});
		ok.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				apply.set(true);
				dialog.setVisible(false);
			}
		});
		cancel.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				dialog.setVisible(false);
			}
		});
		ok.setPreferredSize(cancel.getPreferredSize());
		dialog.setLayout(new GridBagLayout());
		dialog.setTitle("Create template from editor...");
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.fill = GridBagConstraints.BOTH;
		gbc.weightx = 1;
		gbc.weighty = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.gridwidth = 3;
		gbc.insets = new Insets(5, 5, 0, 5);
		dialog.add(new JLabel("Category:"), gbc);
		gbc.gridy++;
		gbc.insets = new Insets(5, 5, 5, 5);
		dialog.add(category, gbc);
		gbc.gridy++;
		gbc.insets = new Insets(5, 5, 0, 5);
		dialog.add(new JLabel("Title:"), gbc);
		gbc.gridy++;
		gbc.insets = new Insets(5, 5, 5, 5);
		dialog.add(title, gbc);
		gbc.gridy++;
		gbc.insets = new Insets(5, 5, 0, 5);
		dialog.add(new JLabel("Description:"), gbc);
		gbc.gridy++;
		gbc.weighty = 1;
		gbc.insets = new Insets(5, 5, 5, 5);
		dialog.add(new JScrollPane(description), gbc);
		gbc.weighty = 0;
		gbc.gridwidth = 1;
		gbc.anchor = GridBagConstraints.SOUTHEAST;
		gbc.gridwidth = 1;
		gbc.gridy++;
		dialog.add(new JLabel(), gbc);
		gbc.weightx = 0;
		gbc.fill = GridBagConstraints.NONE;
		gbc.gridx++;
		dialog.add(ok, gbc);
		gbc.gridx++;
		dialog.add(cancel, gbc);
		dialog.pack();
		dialog.setLocationRelativeTo(this);
		dialog.setModal(true);
		dialog.setVisible(true);
		// Continues here after dialog is closed
		if (apply.get()) {
			try {
				m_repository.createTemplate(
						(String) category.getSelectedItem(), title.getText(),
						description.getText(), m_sourceCodePanel.getEditor()
								.getText());
				refreshCategories();
			} catch (IOException | IllegalArgumentException e) {
				JOptionPane.showMessageDialog(this, e.getMessage(),
						"Could not create template", JOptionPane.ERROR_MESSAGE);
			}
		}
		dialog.dispose();
	}

}
