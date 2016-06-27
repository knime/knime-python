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
package org.knime.code.generic;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.border.BevelBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.table.DefaultTableModel;
import javax.swing.text.BadLocationException;
import javax.swing.text.JTextComponent;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.resources.ResourcesPlugin;
import org.fife.ui.autocomplete.AutoCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.autocomplete.DefaultCompletionProvider;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.spell.SpellingParser;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.DataColumnSpecListCellRenderer;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python.Activator;

/**
 * Abstract source code panel as basis for source code panels for a specific
 * programming language.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
abstract public class SourceCodePanel extends JPanel {

	/**
	 * Represents a variable as it is displayed in the variables table.
	 * 
	 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
	 */
	public static class Variable {

		private String m_name;
		private String m_type;
		private String m_value;

		/**
		 * Creates a variable.
		 * 
		 * @param name
		 *            The name of the variable
		 * @param type
		 *            The type of the variable
		 * @param value
		 *            The value of the variable
		 */
		public Variable(final String name, final String type, final String value) {
			m_name = name;
			m_type = type;
			m_value = value;
		}

		/**
		 * Return the name of the variable.
		 * 
		 * @return the name
		 */
		public String getName() {
			return m_name;
		}

		/**
		 * Return the type of the variable.
		 * 
		 * @return the type
		 */
		public String getType() {
			return m_type;
		}

		/**
		 * Return the value of the variable.
		 * 
		 * @return the value
		 */
		public String getValue() {
			return m_value;
		}

	}

	/**
	 * A status bar containing a status message, a progress bar for running
	 * processes and a stop button.
	 * 
	 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
	 */
	private static class StatusBar extends JPanel {

		private static final long serialVersionUID = 3398321765916443444L;

		private JLabel m_message = new JLabel();
		private JProgressBar m_runningBar = new JProgressBar();
		private JButton m_stopButton = new JButton();

		/**
		 * Creates a status bar.
		 */
		StatusBar() {
			setBorder(new BevelBorder(BevelBorder.LOWERED));
			setLayout(new GridBagLayout());
			GridBagConstraints gbc = new GridBagConstraints();
			gbc.insets = new Insets(5, 5, 5, 5);
			gbc.anchor = GridBagConstraints.NORTHWEST;
			gbc.fill = GridBagConstraints.BOTH;
			gbc.weightx = 1;
			gbc.weighty = 1;
			gbc.gridx = 0;
			gbc.gridy = 0;
			add(m_message, gbc);
			gbc.gridx++;
			gbc.weightx = 0;
			gbc.insets = new Insets(3, 5, 3, 0);
			add(m_runningBar, gbc);
			gbc.gridx++;
			gbc.insets = new Insets(0, 5, 0, 5);
			add(m_stopButton, gbc);
			Icon stopIcon = new ImageIcon(Activator.getFile("org.knime.python", "res/stop.gif").getAbsolutePath());
			m_stopButton.setIcon(stopIcon);
			m_stopButton.setToolTipText("Stop execution");
			m_stopButton.setPreferredSize(new Dimension(m_stopButton.getPreferredSize().height, m_stopButton
					.getPreferredSize().height));
			m_runningBar.setPreferredSize(new Dimension(150, m_stopButton.getPreferredSize().height - 6));
			m_runningBar.setIndeterminate(true);
			setRunning(false);
		}

		/**
		 * Sets the status message.
		 * 
		 * @param statusMessage
		 *            The current status message
		 */
		void setStatusMessage(final String statusMessage) {
			m_message.setText(statusMessage);
		}

		/**
		 * Sets the progress bar and stop button visible / invisible.
		 * 
		 * @param running
		 *            true if the process is currently running, false otherwise
		 */
		void setRunning(final boolean running) {
			m_runningBar.setVisible(running);
			m_stopButton.setVisible(running);
		}

		/**
		 * Sets a callback to be called if the stop button is pressed
		 * 
		 * Note: the callback will be executed in an extra thread and not block
		 * the UI thread.
		 * 
		 * @param callback
		 *            The callback to execute when the stop button is pressed
		 */
		void setStopCallback(final Runnable callback) {
			// Remove old listener first
			ActionListener[] actionListeners = m_stopButton.getActionListeners();
			for (ActionListener listener : actionListeners) {
				m_stopButton.removeActionListener(listener);
			}
			// Add new listener
			m_stopButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					if (callback != null) {
						// Run callback in extra thread and don't block UI
						// thread
						new Thread(callback).start();
					}
				}
			});
		}

		/**
		 * Return the progress bar
		 * 
		 * @return The progress bar
		 */
		JProgressBar getProgressBar() {
			return m_runningBar;
		}

	}

	private static final long serialVersionUID = -3216788918504383870L;

	/**
	 * File containing the users spell checking dictionary.
	 */
	private static final File USER_DICTIONARY = new File(ResourcesPlugin.getWorkspace().getRoot().getLocation()
			.toOSString(), ".metadata" + File.separator + "dict" + File.separator + "userDictionary.txt");
	private static final String[] VARIABLES_COLUMN_NAMES = new String[] { "Name", "Type", "Value" };

	private boolean m_interactive = false;

	private boolean m_running = false;

	private Style m_normalStyle;

	private Style m_errorStyle;
	private RSyntaxTextArea m_editor;

	private StatusBar m_statusBar = new StatusBar();

	private JTextPane m_console = new JTextPane();

	/**
	 * Read only table model.
	 */
	private DefaultTableModel m_varsModel = new DefaultTableModel(VARIABLES_COLUMN_NAMES, 0) {
		private static final long serialVersionUID = -8702103117733835073L;

		@Override
		public boolean isCellEditable(int row, int column) {
			// No cell is editable
			return false;
		};
	};

	private static final NodeLogger LOGGER = NodeLogger.getLogger(SourceCodePanel.class);
	
	private JTable m_vars = new JTable(m_varsModel);
	private JButton m_exec = new JButton("Execute script");
	private JButton m_execSelection = new JButton("Execute selection");
	private JButton m_reset = new JButton("Reset workspace");
	private JButton m_clearConsole = new JButton();
	private DefaultListModel<FlowVariable> m_flowVariablesModel = new DefaultListModel<FlowVariable>();
	private JList<FlowVariable> m_flowVariables = new JList<FlowVariable>(m_flowVariablesModel);
	private JPanel m_flowVariablesPanel;
	private DefaultListModel<DataColumnSpec> m_columnsModel = new DefaultListModel<DataColumnSpec>();
	private JList<DataColumnSpec> m_columns = new JList<DataColumnSpec>(m_columnsModel);
	private JSplitPane m_listsSplit;
	private JSplitPane m_listEditorSplit;
	private JButton m_showImages = new JButton("Show Image");
	private int m_rowLimit = Integer.MAX_VALUE;
	private VariableNames m_variableNames;
	private int[] m_tableEnds;

	/**
	 * Create a source code panel for the given language style.
	 * 
	 * @param syntaxStyle
	 *            One of the language styles defined in {@link SyntaxConstants}
	 * @param flowVariablesName
	 *            Name of the flow variables variable
	 * @param inputTableNames
	 *            Name of the input table variables
	 * @param outputImageNames
	 *            Name of the output image variables
	 */
	public SourceCodePanel(final String syntaxStyle, final VariableNames variableNames) {
		m_editor = createEditor(syntaxStyle);
		m_variableNames = variableNames;
		if (variableNames.getOutputImages().length > 1) {
			m_showImages.setText("Show Images");
		}
		setLayout(new BorderLayout());
		add(m_statusBar, BorderLayout.SOUTH);
		JPanel columnsPanel = new JPanel(new BorderLayout());
		JLabel columnsLabel = new JLabel("Columns");
		columnsLabel.setBorder(new EmptyBorder(5, 5, 5, 5));
		columnsPanel.add(columnsLabel, BorderLayout.NORTH);
		columnsPanel.add(new JScrollPane(m_columns), BorderLayout.CENTER);
		m_flowVariablesPanel = new JPanel(new BorderLayout());
		JLabel flowVariablesLabel = new JLabel("Flow variables");
		flowVariablesLabel.setBorder(new EmptyBorder(5, 5, 5, 5));
		m_flowVariablesPanel.add(flowVariablesLabel, BorderLayout.NORTH);
		m_flowVariablesPanel.add(new JScrollPane(m_flowVariables), BorderLayout.CENTER);
		m_listsSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
		m_listsSplit.setResizeWeight(0.5);
		m_listsSplit.setOneTouchExpandable(true);
		m_listsSplit.setDividerSize(8);
		m_listsSplit.setTopComponent(columnsPanel);
		m_listsSplit.setBottomComponent(m_flowVariablesPanel);
		m_listsSplit.setDividerLocation(200);
		JSplitPane editorWorkspaceSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
		editorWorkspaceSplit.setResizeWeight(0.7);
		editorWorkspaceSplit.setOneTouchExpandable(true);
		editorWorkspaceSplit.setDividerSize(8);
		RTextScrollPane editorScrollPane = new RTextScrollPane(m_editor);
		JPanel editorButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
		editorButtons.add(m_exec);
		editorButtons.add(m_execSelection);
		JPanel editorPanel = new JPanel(new BorderLayout());
		editorPanel.add(editorScrollPane, BorderLayout.CENTER);
		editorPanel.add(editorButtons, BorderLayout.SOUTH);
		JPanel workspaceButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
		workspaceButtons.add(m_reset);
		if (m_variableNames.getOutputImages().length > 0) {
			workspaceButtons.add(m_showImages);
			initShowImages();
		}
		JPanel workspacePanel = new JPanel(new BorderLayout());
		workspacePanel.add(new JScrollPane(m_vars), BorderLayout.CENTER);
		workspacePanel.add(workspaceButtons, BorderLayout.SOUTH);
		editorScrollPane.setFoldIndicatorEnabled(true);
		m_listEditorSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
		m_listEditorSplit.setResizeWeight(0.3);
		m_listEditorSplit.setOneTouchExpandable(true);
		m_listEditorSplit.setDividerSize(8);
		m_listEditorSplit.setLeftComponent(m_listsSplit);
		m_listEditorSplit.setRightComponent(editorPanel);
		m_listEditorSplit.setDividerLocation(200);
		editorWorkspaceSplit.setLeftComponent(m_listEditorSplit);
		editorWorkspaceSplit.setRightComponent(workspacePanel);
		editorWorkspaceSplit.setDividerLocation(700);
		JPanel consoleButtons = new JPanel(new FlowLayout(FlowLayout.LEADING));
		consoleButtons.add(m_clearConsole);
		JPanel consolePanel = new JPanel(new BorderLayout());
		consolePanel.add(new JScrollPane(m_console), BorderLayout.CENTER);
		consolePanel.add(consoleButtons, BorderLayout.EAST);
		JSplitPane editorConsoleSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
		editorConsoleSplit.setResizeWeight(0.7);
		editorConsoleSplit.setOneTouchExpandable(true);
		editorConsoleSplit.setDividerSize(8);
		editorConsoleSplit.setTopComponent(editorWorkspaceSplit);
		editorConsoleSplit.setBottomComponent(consolePanel);
		editorConsoleSplit.setDividerLocation(400);
		add(editorConsoleSplit, BorderLayout.CENTER);
		Icon clearIcon = new ImageIcon(Activator.getFile("org.knime.python", "res/clear.gif").getAbsolutePath());
		m_clearConsole.setIcon(clearIcon);
		m_clearConsole.setToolTipText("Clear console");
		m_clearConsole.setPreferredSize(new Dimension(m_clearConsole.getPreferredSize().height, m_clearConsole
				.getPreferredSize().height));
		// Console style for normal text with black text
		m_normalStyle = m_console.addStyle("normalstyle", null);
		StyleConstants.setForeground(m_normalStyle, Color.black);
		// Console style for errors with red text
		m_errorStyle = m_console.addStyle("errorstyle", null);
		StyleConstants.setForeground(m_errorStyle, Color.red);
		// Configure auto completion
		CompletionProvider provider = createCompletionProvider();
		AutoCompletion ac = new AutoCompletion(provider);
		ac.setAutoActivationDelay(100);
		ac.setAutoActivationEnabled(true);
		ac.setShowDescWindow(true);
		ac.setDescriptionWindowSize(580, 300);
		ac.setParameterAssistanceEnabled(true);
		ac.install(m_editor);
		// Configure spell checker
		File dictFile = Activator.getFile("org.fife.rsyntaxtextarea", "res" + File.separator + "english_dic.zip");
		if (dictFile != null) {
			File zip = new File(dictFile.getAbsolutePath());
			try {
				SpellingParser parser = SpellingParser.createEnglishSpellingParser(zip, true);
				if (!USER_DICTIONARY.exists()) {
					USER_DICTIONARY.getParentFile().mkdirs();
					USER_DICTIONARY.createNewFile();
				}
				parser.setUserDictionary(USER_DICTIONARY);
				m_editor.addParser(parser);
			} catch (IOException e1) {
				LOGGER.warn(e1.getMessage(), e1);
			}
		} else {
			LOGGER.warn("Could not locate org.fife.rsyntaxtextarea/res/english_dic.zip");
		}
		// Configure console
		m_console.setEditable(false);
		m_console.setDragEnabled(true);
		m_console.setText("");
		// Add listeners to buttons
		m_exec.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				runExec(m_editor.getText());
			}
		});
		m_execSelection.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				String selectedText = m_editor.getSelectedText();
				if (selectedText != null && !selectedText.isEmpty()) {
					runExec(selectedText);
				} else {
					setStatusMessage("Nothing selected");
				}
			}
		});
		m_reset.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				runReset();
			}
		});
		m_clearConsole.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				m_console.setText("");
			}
		});
		m_vars.addMouseListener(new MouseAdapter() {
			public void mousePressed(MouseEvent me) {
				JTable table = (JTable) me.getSource();
				Point p = me.getPoint();
				int row = table.rowAtPoint(p);
				if (me.getClickCount() == 2) {
					String value = m_varsModel.getValueAt(row, 2).toString();
					if (!value.isEmpty())
						messageToConsole(m_varsModel.getValueAt(row, 0).toString() + ":\n" + value);
				}
			}
		});
		// Configure font for console and variables table
		Font font = m_console.getFont();
		Font newFont = new Font("monospaced", font.getStyle(), font.getSize());
		m_console.setFont(newFont);
		m_vars.setFont(newFont);
		m_statusBar.setStatusMessage(" ");
		m_columns.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		m_flowVariables.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		m_columns.setCellRenderer(new DataColumnSpecListCellRenderer());
		m_flowVariables.setCellRenderer(new FlowVariableListCellRenderer());
		setInteractive(m_interactive);
		setPreferredSize(new Dimension(1000, 600));
		m_flowVariables.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					int index = m_flowVariables.locationToIndex(evt.getPoint());
					FlowVariable flowVariable = m_flowVariablesModel.get(index);
					m_editor.replaceSelection(createVariableAccessString(m_variableNames.getFlowVariables(), flowVariable.getName()));
					m_editor.requestFocus();
				}
			}
		});
		m_columns.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					int index = m_columns.locationToIndex(evt.getPoint());
					DataColumnSpec column = m_columnsModel.get(index);
					int tableNumber = findTableForColumnIndex(index);
					m_editor.replaceSelection(createVariableAccessString(m_variableNames.getInputTables()[tableNumber], column.getName()));
					m_editor.requestFocus();
				}
			}
		});
		setColumnListEnabled(variableNames.getInputTables().length>0);
	}
	
	private int findTableForColumnIndex(final int columnIndex) {
		for (int i = 0; i < m_tableEnds.length; i++) {
			if (columnIndex < m_tableEnds[i]) {
				return i;
			}
		}
		return 0;
	}

	/**
	 * Initializes the show images button.
	 */
	private void initShowImages() {
		m_showImages.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				final JFrame window = new JFrame();
				window.setTitle("Python image");
				Container contentPane = window.getContentPane();
				contentPane.setLayout(new BorderLayout());
				final JLabel imageLabel = new JLabel();
				imageLabel.setForeground(Color.RED);
				setImage(imageLabel, m_variableNames.getOutputImages()[0]);
				contentPane.add(new JScrollPane(imageLabel), BorderLayout.CENTER);
				JPanel buttons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
				JButton closeButton = new JButton("Close");
				buttons.add(closeButton);
				contentPane.add(buttons, BorderLayout.SOUTH);
				if (m_variableNames.getOutputImages().length > 1) {
					window.setTitle("Python images");
					final JComboBox<String> imageSelection = new JComboBox<String>(m_variableNames.getOutputImages());
					contentPane.add(imageSelection, BorderLayout.NORTH);
					imageSelection.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent e) {
							setImage(imageLabel, (String) imageSelection.getSelectedItem());
							window.pack();
						}
					});
				}
				closeButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						window.setVisible(false);
					}
				});
				window.pack();
				window.setLocationRelativeTo(SourceCodePanel.this);
				window.setVisible(true);
			}
		});
	}

	/**
	 * Sets the image that will be shown in the show image window.
	 * 
	 * @param label
	 *            The label to put the image into
	 * @param imageName
	 *            Name of the image to load
	 */
	private void setImage(final JLabel label, final String imageName) {
		ImageContainer image = getOutImage(imageName);
		if (image != null) {
			label.setText(null);
			label.setIcon(new ImageIcon(image.getBufferedImage()));
			label.setBorder(null);
		} else {
			label.setIcon(null);
			label.setText("Could not load image: " + imageName);
			label.setBorder(new EmptyBorder(10, 10, 10, 10));
		}
	}

	/**
	 * Opens and initializes the panel.
	 */
	public void open() {
		// Clean console and variables table
		m_console.setText("");
		m_varsModel.setRowCount(0);
		m_statusBar.getProgressBar().setValue(0);
	}

	/**
	 * Closes and cleans up the panel.
	 */
	public void close() {
		// Nothing to do
	}

	/**
	 * Save settings to the given {@link SourceCodeConfig}.
	 * 
	 * @param config
	 *            The config to save to
	 * @throws InvalidSettingsException
	 *             If the current settings are invalid
	 */
	public void saveSettingsTo(final SourceCodeConfig config) throws InvalidSettingsException {
		config.setSourceCode(m_editor.getText());
	}

	/**
	 * Loads settings from the given {@link SourceCodeConfig}.
	 * 
	 * @param config
	 *            The config to load from
	 * @param specs
	 *            Input port specs
	 * @throws NotConfigurableException
	 *             If the panel is not configurable
	 */
	public void loadSettingsFrom(final SourceCodeConfig config, PortObjectSpec[] specs) throws NotConfigurableException {
		m_editor.setText(config.getSourceCode());
		List<DataTableSpec> tableSpecs = new ArrayList<DataTableSpec>();
		for (PortObjectSpec spec : specs) {
			if (spec instanceof DataTableSpec) {
				tableSpecs.add((DataTableSpec) spec);
			}
		}
		updateSpec(tableSpecs.toArray(new DataTableSpec[tableSpecs.size()]));
	}

	/**
	 * Updates the input data tables.
	 * 
	 * @param inputData
	 *            The input data tables
	 */
	public void updateData(final BufferedDataTable[] inputData) {
		DataTableSpec[] tableSpecs = new DataTableSpec[inputData.length];
		for (int i = 0; i < inputData.length; i++) {
			tableSpecs[i] = inputData[i] == null ? null : inputData[i].getDataTableSpec();
		}
		updateSpec(tableSpecs);
	}

	/**
	 * Updates the spec of the input tables.
	 * 
	 * @param specs
	 *            The input table specs
	 */
	public void updateSpec(final DataTableSpec[] specs) {
		m_tableEnds = new int[specs.length];
		int endPosition = 0;
		m_columnsModel.clear();
		for (int i = 0; i < specs.length; i++) {
			if (specs[i] != null) {
				endPosition += specs[i].getNumColumns();
				m_tableEnds[i] = endPosition;
				for (DataColumnSpec colSpec : specs[i]) {
					m_columnsModel.addElement(colSpec);
				}
			}
		}
	}

	/**
	 * Updates the available flow variables.
	 * 
	 * @param flowVariables
	 *            The flow variables
	 */
	public void updateFlowVariables(final FlowVariable[] flowVariables) {
		m_flowVariablesModel.clear();
		for (FlowVariable flowVariable : flowVariables) {
			m_flowVariablesModel.addElement(flowVariable);
		}
	}

	/**
	 * Gets the flow variables in the current workspace.
	 * 
	 * @return The flow variables
	 */
	protected List<FlowVariable> getFlowVariables() {
		return (List<FlowVariable>) Collections.list(m_flowVariablesModel.elements());
	}

	/**
	 * Sets whether the column list is shown or not
	 * 
	 * @param enabled
	 *            If true the column list will be shown in the upper left, if
	 *            false it will be hidden
	 */
	protected void setColumnListEnabled(final boolean enabled) {
		m_listEditorSplit.setLeftComponent(enabled ? m_listsSplit : m_flowVariablesPanel);
	}

	/**
	 * Sets this panel to be interactive (can execute code, inspect variables,
	 * ...) or not.
	 * 
	 * @param interactive
	 *            true if the panel is interactive
	 */
	protected void setInteractive(final boolean interactive) {
		runInUiThread(new Runnable() {
			@Override
			public void run() {
				m_interactive = interactive;
				// These buttons can only be clicked if the panel is interactive
				// and no execution is currently running
				m_execSelection.setEnabled(interactive && !m_running);
				m_exec.setEnabled(interactive && !m_running);
				m_reset.setEnabled(interactive && !m_running);
				m_showImages.setEnabled(interactive && !m_running);
			}
		});
	}

	/**
	 * Sets this panel to display that it is running code.
	 * 
	 * @param running
	 *            true if code is currently executing, false otherwise
	 */
	protected void setRunning(final boolean running) {
		runInUiThread(new Runnable() {
			@Override
			public void run() {
				boolean wasRunning = m_running;
				m_running = running;
				setInteractive(m_interactive);
				m_statusBar.setRunning(running);
				// Update variables if we just finished running
				if (wasRunning && !running) {
					updateVariables();
				}
			}
		});
	}

	/**
	 * Appends the given message to the console.
	 * 
	 * @param text
	 *            The text to append
	 */
	protected void messageToConsole(final String text) {
		if (text != null && !text.trim().isEmpty()) {
			runInUiThread(new Runnable() {
				@Override
				public void run() {
					StyledDocument doc = m_console.getStyledDocument();
					String string = ScriptingNodeUtils.shortenString(text);
					try {
						// Messages use normal style
						doc.insertString(doc.getLength(), string + "\n", m_normalStyle);
					} catch (BadLocationException e) {
						//
					}
				}
			});
		}
	}

	/**
	 * Appends the given error to the console
	 * 
	 * @param text
	 *            The error to append
	 */
	protected void errorToConsole(final String text) {
		if (text != null && !text.trim().isEmpty()) {
			runInUiThread(new Runnable() {
				@Override
				public void run() {
					StyledDocument doc = m_console.getStyledDocument();
					String string = ScriptingNodeUtils.shortenString(text);
					try {
						// Errors use error style
						doc.insertString(doc.getLength(), string + "\n", m_errorStyle);
					} catch (BadLocationException e) {
						//
					}
				}
			});
		}
	}

	/**
	 * Sets the given status message as new status in the status bar.
	 * 
	 * @param statusMessage
	 *            The new status
	 */
	protected void setStatusMessage(final String statusMessage) {
		runInUiThread(new Runnable() {
			@Override
			public void run() {
				m_statusBar.setStatusMessage(statusMessage);
			}
		});
	}

	/**
	 * Sets the runnable that will be called when the stop button in the status
	 * bar is clicked.
	 * 
	 * @param runnable
	 *            The runnable to call when stop is clicked
	 */
	protected void setStopCallback(final Runnable runnable) {
		m_statusBar.setStopCallback(runnable);
	}

	/**
	 * Returns the progress bar of the status bar.
	 * 
	 * @return The progress bar
	 */
	protected JProgressBar getProgressBar() {
		return m_statusBar.getProgressBar();
	}

	/**
	 * Executes the given source code.
	 * 
	 * This method should add the output to the console.
	 * 
	 * @param sourceCode
	 *            The source code to execute
	 */
	protected abstract void runExec(final String sourceCode);

	/**
	 * Initiates an update of the listed variables.
	 * 
	 * This method should call {@link #setVariables(Variable[])} once the
	 * variables have been retrieved.
	 */
	protected abstract void updateVariables();

	/**
	 * Sets the variables displayed in the variable table
	 * 
	 * @param variables
	 *            The variables to display
	 */
	protected void setVariables(final Variable[] variables) {
		// Create vector and put it into the model
		Object[][] variablesVector = new Object[variables.length][];
		for (int i = 0; i < variables.length; i++) {
			variablesVector[i] = new Object[3];
			variablesVector[i][0] = variables[i].getName();
			variablesVector[i][1] = variables[i].getType();
			variablesVector[i][2] = variables[i].getValue();
		}
		m_varsModel.setDataVector(variablesVector, VARIABLES_COLUMN_NAMES);
	}

	/**
	 * Resets the current workspace.
	 */
	protected abstract void runReset();

	/**
	 * Retrieves possible completions for the given source code at the given
	 * cursor position
	 * 
	 * @param provider
	 *            {@link CompletionProvider} for creation of the
	 *            {@link Completion} objects
	 * @param sourceCode
	 *            The source code to complete
	 * @param line
	 *            Cursor position (line)
	 * @param column
	 *            Cursor position (column)
	 * @return List of possible completions
	 */
	protected abstract List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
			final int line, final int column);

	/**
	 * Creates a completion provider that will be used to suggest completions.
	 * 
	 * The created provider will utilize
	 * {@link #getCompletionsFor(CompletionProvider, String, int, int)} to
	 * retrieve possible completions.
	 * 
	 * @return The {@link CompletionProvider}
	 */
	private CompletionProvider createCompletionProvider() {
		DefaultCompletionProvider provider = new DefaultCompletionProvider() {
			@Override
			public List<Completion> getCompletions(JTextComponent comp) {
				List<Completion> completions = super.getCompletions(comp);
				// Get source code from editor
				String sourceCode = comp.getText();
				// Caret position only gives as the number of characters before
				// the cursor
				// We need to inspect the code before the cursor to figure out
				// the line and column numbers
				String codeBeforeCaret = sourceCode.substring(0, comp.getCaretPosition());
				// Line = how many newlines are in the code?
				int line = StringUtils.countMatches(codeBeforeCaret, "\n");
				// Column = how long is the last line of the code?
				int column = codeBeforeCaret.substring(codeBeforeCaret.lastIndexOf("\n") + 1).length();
				// Add completions from getCompletionsFor()
				completions.addAll(getCompletionsFor(this, sourceCode, line, column));
				return completions;
			}
		};
		// Automatically suggest after '.'
		provider.setAutoActivationRules(false, ".");
		return provider;
	}

	/**
	 * Runs the given runnable in the UI thread.
	 * 
	 * @param runnable
	 *            The runnable to run in the UI thread
	 */
	private static void runInUiThread(final Runnable runnable) {
		if (SwingUtilities.isEventDispatchThread()) {
			// We are in UI thread, just run it
			runnable.run();
		} else {
			// Queue up for execution in UI thread
			SwingUtilities.invokeLater(runnable);
		}
	}

	/**
	 * Gets the output image with the given name
	 * 
	 * @param name
	 *            The variable name in the workspace
	 * @return The image
	 */
	protected ImageContainer getOutImage(final String name) {
		return null;
	}

	/**
	 * Sets the row limit used to put tables into the workspace.
	 * 
	 * @param rowLimit
	 *            The maximum number of rows per table
	 */
	void setRowLimit(final int rowLimit) {
		m_rowLimit = rowLimit;
	}

	/**
	 * Gets the row limit used to put tables into the workspace.
	 * 
	 * @return The maximum number of rows per table
	 */
	protected int getRowLimit() {
		return m_rowLimit;
	}

	/**
	 * Creates the string used to access a variable in the source code.
	 * 
	 * @param variable
	 *            The variable name
	 * @param field
	 *            Name of the field inside the variable
	 * @return Variable excess string
	 */
	protected abstract String createVariableAccessString(final String variable, final String field);

	/**
	 * Get the name for the flow variables variable.
	 * 
	 * @return The name
	 */
	protected VariableNames getVariableNames() {
		return m_variableNames;
	}
	
	public RSyntaxTextArea getEditor() {
		return m_editor;
	}
	
	public static RSyntaxTextArea createEditor(final String syntaxStyle) {
		RSyntaxTextArea editor = new RSyntaxTextArea();
		editor.setSyntaxEditingStyle(syntaxStyle);
		editor.setCodeFoldingEnabled(true);
		editor.setAntiAliasingEnabled(true);
		editor.setAutoIndentEnabled(true);
		editor.setFadeCurrentLineHighlight(true);
		editor.setHighlightCurrentLine(true);
		editor.setLineWrap(false);
		editor.setRoundedSelectionEdges(true);
		editor.setBorder(new EtchedBorder());
		editor.setTabSize(4);
		return editor;
	}

}
