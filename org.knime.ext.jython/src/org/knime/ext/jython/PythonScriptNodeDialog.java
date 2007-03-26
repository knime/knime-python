/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 */
package org.knime.ext.jython;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.event.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import javax.swing.AbstractAction;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.TableColumn;
import javax.swing.table.TableCellEditor;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.*;

/**
 * <code>NodeDialog</code> for the "JPython Script" Node.
 *
 * @author Tripos
 */
public class PythonScriptNodeDialog extends NodeDialogPane
{
	private static NodeLogger logger = NodeLogger.getLogger(PythonScriptNodeDialog.class);
	private JTextArea scriptTextArea = new JTextArea(10,40);
	private JTable table;
	private int counter = 1;

	/**
	 * New pane for configuring ScriptedNode node dialog
	 *
	 * @param title
	 *            The title
	 */
	protected PythonScriptNodeDialog()
	{
		super();
		
		scriptTextArea.setAutoscrolls(true);
	
		// construct the output column selection panel
		JPanel outputPanel = new JPanel();
		outputPanel.setLayout(new BorderLayout());
		JPanel outputButtonPanel = new JPanel();
		JPanel outputMainPanel = new JPanel(new BorderLayout());
		
		JButton addButton = new JButton(new AbstractAction() {

        	public void actionPerformed(ActionEvent e) {
        		((ScriptNodeOutputColumnsTableModel) table.getModel()).addRow("script output " + counter,
        				"String");
        		counter++;
        	}
        });
        addButton.setText("Add Output Column");
        
        JButton removeButton = new JButton(new AbstractAction() {

        	public void actionPerformed(ActionEvent e) {
        		int[] selectedRows = table.getSelectedRows();
        		logger.debug("selectedRows = " + selectedRows);
        		
        		if (selectedRows.length == 0) {
        			return;
        		}
        		
        		for (int i = selectedRows.length - 1; i >= 0; i--) {
        			logger.debug("   removal " + i + ": removing row " + selectedRows[i]);
        			((ScriptNodeOutputColumnsTableModel) table.getModel()).removeRow(selectedRows[i]);
        		}
        	}
        });
        removeButton.setText("Remove Output Column");       
        
		outputButtonPanel.add(addButton);
		outputButtonPanel.add(removeButton);
		
		table = new JTable();
		table.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);
		
		table.setAutoscrolls(true);
		ScriptNodeOutputColumnsTableModel model = new ScriptNodeOutputColumnsTableModel();
		model.addColumn("Column name");
		model.addColumn("Column type");
		model.addRow("script output " + counter, "String");
		counter++;
		table.setModel(model);
		
		outputMainPanel.add(table.getTableHeader(), BorderLayout.PAGE_START);
		outputMainPanel.add(table, BorderLayout.CENTER);
		outputPanel.add(outputButtonPanel, BorderLayout.PAGE_START);
		outputPanel.add(outputMainPanel, BorderLayout.CENTER);
		
		TableColumn typeColumn = table.getColumnModel().getColumn(1);
		JComboBox typeSelector = new JComboBox();
		typeSelector.addItem("String");
		typeSelector.addItem("Integer");
		typeSelector.addItem("Double");
		typeColumn.setCellEditor(new DefaultCellEditor(typeSelector));
		
		
		// construct the panel for script loading/authoring
		JPanel scriptPanel = new JPanel(new BorderLayout());
		
		JPanel scriptButtonPanel = new JPanel();
        JButton scriptButton = new JButton(new AbstractAction() {
        	
        	JFileChooser fileChooser = new JFileChooser();
        	
        	public void actionPerformed(ActionEvent e) {
        		
        		// open the file dialog
        		int returnVal = fileChooser.showOpenDialog((Component) e.getSource());

        		if (returnVal != JFileChooser.APPROVE_OPTION) {
        			return;
        		}
        		
        		// check for file existence
        		File file = fileChooser.getSelectedFile();
        		if (!file.exists()) {
        			return;
        		}
        		
        		// read the contents and put them in the script textarea
        		StringBuffer buffer = new StringBuffer();
        		BufferedReader reader;
        		try {
            		reader = new BufferedReader(new FileReader(file));
            		while (reader.ready()) {
            			String line = reader.readLine();
            			buffer.append(line + "\n");
            		}
            		reader.close();
            		
        		} catch (IOException exc) {
        			exc.printStackTrace();
        		}
        		
        		scriptTextArea.setText(buffer.toString());
        	}
        });
        scriptButton.setText("Load Script from File");
        
		scriptButtonPanel.add(scriptButton);
		
		JPanel scriptMainPanel = new JPanel(new BorderLayout());
		scriptMainPanel.add(new JLabel("Script: "), BorderLayout.NORTH);
		scriptMainPanel.add(new JScrollPane(scriptTextArea), BorderLayout.CENTER);
		
		scriptPanel.add(scriptButtonPanel, BorderLayout.PAGE_START);
		scriptPanel.add(scriptMainPanel, BorderLayout.CENTER);
		
		addTab("Script Output", outputPanel);
		addTab("Script", scriptPanel);
	}

	/**
	 * {@inheritDoc}
	 */
	protected void loadSettingsFrom(final NodeSettingsRO settings,
			final DataTableSpec[] specs)
	{        
		String script = settings.getString(PythonScriptNodeModel.SCRIPT, null);
		if (script == null) {
			script = "";
		}
		scriptTextArea.setText(script);
		
		String[] dataTableColumnNames = 
			settings.getStringArray(PythonScriptNodeModel.COLUMN_NAMES, new String[0]);
		String[] dataTableColumnTypes = 
			settings.getStringArray(PythonScriptNodeModel.COLUMN_TYPES, new String[0]);

		if (dataTableColumnNames == null) {
			return;
		}
		
		((ScriptNodeOutputColumnsTableModel) table.getModel()).clearRows();
		for (int i=0; i < dataTableColumnNames.length; i++) {
			((ScriptNodeOutputColumnsTableModel) table.getModel()).addRow(dataTableColumnNames[i],
					dataTableColumnTypes[i]);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	protected void saveSettingsTo(final NodeSettingsWO settings)
			throws InvalidSettingsException
	{
		// work around a jtable cell value persistence problem
		// by explicitly stopping editing if a cell is currently in edit mode
		int editingRow = table.getEditingRow();
		int editingColumn = table.getEditingColumn();
		
		if (editingRow != -1 && editingColumn != -1) {
			TableCellEditor editor = table.getCellEditor(editingRow, editingColumn);
			editor.stopCellEditing();
		}
		
		// save the settings
		String scriptSetting = scriptTextArea.getText();
		if (scriptSetting == null || "".equals(scriptSetting)) {
			throw new InvalidSettingsException("Please specify a script to be run.");
		}
		settings.addString(PythonScriptNodeModel.SCRIPT, scriptTextArea.getText());

		String[] columnNames = 
			((ScriptNodeOutputColumnsTableModel) table.getModel()).getDataTableColumnNames();
		settings.addStringArray(PythonScriptNodeModel.COLUMN_NAMES, columnNames);
		
		String[] columnTypes = 
			((ScriptNodeOutputColumnsTableModel) table.getModel()).getDataTableColumnTypes();
		settings.addStringArray(PythonScriptNodeModel.COLUMN_TYPES, columnTypes);
	}

}
