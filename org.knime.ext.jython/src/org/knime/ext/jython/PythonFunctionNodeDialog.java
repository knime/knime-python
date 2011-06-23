/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 */
package org.knime.ext.jython;

import java.awt.*;
import javax.swing.*;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.*;

/**
 * <code>NodeDialog</code> for the "JPython Function" Node.
 *
 * @author Tripos
 */
public class PythonFunctionNodeDialog extends NodeDialogPane
{
	private JTextArea scriptTextArea = new JTextArea(10,40);
	private JTextField colNameTextField = new JTextField();
	private JComboBox  colTypeSelector = new JComboBox();
	/**
	 * New pane for configuring ScriptedNode node dialog
	 *
	 * @param title
	 *            The title
	 */
	protected PythonFunctionNodeDialog()
	{
		super();
		
		JPanel settingsPanel = new JPanel(new BorderLayout());
		
		scriptTextArea.setAutoscrolls(true);
	
		// construct the output column panel
		JPanel outputPanel = new JPanel();
		outputPanel.setLayout(new GridLayout(0,2));
		outputPanel.setBorder(new TitledBorder(
				new EtchedBorder(EtchedBorder.LOWERED), "Output Column"));
		
		colTypeSelector = new JComboBox();
		colTypeSelector.addItem("String");
		colTypeSelector.addItem("Integer");
		colTypeSelector.addItem("Double");
		
		outputPanel.add(new JLabel("output column name"));
		outputPanel.add(colNameTextField);
		outputPanel.add(new JLabel("output column type"));
		outputPanel.add(colTypeSelector);
		
		// construct the panel for script/function authoring
		JPanel scriptPanel = new JPanel(new BorderLayout());
		scriptPanel.setBorder(new TitledBorder(
				new EtchedBorder(EtchedBorder.LOWERED), "JPython Function"));
		scriptPanel.add(new JScrollPane(scriptTextArea), BorderLayout.CENTER);
		scriptTextArea.setText("");
		
		settingsPanel.add(outputPanel, BorderLayout.NORTH);
		settingsPanel.add(scriptPanel, BorderLayout.CENTER);
		addTab("Settings", settingsPanel);
	}

	/**
	 * {@inheritDoc}
	 */
	protected void loadSettingsFrom(final NodeSettingsRO settings,
			final DataTableSpec[] specs)
	{
		String script = settings.getString(PythonScriptNodeModel.SCRIPT, null);
		if (script == null || "".equals(script)) {
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
		
		String colName = dataTableColumnNames[0];
		String colType = dataTableColumnTypes[0];
		
		colNameTextField.setText(colName);
		colTypeSelector.setSelectedItem(colType);
	}

	/**
	 * {@inheritDoc}
	 */
	protected void saveSettingsTo(final NodeSettingsWO settings)
			throws InvalidSettingsException
	{
		String scriptSetting = scriptTextArea.getText();
		if (scriptSetting == null || "".equals(scriptSetting)) {
			throw new InvalidSettingsException("Please specify a cell function to be run.");
		}
		settings.addString(PythonScriptNodeModel.SCRIPT, scriptTextArea.getText());

		String colName = colNameTextField.getText();
		if (colName == null || "".equals(colName)) {
			throw new InvalidSettingsException("Please specify an output column name.");
		}
		
		String colType = (String) colTypeSelector.getSelectedItem();
		if (colType == null || "".equals(colType)) {
			throw new InvalidSettingsException("Please specify an output column type.");
		}
		
		String[] columnNames = new String[] {colName};
		settings.addStringArray(PythonScriptNodeModel.COLUMN_NAMES, columnNames);
		
		String[] columnTypes = new String[] {colType};
		settings.addStringArray(PythonScriptNodeModel.COLUMN_TYPES, columnTypes);
	}

}
