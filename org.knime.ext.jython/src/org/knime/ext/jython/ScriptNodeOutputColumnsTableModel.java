package org.knime.ext.jython;

import javax.swing.table.*;
import java.util.*;

public class ScriptNodeOutputColumnsTableModel extends AbstractTableModel {

	private static final long serialVersionUID = 3748218849626706004L;
	ArrayList<ArrayList<Object>> data = new ArrayList<ArrayList<Object>>();
	ArrayList<String> columnNames = new ArrayList<String>();
	
	public String getColumnName(int col) { 
	    return columnNames.get(col).toString(); 
	}
	
	public int getRowCount() { 
		return data.size(); 
	}
	
	public int getColumnCount() { 
		return columnNames.size(); 
	}
	
	public Object getValueAt(int row, int col) {
		
		ArrayList<Object> rowList = data.get(row);
		return rowList.get(col);
	}
	
	public boolean isCellEditable(int row, int col) { return true; }
	
	public void setValueAt(Object value, int row, int col) {
		ArrayList<Object> rowList = data.get(row);
		rowList.set(col, value);
	    fireTableCellUpdated(row, col);
	}
	
	public void addRow(Object dataTableColumnName, Object dataTableColumnType) {
		ArrayList<Object> row = new ArrayList<Object>();
		row.add(dataTableColumnName);
		row.add(dataTableColumnType);
		
		data.add(row);
		
		int rowNum = data.size() - 1;
		fireTableRowsInserted(rowNum, rowNum);
	}
	
	public void removeRow(int row) {
		data.remove(row);
		fireTableRowsDeleted(row, row);
	}
	
	public void addColumn(String columnName) {
		columnNames.add(columnName);
	}
	
	public String[] getDataTableColumnNames() {
		return getDataTableValues(0);
	}
	
	public String[] getDataTableColumnTypes() {
		return getDataTableValues(1);
	}
	
	private String[] getDataTableValues(int colIndex) {
		String[] dataTableColumnValues = new String[data.size()];
		
		Iterator<ArrayList<Object>> i = data.iterator();
		int rowNum = 0;
		while (i.hasNext()) {
			ArrayList<Object> row = i.next();
			dataTableColumnValues[rowNum] = (String) row.get(colIndex);
			rowNum++;
		}
		return dataTableColumnValues;
	}
	
	public void clearRows() {
		data = new ArrayList<ArrayList<Object>>();
	}
}
