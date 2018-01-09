/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
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
