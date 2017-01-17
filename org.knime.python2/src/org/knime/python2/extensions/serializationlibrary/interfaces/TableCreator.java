package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * Used to create a table by adding rows.
 * 
 * @author Patrick Winter
 */
public interface TableCreator {
	
	/**
	 * @param row The row to add to the table.
	 */
	void addRow(Row row);
	
	/**
	 * @return The {@link TableSpec}.
	 */
	TableSpec getTableSpec();

}
