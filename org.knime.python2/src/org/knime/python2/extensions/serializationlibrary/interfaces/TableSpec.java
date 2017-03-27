package org.knime.python2.extensions.serializationlibrary.interfaces;

import java.util.Map;

/**
 * Spec of a table, containing the column names and column types.
 * 
 * @author Patrick Winter
 */
public interface TableSpec {
	
	/**
	 * @return The types of the columns.
	 */
	Type[] getColumnTypes();
	
	/**
	 * @return The names of the columns.
	 */
	String[] getColumnNames();
	
	/**
	 * @return The number of columns.
	 */
	int getNumberColumns();
	
	/**
	 * Find the index of the column with the given name.
	 * 
	 * @param name The name of the column.
	 * @return Index of the column with the given name, -1 if the column has not been found.
	 */
	int findColumn(String name);
	
	/**
	 * @return Map containing the column names as key and the corresponding serializer id as value.
	 */
	Map<String, String> getColumnSerializers();

}
