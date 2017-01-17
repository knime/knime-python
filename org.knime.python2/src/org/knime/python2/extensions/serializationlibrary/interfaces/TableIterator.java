package org.knime.python2.extensions.serializationlibrary.interfaces;

import java.util.Iterator;

/**
 * {@link Iterator} for the {@link Row}s contained in a table.
 * 
 * @author Patrick Winter
 */
public interface TableIterator extends Iterator<Row> {
	
	/**
	 * {@inheritDoc}
	 */
	Row next();

	/**
	 * {@inheritDoc}
	 */
	boolean hasNext();
	
	/**
	 * @return The number of rows that have not been requested via {@link #next()}.
	 */
	int getNumberRemainingRows();
	
	/**
	 * @return The {@link TableSpec}.
	 */
	TableSpec getTableSpec();

}
