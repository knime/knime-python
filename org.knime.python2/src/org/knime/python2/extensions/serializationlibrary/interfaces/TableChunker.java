package org.knime.python2.extensions.serializationlibrary.interfaces;

public interface TableChunker {
	
	/**
	 * @return Has more chunks
	 */
	boolean hasNextChunk();
	
	/**
	 * @return A {@link TableIterator} over the next numRows rows of the underlying data structure
	 */
	TableIterator nextChunk(int numRows);
	
	/**
	 * @return The number of rows that have not been requested via {@link #next()}.
	 */
	int getNumberRemainingRows();
	
	/**
	 * @return The {@link TableSpec}.
	 */
	TableSpec getTableSpec();

}
