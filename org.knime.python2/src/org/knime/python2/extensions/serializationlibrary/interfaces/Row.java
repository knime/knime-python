package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * A row containing {@link Cell}s.
 * 
 * @author Patrick Winter
 */
public interface Row extends Iterable<Cell> {
	
	/**
	 * Sets the {@link Cell} at the given index.
	 * 
	 * @param cell The {@link Cell} to set.
	 * @param index The index.
	 */
	void setCell(Cell cell, int index);
	
	/**
	 * @return The number of cells in this row.
	 */
	int getNumberCells();
	
	/**
	 * @return Row key of this row.
	 */
	String getRowKey();
	
	/**
	 * Get the {@link Cell} for the given index.
	 * 
	 * @param index Index of the cell to get.
	 * @return The {@link Cell}.
	 */
	Cell getCell(int index);
	
}
