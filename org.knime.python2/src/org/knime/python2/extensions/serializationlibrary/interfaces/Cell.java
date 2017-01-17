package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * A cell containing a single value (or array of values).
 * 
 * @author Patrick Winter
 */
public interface Cell {
	
	/**
	 * @return Name of the column containing this cell.
	 */
	String getColumnName();

	/**
	 * @return {@link Type} of the column containing this cell.
	 */
	Type getColumnType();
	
	/**
	 * @return true if the value of this cell is missing, false otherwise.
	 */
	boolean isMissing();
	
	/**
	 * @return The boolean value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#BOOLEAN}.
	 */
	Boolean getBooleanValue() throws IllegalStateException;

	/**
	 * @return The boolean array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#BOOLEAN_LIST} or {@link Type#BOOLEAN_SET}.
	 */
	Boolean[] getBooleanArrayValue() throws IllegalStateException;
	
	/**
	 * @return The integer value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#INTEGER}.
	 */
	Integer getIntegerValue() throws IllegalStateException;

	/**
	 * @return The integer array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#INTEGER_LIST} or {@link Type#INTEGER_SET}.
	 */
	Integer[] getIntegerArrayValue() throws IllegalStateException;

	/**
	 * @return The long value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#LONG}.
	 */
	Long getLongValue() throws IllegalStateException;

	/**
	 * @return The long array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#LONG_LIST} or {@link Type#LONG_SET}.
	 */
	Long[] getLongArrayValue() throws IllegalStateException;

	/**
	 * @return The double value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#DOUBLE}.
	 */
	Double getDoubleValue() throws IllegalStateException;

	/**
	 * @return The double array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#DOUBLE_LIST} or {@link Type#DOUBLE_SET}.
	 */
	Double[] getDoubleArrayValue() throws IllegalStateException;

	/**
	 * @return The string value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#STRING}.
	 */
	String getStringValue() throws IllegalStateException;

	/**
	 * @return The string array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#STRING_LIST} or {@link Type#STRING_SET}.
	 */
	String[] getStringArrayValue() throws IllegalStateException;

	/**
	 * @return The bytes value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#BYTES}.
	 */
	Byte[] getBytesValue() throws IllegalStateException;

	/**
	 * @return The bytes array value of this cell.
	 * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not {@link Type#BYTES_LIST} or {@link Type#BYTES_SET}.
	 */
	Byte[][] getBytesArrayValue() throws IllegalStateException;

}
