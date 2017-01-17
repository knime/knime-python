package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * A serialization library used to encode and decode tables for data transfer between java and python.
 * 
 * @author Patrick Winter
 */
public interface SerializationLibrary {
	
	/**
	 * Converts the given table into bytes for transfer to python.
	 * 
	 * @param rowIterator Iterator for the table that should be converted.
	 * @return The bytes that should be send to python.
	 */
	byte[] tableToBytes(TableIterator tableIterator);
	
	/**
	 * Adds the rows contained in the bytes to the given {@link tableCreator}.
	 * 
	 * @param tableCreator The {@link TableCreator} that the rows should be added to.
	 * @param bytes The bytes containing the encoded table.
	 */
	void bytesIntoTable(TableCreator tableCreator, byte[] bytes);
	
	/**
	 * Extracts the {@link TableSpec} of the given table.
	 * 
	 * @param bytes The encoded table.
	 * @return The {@link TableSpec} of the encoded table.
	 */
	TableSpec tableSpecFromBytes(byte[] bytes);

}
