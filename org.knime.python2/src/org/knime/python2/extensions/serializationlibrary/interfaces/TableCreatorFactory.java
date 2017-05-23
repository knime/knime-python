package org.knime.python2.extensions.serializationlibrary.interfaces;

public interface TableCreatorFactory {
	
	public TableCreator createTableCreator(TableSpec spec, int tableSize);

}
