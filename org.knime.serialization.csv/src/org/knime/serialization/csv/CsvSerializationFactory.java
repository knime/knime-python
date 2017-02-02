package org.knime.serialization.csv;

import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;

public class CsvSerializationFactory implements SerializationLibraryFactory {

	@Override
	public String getName() {
		return "CSV";
	}

	@Override
	public SerializationLibrary createInstance() {
		return new CsvSerializationLibrary();
	}

}
