package org.knime.flatbuffers;

import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;

/**
 * @author Oliver Sampson, University of Konstanz
 *
 */
public class FlatbuffersColumnSerializationFactory implements SerializationLibraryFactory {

	@Override
	public String getName() {		
		return "Flatbuffers Column Serialization";
	}

	@Override
	public SerializationLibrary createInstance() {		
		return new Flatbuffers();
	}

}
