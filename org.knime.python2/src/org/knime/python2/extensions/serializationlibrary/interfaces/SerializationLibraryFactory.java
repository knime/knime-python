package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * Creates a {@link SerializationLibrary}.
 * 
 * @author Patrick Winter
 */
public interface SerializationLibraryFactory {
	
	/**
	 * @return The name of the serialization library.
	 */
	String getName();
	
	/**
	 * @return A new instance of the {@link SerializationLibrary}.
	 */
	SerializationLibrary createInstance();

}
