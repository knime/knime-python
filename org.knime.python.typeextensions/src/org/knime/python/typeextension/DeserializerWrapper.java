package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.python2.typeextension.Deserializer;

public class DeserializerWrapper implements Deserializer {
	
	private final org.knime.python.typeextension.Deserializer m_deserializer;
	
	public DeserializerWrapper(org.knime.python.typeextension.Deserializer deserializer) {
		m_deserializer = deserializer;
	}

	@Override
	public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
		return m_deserializer.deserialize(bytes, fileStoreFactory);
	}

}
