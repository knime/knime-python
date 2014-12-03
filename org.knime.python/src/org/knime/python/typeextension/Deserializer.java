package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;

public interface Deserializer {
	
	public DataCell deserialize(final byte[] bytes, final FileStoreFactory fileStoreFactory) throws IOException;

}
