package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.filestore.FileStoreFactory;

public abstract class Deserializer<Value extends DataValue> {
	
	private DataType m_type;
	
	public Deserializer(final DataType dataType) {
		m_type = dataType;
	}
	
	public final DataType getDataType() {
		return m_type;
	}
	
	public abstract DataCell deserialize(final byte[] bytes, final FileStoreFactory fileStoreFactory) throws IOException;

}
