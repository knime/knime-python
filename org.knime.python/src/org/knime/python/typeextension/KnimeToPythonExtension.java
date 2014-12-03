package org.knime.python.typeextension;

import org.knime.core.data.DataValue;

public class KnimeToPythonExtension {

	private String m_id;
	private String m_pythonDeserializerPath;
	private SerializerFactory<? extends DataValue> m_javaSerializerFactory;
	
	public KnimeToPythonExtension(String id, String pythonDeserializerPath, SerializerFactory<? extends DataValue> javaSerializer) {
		m_id = id;
		m_pythonDeserializerPath = pythonDeserializerPath;
		m_javaSerializerFactory = javaSerializer;
	}
	
	public String getId() {
		return m_id;
	}

	public String getPythonDeserializerPath() {
		return m_pythonDeserializerPath;
	}
	
	public SerializerFactory<? extends DataValue> getJavaSerializerFactory() {
		return m_javaSerializerFactory;
	}
	
}
