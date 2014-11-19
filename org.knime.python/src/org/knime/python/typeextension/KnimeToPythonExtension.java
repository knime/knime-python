package org.knime.python.typeextension;

import org.knime.core.data.DataValue;

public class KnimeToPythonExtension {

	private String m_id;
	private String m_pythonDeserializerPath;
	private Serializer<? extends DataValue> m_javaSerializer;
	
	public KnimeToPythonExtension(String id, String pythonDeserializerPath, Serializer<? extends DataValue> javaSerializer) {
		m_id = id;
		m_pythonDeserializerPath = pythonDeserializerPath;
		m_javaSerializer = javaSerializer;
	}
	
	public String getId() {
		return m_id;
	}

	public String getPythonDeserializerPath() {
		return m_pythonDeserializerPath;
	}
	
	public Serializer<? extends DataValue> getJavaSerializer() {
		return m_javaSerializer;
	}
	
}
