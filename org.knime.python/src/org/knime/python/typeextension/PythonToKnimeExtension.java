package org.knime.python.typeextension;

import org.knime.core.data.DataValue;

public class PythonToKnimeExtension {

	private String m_id;
	private String m_type;
	private String m_pythonSerializerPath;
	private Deserializer<? extends DataValue> m_javaDeserializer;
	
	PythonToKnimeExtension(String id, String type, String pythonSerializerPath, Deserializer<? extends DataValue> javaDeserializer) {
		m_id = id;
		m_type = type;
		m_pythonSerializerPath = pythonSerializerPath;
		m_javaDeserializer = javaDeserializer;
	}

	public String getId() {
		return m_id;
	}

	public String getType() {
		return m_type;
	}

	public String getPythonSerializerPath() {
		return m_pythonSerializerPath;
	}
	
	public Deserializer<? extends DataValue> getJavaDeserializer() {
		return m_javaDeserializer;
	}

}
