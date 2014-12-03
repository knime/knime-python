package org.knime.python.typeextension;


public class PythonToKnimeExtension {

	private String m_id;
	private String m_type;
	private String m_pythonSerializerPath;
	private DeserializerFactory m_javaDeserializerFactory;
	
	PythonToKnimeExtension(String id, String type, String pythonSerializerPath, DeserializerFactory javaDeserializer) {
		m_id = id;
		m_type = type;
		m_pythonSerializerPath = pythonSerializerPath;
		m_javaDeserializerFactory = javaDeserializer;
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
	
	public DeserializerFactory getJavaDeserializerFactory() {
		return m_javaDeserializerFactory;
	}

}
