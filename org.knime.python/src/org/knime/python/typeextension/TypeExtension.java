package org.knime.python.typeextension;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.python.Activator;

public class TypeExtension {

	private static Map<String, TypeExtension> typeExtensions;
	
	private String m_id;
	private String m_type;
	private String m_pythonSerializerPath;
	private Serializer<? extends DataValue> m_javaSerializer;

	public TypeExtension(final String id, final String type, final String pythonSerializerPath, final Serializer<? extends DataValue> javaSerializer) {
		m_id = id;
		m_type = type;
		m_pythonSerializerPath = pythonSerializerPath;
		m_javaSerializer = javaSerializer;
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
	
	public Serializer<? extends DataValue> getJavaSerializer() {
		return m_javaSerializer;
	}
	
	@SuppressWarnings({ "unchecked" })
	public static void init() {
		typeExtensions = new HashMap<String, TypeExtension>();
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-serializer");
				if (o instanceof Serializer) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-serializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						Serializer<? extends DataValue> serializer = (Serializer<? extends DataValue>) o;
						String id = config.getAttribute("id");
						typeExtensions.put(id, new TypeExtension(id, config
								.getAttribute("python-type"), file.getAbsolutePath(), serializer));
					}
				}
			} catch (CoreException e) {
				e.printStackTrace();
			}
		}
	}

	public static Collection<TypeExtension> getTypeExtensions() {
		return typeExtensions.values();
	}

	public static TypeExtension getTypeExtension(final DataType type) {
		for (TypeExtension typeExtension : typeExtensions.values()) {
			if (type.getPreferredValueClass().equals(typeExtension.getJavaSerializer().getDataValue())) {
				return typeExtension;
			}
		}
		for (TypeExtension typeExtension : typeExtensions.values()) {
			if (typeExtension.getJavaSerializer().isCompatible(type)) {
				return typeExtension;
			}
		}
		return null;
	}

	public static TypeExtension getTypeExtension(final String id) {
		return typeExtensions.get(id);
	}

}
