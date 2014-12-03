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
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;

public class KnimeToPythonExtensions {
	
	private static Map<String, KnimeToPythonExtension> extensions = new HashMap<String, KnimeToPythonExtension>();
	private Map<String, Serializer<? extends DataValue>> m_serializers = new HashMap<String, Serializer<? extends DataValue>>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(KnimeToPythonExtensions.class);
		
	@SuppressWarnings({ "unchecked" })
	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.knimetopython");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-serializer-factory");
				if (o instanceof SerializerFactory) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-deserializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						SerializerFactory<? extends DataValue> serializer = (SerializerFactory<? extends DataValue>) o;
						String id = config.getAttribute("id");
						extensions.put(id, new KnimeToPythonExtension(id, file.getAbsolutePath(), serializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}
	
	public Serializer<? extends DataValue> getSerializer(final String id) {
		if (!m_serializers.containsKey(id)) {
			m_serializers.put(id, extensions.get(id).getJavaSerializerFactory().createSerializer());
		}
		return m_serializers.get(id);
	}
	
	public static KnimeToPythonExtension getExtension(final DataType type) {
		for (KnimeToPythonExtension extension : extensions.values()) {
			Class<? extends DataValue> preferredValueClass = type.getPreferredValueClass();
			if (preferredValueClass != null && preferredValueClass.equals(extension.getJavaSerializerFactory().getDataValue())) {
				return extension;
			}
		}
		for (KnimeToPythonExtension extension : extensions.values()) {
			if (extension.getJavaSerializerFactory().isCompatible(type)) {
				return extension;
			}
		}
		return null;
	}
	
	public static Collection<KnimeToPythonExtension> getExtensions() {
		return extensions.values();
	}

}
