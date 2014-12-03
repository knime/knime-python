package org.knime.python.typeextension;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;

public class PythonToKnimeExtensions {

	private static Map<String, PythonToKnimeExtension> extensions = new HashMap<String, PythonToKnimeExtension>();
	private Map<String, Deserializer> m_deserializers = new HashMap<String, Deserializer>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonToKnimeExtensions.class);

	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.pythontoknime");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-deserializer");
				if (o instanceof DeserializerFactory) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-serializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						DeserializerFactory deserializer = (DeserializerFactory) o;
						String id = config.getAttribute("id");
						extensions.put(id, new PythonToKnimeExtension(id, config.getAttribute("python-type-identifier"), file.getAbsolutePath(), deserializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}
	
	public Deserializer getDeserializer(final String id) {
		if (!m_deserializers.containsKey(id)) {
			m_deserializers.put(id, extensions.get(id).getJavaDeserializerFactory().createDeserializer());
		}
		return m_deserializers.get(id);
	}
	
	public static PythonToKnimeExtension getExtension(final String id) {
		return extensions.get(id);
	}
	
	public static Collection<PythonToKnimeExtension> getExtensions() {
		return extensions.values();
	}
	
}
