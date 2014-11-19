package org.knime.python.typeextension;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;

public class TypeExtensions {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(TypeExtensions.class);
	
	private static List<KnimeToPythonExtension> knimeToPythonExtensions;
	private static Map<String, PythonToKnimeExtension> pythonToKnimeExtensions;
		
	@SuppressWarnings({ "unchecked" })
	public static void init() {
		knimeToPythonExtensions = new ArrayList<KnimeToPythonExtension>();
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.knimetopython");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-serializer");
				if (o instanceof Serializer) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-deserializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						Serializer<? extends DataValue> serializer = (Serializer<? extends DataValue>) o;
						String id = config.getAttribute("id");
						knimeToPythonExtensions.add(new KnimeToPythonExtension(id, file.getAbsolutePath(), serializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		pythonToKnimeExtensions = new HashMap<String, PythonToKnimeExtension>();
		configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.pythontoknime");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-deserializer");
				if (o instanceof Deserializer) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-serializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						Deserializer<? extends DataValue> deserializer = (Deserializer<? extends DataValue>) o;
						String id = config.getAttribute("id");
						pythonToKnimeExtensions.put(id, new PythonToKnimeExtension(id, config.getAttribute("python-type-identifier"), file.getAbsolutePath(), deserializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	public static Collection<KnimeToPythonExtension> getKnimeToPythonExtensions() {
		return knimeToPythonExtensions;
	}
	
	public static Collection<PythonToKnimeExtension> getPythonToKnimeExtensions() {
		return pythonToKnimeExtensions.values();
	}

	public static KnimeToPythonExtension getKnimeToPythonExtension(final DataType type) {
		for (KnimeToPythonExtension extension : knimeToPythonExtensions) {
			Class<? extends DataValue> preferredValueClass = type.getPreferredValueClass();
			if (preferredValueClass != null && preferredValueClass.equals(extension.getJavaSerializer().getDataValue())) {
				return extension;
			}
		}
		for (KnimeToPythonExtension extension : knimeToPythonExtensions) {
			if (extension.getJavaSerializer().isCompatible(type)) {
				return extension;
			}
		}
		return null;
	}

	public static PythonToKnimeExtension getPythonToKnimeExtension(final String id) {
		return pythonToKnimeExtensions.get(id);
	}

}
