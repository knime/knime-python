package org.knime.python.kernel;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;
import org.knime.python.typeextension.TypeExtensions;

public class PythonModuleExtensions {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(TypeExtensions.class);
	
	private static Map<String, String> pythonModules;
	
	public static void init() {
		pythonModules = new HashMap<String, String>();
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.module");
		for (IConfigurationElement config : configs) {
			String pluginId = config.getContributor().getName();
			String path = config.getAttribute("path");
			String name = path.contains("/") ? path.substring(path.lastIndexOf("/") + 1, path.length()) : path;
			name = name.substring(0, name.lastIndexOf('.'));
			File file = Activator.getFile(pluginId, path);
			if (file == null || !file.exists() || file.isDirectory()) {
				LOGGER.error("Could not find the file " + path + " in plugin " + pluginId);
			} else if (pythonModules.containsKey(name)) {
				LOGGER.error("A module with the name " + name + " is already registered, ignoring the new one");
			} else {
				pythonModules.put(name, file.getAbsolutePath());
			}
		}
	}
	
	public static Collection<String> getPythonModules() {
		return pythonModules.values();
	}

}
