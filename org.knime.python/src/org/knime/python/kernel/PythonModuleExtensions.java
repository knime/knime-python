package org.knime.python.kernel;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;
import org.knime.python.typeextension.TypeExtensions;

public class PythonModuleExtensions {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(TypeExtensions.class);
	
	private static Set<String> pythonModulePaths;
	
	public static void init() {
		pythonModulePaths = new HashSet<String>();
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.modules");
		for (IConfigurationElement config : configs) {
			String pluginId = config.getContributor().getName();
			String path = config.getAttribute("path");
			File file = Activator.getFile(pluginId, path);
			if (file == null || !file.exists() || !file.isDirectory()) {
				LOGGER.error("Could not find the directory " + path + " in plugin " + pluginId);
			} else {
				pythonModulePaths.add(file.getAbsolutePath());
			}
		}
	}
	
	public static String getPythonPath() {
		return StringUtils.join(pythonModulePaths, ":");
	}

}
