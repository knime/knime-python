package org.knime.code.generic.templates;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python.Activator;

public class SourceCodeTemplatesExtensions {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(SourceCodeTemplatesExtensions.class);
	
	private static List<File> templateFolders;
	
	public static void init() {
		templateFolders = new ArrayList<File>();
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.sourcecodetemplates");
		for (IConfigurationElement config : configs) {
			String pluginId = config.getContributor().getName();
			String path = config.getAttribute("path");
			File folder = Activator.getFile(pluginId, path);
			if (folder != null && folder.isDirectory()) {
				templateFolders.add(folder);
			} else {
				LOGGER.error("Could not find templates folder " + path + " in plugin " + pluginId);
			}
		}
	}
	
	public static List<File> getTemplateFolders() {
		return templateFolders;
	}

}
