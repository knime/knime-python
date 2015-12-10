package org.knime.python;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

public class PythonPreferenceInitializer extends AbstractPreferenceInitializer {

	/**
	 * Use the command 'python' without a specified location as default
	 */
	public static final String DEFAULT_PYTHON_PATH = "python";

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPreferenceInitializer.class);

	@Override
	public void initializeDefaultPreferences() {
		IEclipsePreferences prefs = DefaultScope.INSTANCE.getNode("org.knime.python");
		prefs.put("pythonPath", DEFAULT_PYTHON_PATH);
		try {
			prefs.flush();
		} catch (BackingStoreException e) {
			LOGGER.error("Could not save preferences: " + e.getMessage(), e);
		}
	}

}
