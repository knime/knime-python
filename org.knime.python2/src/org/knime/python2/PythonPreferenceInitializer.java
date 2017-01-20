package org.knime.python2;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

public class PythonPreferenceInitializer extends AbstractPreferenceInitializer {

	/**
	 * Use the command 'python' without a specified location as default
	 */
	public static final String DEFAULT_PYTHON_2_PATH = "python";
	
	/**
	 * Use the command 'python3' without a specified location as default
	 */
	public static final String DEFAULT_PYTHON_3_PATH = "python3";
	
	public static final String DEFAULT_SERIALIZER_ID = "org.knime.serialization.flatbuffers.column";

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPreferenceInitializer.class);

	@Override
	public void initializeDefaultPreferences() {
		IEclipsePreferences prefs = DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID);
		prefs.put(PythonPreferencePage.PYTHON_2_PATH_CFG, DEFAULT_PYTHON_2_PATH);
		prefs.put(PythonPreferencePage.PYTHON_3_PATH_CFG, DEFAULT_PYTHON_3_PATH);
		prefs.put(PythonPreferencePage.SERIALIZER_ID_CFG, DEFAULT_SERIALIZER_ID);
		try {
			prefs.flush();
		} catch (BackingStoreException e) {
			LOGGER.error("Could not save preferences: " + e.getMessage(), e);
		}
	}

}
