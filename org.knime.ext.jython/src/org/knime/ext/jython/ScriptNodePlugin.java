/* @(#)$RCSfile$ 
 * $Revision$ $Date$ $Author$
 *
 */
package org.knime.ext.jython;

import java.util.ArrayList;
import java.util.Iterator;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtension;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.Plugin;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleContext;

/**
 * This is the eclipse bundle activator.
 * 
 * @author Tripos
 */
public class ScriptNodePlugin extends Plugin
{

	/** Make sure that this *always* matches the ID in plugin.xml. */
	public static final String PLUGIN_ID = "org.knime.ext.jython.script11";

	// The shared instance.
	private static ScriptNodePlugin plugin;
	private static NodeLogger logger = NodeLogger.getLogger(ScriptNodePlugin.class);

	/**
	 * The constructor.
	 */
	public ScriptNodePlugin()
	{
		super();
		plugin = this;
	}

	/**
	 * This method is called upon plug-in activation.
	 * 
	 * @param context
	 *            The OSGI bundle context
	 * @throws Exception
	 *             If this plugin could not be started
	 */
	public void start(final BundleContext context) throws Exception
	{
		super.start(context);

        loadClasspathExtensions();    
	}
	
	private void loadClasspathExtensions() throws Exception {

	    IExtensionPoint pt = 
			Platform
			.getExtensionRegistry()
			.getExtensionPoint("org.knime.ext.jython.classpath");
		IExtension[] extensions = pt.getExtensions();       
    
	    ArrayList<Object> classpathExtensions = new ArrayList<Object>();
	    for (int i=0; i < extensions.length; i++) {
	    	IConfigurationElement[] configElements = 
	    		extensions[i].getConfigurationElements();
	    	for (int j=0; j < configElements.length; j++) {

	    		IConfigurationElement configElement = configElements[j];
	    		
	    		try {
		    		if (configElement.getName().equals("classpath")) {
		    			Object extensionClass = configElement.createExecutableExtension("class");
		    			classpathExtensions.add(extensionClass);
		    		}    
	    		} catch (Exception e) {
	    			logger.debug("Could not load contributed classpath jython extension: " +
	    					configElement);
	    		}
	    	}
	    }
	    
	    Iterator iter = classpathExtensions.iterator();
	    StringBuffer javaExtDirExtensions = new StringBuffer();
	    StringBuffer javaClasspathExtensions = new StringBuffer();
	    String pathSep = System.getProperty("path.separator");
	    while (iter.hasNext()) {
	    	IClasspathExtension extension = (IClasspathExtension) iter.next();
	    	
	    	String[] javaExtDirsList = extension.getJavaExtDirs();   	
	    	for (int i=0; i < javaExtDirsList.length; i++) {
	    		javaExtDirExtensions.append(pathSep + javaExtDirsList[i]);
	    	}
	    	
	    	String[] javaClasspathEntriesList = extension.getJavaClasspathEntries();
	    	for (int i=0; i < javaClasspathEntriesList.length; i++) {
	    		javaClasspathExtensions.append(pathSep + javaClasspathEntriesList[i]);
	    	}
	    }
	    
	    PythonScriptNodeModel.setJavaExtDirsExtensionPath(javaExtDirExtensions.toString());
	    PythonScriptNodeModel.setJavaClasspathExtensionPath(javaClasspathExtensions.toString());
	}

	/**
	 * This method is called when the plug-in is stopped.
	 * 
	 * @param context
	 *            The OSGI bundle context
	 * @throws Exception
	 *             If this plugin could not be stopped
	 */
	public void stop(final BundleContext context) throws Exception
	{
		super.stop(context);
		plugin = null;
	}

	/**
	 * Returns the shared instance.
	 * 
	 * @return Singleton instance of the Plugin
	 */
	public static ScriptNodePlugin getDefault()
	{
		return plugin;
	}

}
