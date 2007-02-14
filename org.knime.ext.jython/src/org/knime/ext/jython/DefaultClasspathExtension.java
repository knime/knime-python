package org.knime.ext.jython;

public class DefaultClasspathExtension implements IClasspathExtension {

	public String[]  getJavaExtDirs() {
		return new String[0];
	}
	
	public String[] getJavaClasspathEntries() {
		return new String[0];
	}
}
