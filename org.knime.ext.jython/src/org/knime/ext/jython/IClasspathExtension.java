package org.knime.ext.jython;

public interface IClasspathExtension {
	public String[] getJavaExtDirs();
	public String[] getJavaClasspathEntries();
}
