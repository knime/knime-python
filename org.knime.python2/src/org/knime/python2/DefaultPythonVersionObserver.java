package org.knime.python2;

public interface DefaultPythonVersionObserver {
	
	public void notifyChange(DefaultPythonVersionOption option);
	
	public void addOption(DefaultPythonVersionOption option);

}
