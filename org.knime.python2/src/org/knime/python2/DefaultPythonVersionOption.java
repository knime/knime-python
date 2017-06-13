package org.knime.python2;

public interface DefaultPythonVersionOption {
	
	public boolean isSelected();
	
	public void updateDefaultPythonVersion(DefaultPythonVersionOption option);
	
	public void setObserver(DefaultPythonVersionObserver observer);
	
	public void notifyChange();

}
