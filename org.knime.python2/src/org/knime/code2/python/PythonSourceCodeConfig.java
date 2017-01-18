package org.knime.code2.python;

import org.knime.code2.generic.SourceCodeConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

public class PythonSourceCodeConfig extends SourceCodeConfig {
	
	private static final String CFG_USE_PYTHON3 = "usePython3";
	private static final boolean DEFAULT_USE_PYTHON3 = true;
	private boolean m_usePython3 = DEFAULT_USE_PYTHON3;
	
	@Override
	public void saveTo(NodeSettingsWO settings) {
		super.saveTo(settings);
		settings.addBoolean(CFG_USE_PYTHON3, m_usePython3);
	}
	
	@Override
	public void loadFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		super.loadFrom(settings);
		m_usePython3 = settings.getBoolean(CFG_USE_PYTHON3);
	}
	
	@Override
	public void loadFromInDialog(NodeSettingsRO settings) {
		super.loadFromInDialog(settings);
		m_usePython3 = settings.getBoolean(CFG_USE_PYTHON3, DEFAULT_USE_PYTHON3);
	}
	
	public boolean getUsePython3() {
		return m_usePython3;
	}
	
	public void setUsePython3(final boolean usePython3) {
		m_usePython3 = usePython3;
	}
	
	

}
