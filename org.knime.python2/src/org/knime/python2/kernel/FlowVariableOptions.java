package org.knime.python2.kernel;

import java.util.Map;

import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonPreferencePage;

public class FlowVariableOptions {
	
	private boolean m_overrulePreferencePage = false;
	
	private String m_serializerId = null;
	
	public FlowVariableOptions() {
		
	}
	
	public boolean getOverrulePreferencePage() {
		return m_overrulePreferencePage;
	}

	public void setOverrulePreferencePage(boolean overrulePreferencePage) {
		this.m_overrulePreferencePage = overrulePreferencePage;
	}

	public String getSerializerId() {
		return m_serializerId;
	}

	public void setSerializerId(String serializerId) {
		this.m_serializerId = serializerId;
	}
	
	public static FlowVariableOptions parse(Map<String, FlowVariable> map) {
		FlowVariableOptions options = new FlowVariableOptions();
		FlowVariable fv = map.get("python_serialization_library");
	    String serLib = (fv == null) ? null:fv.getStringValue();
		if( serLib != null && PythonPreferencePage.getAvailableSerializerIds().contains(serLib) ) {
			options.setOverrulePreferencePage(true);
			options.setSerializerId(serLib);
		}
		return options;
	}

}
