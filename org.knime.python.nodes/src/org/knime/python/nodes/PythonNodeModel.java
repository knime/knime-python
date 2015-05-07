package org.knime.python.nodes;

import java.util.Collection;
import java.util.Map;

import org.knime.base.node.util.exttool.ExtToolOutputNodeModel;
import org.knime.code.generic.SourceCodeConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;

public abstract class PythonNodeModel<Config extends SourceCodeConfig> extends ExtToolOutputNodeModel {
	
	Config m_config = createConfig();
    
	public PythonNodeModel(final PortType[] inPortTypes,
            final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }
	
	protected abstract Config createConfig();
	
	protected final Config getConfig() {
		return m_config;
	}
	
	/**
	 * Push new variables to the stack.
	 * 
	 * Only pushes new variables to the stack if they are new or changed in type or value.
	 * 
	 * @param newVariables The flow variables to push
	 */
	protected void addNewVariables(Collection<FlowVariable> newVariables) {
		Map<String, FlowVariable> flowVariables = getAvailableFlowVariables();
        for (FlowVariable variable : newVariables) {
        	// Only push if variable is new or has changed type or value
        	boolean push = true;
        	if (flowVariables.containsKey(variable.getName())) {
        		// Old variable with the name exists
        		FlowVariable oldVariable = flowVariables.get(variable.getName());
        		if (oldVariable.getType().equals(variable.getType())) {
        			// Old variable has the same type
        			if (variable.getType().equals(Type.INTEGER)) {
        				if (oldVariable.getIntValue() == variable.getIntValue()) {
        					// Old variable has the same value
        					push = false;
        				}
        			} else if (variable.getType().equals(Type.DOUBLE)) {
        				if (new Double(oldVariable.getDoubleValue()).equals(new Double(variable.getDoubleValue()))) {
        					// Old variable has the same value
        					push = false;
        				}
        			} else if (variable.getType().equals(Type.STRING)) {
        				if (oldVariable.getStringValue().equals(variable.getStringValue())) {
        					// Old variable has the same value
        					push = false;
        				}
        			}
        		}
        	}
        	if (push) {
	            if (variable.getType().equals(Type.INTEGER)) {
	                pushFlowVariableInt(variable.getName(), variable.getIntValue());
	            } else if (variable.getType().equals(Type.DOUBLE)) {
	                pushFlowVariableDouble(variable.getName(), variable.getDoubleValue());
	            } else if (variable.getType().equals(Type.STRING)) {
	                pushFlowVariableString(variable.getName(), variable.getStringValue());
	            }
        	}
        }
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		m_config.saveTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		Config config = createConfig();
		config.loadFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		Config config = createConfig();
		config.loadFrom(settings);
		m_config = config;
	}

}
