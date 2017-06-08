/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */

package org.knime.python2.kernel;

import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;

/**
 * Options for the PythonKernel. Includes {@link SerializationOptions} and the python version
 * that should be used.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class PythonKernelOptions {
	
	public final static boolean DEFAULT_USE_PYTHON3 = true;
		
	private boolean m_usePython3 = DEFAULT_USE_PYTHON3;
	
	private SerializationOptions m_serializationOptions = new SerializationOptions();
	
	private FlowVariableOptions m_flowVariableOptions = new FlowVariableOptions();
	
	public PythonKernelOptions() {
		
	}
	
	public PythonKernelOptions(boolean usePython3, boolean convertMissingToPython, 
			boolean convertMissingFromPython, SentinelOption sentinelOption, int sentinelValue) {
		m_usePython3 = usePython3;
		m_serializationOptions.setConvertMissingFromPython(convertMissingFromPython);
		m_serializationOptions.setConvertMissingToPython(convertMissingToPython);
		m_serializationOptions.setSentinelOption(sentinelOption);
		m_serializationOptions.setSentinelValue(sentinelValue);
	}
	
	public PythonKernelOptions(PythonKernelOptions other)
	{
		this(other.getUsePython3(), other.getConvertMissingToPython(), other.getConvertMissingFromPython(), 
				other.getSentinelOption(), other.getSentinelValue());
	}

	public boolean getUsePython3() {
		return m_usePython3;
	}

	public void setUsePython3(boolean m_usePython3) {
		this.m_usePython3 = m_usePython3;
	}

	public boolean getConvertMissingToPython() {
		return m_serializationOptions.getConvertMissingToPython();
	}

	public void setConvertMissingToPython(boolean m_convertMissingToPython) {
		this.m_serializationOptions.setConvertMissingToPython(m_convertMissingToPython);
	}

	public boolean getConvertMissingFromPython() {
		return m_serializationOptions.getConvertMissingFromPython();
	}

	public void setConvertMissingFromPython(boolean m_convertMissingFromPython) {
		this.m_serializationOptions.setConvertMissingFromPython(m_convertMissingFromPython);
	}

	public SentinelOption getSentinelOption() {
		return m_serializationOptions.getSentinelOption();
	}

	public void setSentinelOption(SentinelOption m_sentinelOption) {
		this.m_serializationOptions.setSentinelOption(m_sentinelOption);
	}

	public int getSentinelValue() {
		return m_serializationOptions.getSentinelValue();
	}

	public void setSentinelValue(int m_sentinelValue) {
		this.m_serializationOptions.setSentinelValue( m_sentinelValue);
	}
	
	public SerializationOptions getSerializationOptions() {
		return m_serializationOptions;
	}
	
	public boolean getOverrulePreferencePage() {
		return m_flowVariableOptions.getOverrulePreferencePage();
	}

	public void setOverrulePreferencePage(boolean overrulePreferencePage) {
		m_flowVariableOptions.setOverrulePreferencePage(overrulePreferencePage);
	}

	public String getSerializerId() {
		return m_flowVariableOptions.getSerializerId();
	}

	public void setSerializerId(String serializerId) {
		m_flowVariableOptions.setSerializerId(serializerId);
	}
	
	public FlowVariableOptions getFlowVariableOptions() {
		return m_flowVariableOptions;
	}
	
	public void setFlowVariableOptions(FlowVariableOptions options) {
		m_flowVariableOptions = options;
	}

}
