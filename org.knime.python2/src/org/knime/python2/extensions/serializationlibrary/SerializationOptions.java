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

package org.knime.python2.extensions.serializationlibrary;

import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 * Options for controlling the serialization process. Missing values for Int and Long columns
 * may be replaced by a sentinel value in order to avoid automatic conversion to Double in python.
 * The sentinel may be replaced with missing values for data coming from python.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class SerializationOptions {
	
	public final static boolean DEFAULT_CONVERT_MISSING_TO_PYTHON = false;
	public final static boolean DEFAULT_CONVERT_MISSING_FROM_PYTHON = false;
	public final static SentinelOption DEFAULT_SENTINEL_OPTION = SentinelOption.MIN_VAL;
	public final static int DEFAULT_SENTINEL_VALUE = 0;
	
	private boolean m_convertMissingToPython = DEFAULT_CONVERT_MISSING_TO_PYTHON;
	private boolean m_convertMissingFromPython = DEFAULT_CONVERT_MISSING_FROM_PYTHON;
	private SentinelOption m_sentinelOption = DEFAULT_SENTINEL_OPTION;
	private int m_sentinelValue = DEFAULT_SENTINEL_VALUE;
	
	public SerializationOptions() {
		
	}
	
	public SerializationOptions(boolean usePython3, boolean convertMissingToPython, 
			boolean convertMissingFromPython, SentinelOption sentinelOption, int sentinelValue) {
		m_convertMissingFromPython = convertMissingFromPython;
		m_convertMissingToPython = convertMissingToPython;
		m_sentinelOption = sentinelOption;
		m_sentinelValue = sentinelValue;
	}
	
	public SerializationOptions(SerializationOptions other)
	{
		m_convertMissingFromPython = other.getConvertMissingFromPython();
		m_convertMissingToPython = other.getConvertMissingToPython();
		m_sentinelOption = other.getSentinelOption();
		m_sentinelValue = other.getSentinelValue();
	}
	
	public boolean getConvertMissingToPython() {
		return m_convertMissingToPython;
	}

	public void setConvertMissingToPython(boolean m_convertMissingToPython) {
		this.m_convertMissingToPython = m_convertMissingToPython;
	}

	public boolean getConvertMissingFromPython() {
		return m_convertMissingFromPython;
	}

	public void setConvertMissingFromPython(boolean m_convertMissingFromPython) {
		this.m_convertMissingFromPython = m_convertMissingFromPython;
	}

	public SentinelOption getSentinelOption() {
		return m_sentinelOption;
	}

	public void setSentinelOption(SentinelOption m_sentinelOption) {
		this.m_sentinelOption = m_sentinelOption;
	}

	public int getSentinelValue() {
		return m_sentinelValue;
	}

	public void setSentinelValue(int m_sentinelValue) {
		this.m_sentinelValue = m_sentinelValue;
	}
	
	/**
	 * Return the sentinel value for the given type.
	 * @param type	a {@Type} (either INTEGER or LONG)
	 * @return	the sentinel value based on the stored options
	 * @throws IllegalArgumentException if type cannot be processed
	 */
	
	public long getSentinelForType(Type type) throws IllegalArgumentException {
		if(m_sentinelOption == SentinelOption.CUSTOM)
			return m_sentinelValue;
		else if(m_sentinelOption == SentinelOption.MAX_VAL) {
			if(type == Type.INTEGER) {
				return Integer.MAX_VALUE;
			} else if(type == Type.LONG) {
				return Long.MAX_VALUE;
			}
		} else {
			if(type == Type.INTEGER) {
				return Integer.MIN_VALUE;
			} else if(type == Type.LONG) {
				return Long.MIN_VALUE;
			}
		}
		throw new IllegalArgumentException("Sentinel does not exist for type " + type.toString());
	}
	
	/**
	 * Indicate if the given value equals the sentinel value for the given type.
	 * @param type 	a {@Type} (INTEGER or LONG)
	 * @param value	the value to check
	 * @return	value == sentinel based on the stored options and the type
	 */
	public boolean isSentinel(Type type, long value) {
		if(m_sentinelOption == SentinelOption.CUSTOM)
			return value == m_sentinelValue;
		else if(m_sentinelOption == SentinelOption.MAX_VAL) {
			if(type == Type.INTEGER) {
				return value == Integer.MAX_VALUE;
			} else if(type == Type.LONG) {
				return value == Long.MAX_VALUE;
			}
		} else {
			if(type == Type.INTEGER) {
				return value == Integer.MIN_VALUE;
			} else if(type == Type.LONG) {
				return value == Long.MIN_VALUE;
			}
		}
		return false;
	}
}
