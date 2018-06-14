/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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

import java.util.ArrayList;
import java.util.List;

import org.knime.python2.Activator;
import org.knime.python2.PythonPreferencePage;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;

/**
 * Options for the PythonKernel. Includes {@link SerializationOptions} and the python version that should be used.
 *
 * @author Clemens von Schwerin, KNIME, Konstanz, Germany
 *
 */

public class PythonKernelOptions {

    private PythonVersionOption m_usePython3;

    private SerializationOptions m_serializationOptions = new SerializationOptions();

    private FlowVariableOptions m_flowVariableOptions = new FlowVariableOptions();

    private List<String> m_additionalRequiredModules = new ArrayList<String>();

    private String m_kernelScriptPath;

    /**
     * The default number of rows to transfer per chunk.
     */
    public static final int DEFAULT_CHUNK_SIZE = 500000;

    private int m_chunkSize = DEFAULT_CHUNK_SIZE;

    /**
     * Default constructor. Consults the {@link PythonPreferencePage} for the default python version to use.
     */
    public PythonKernelOptions() {
        m_usePython3 = getPreferencePythonVersion();
        m_kernelScriptPath = Activator.getFile(Activator.PLUGIN_ID, "py/PythonKernelLauncher.py").getAbsolutePath();
    }

    /**
     * Constructor.
     *
     * @param usePython3 flag indicating if python3 is to be used
     * @param convertMissingToPython convert missing values to sentinel on the way to python
     * @param convertMissingFromPython convert sentinel to missing values on the way from python to KNIME
     * @param sentinelOption the sentinel option
     * @param sentinelValue the sentinel value (only used if sentinelOption is CUSTOM)
     * @param chunkSize the number of rows to transfer per chunk
     */
    public PythonKernelOptions(final PythonVersionOption usePython3, final boolean convertMissingToPython,
        final boolean convertMissingFromPython, final SentinelOption sentinelOption, final int sentinelValue,
        final int chunkSize) {
        m_usePython3 = usePython3;
        m_serializationOptions.setConvertMissingFromPython(convertMissingFromPython);
        m_serializationOptions.setConvertMissingToPython(convertMissingToPython);
        m_serializationOptions.setSentinelOption(sentinelOption);
        m_serializationOptions.setSentinelValue(sentinelValue);
        m_chunkSize = chunkSize;
        m_kernelScriptPath = Activator.getFile(Activator.PLUGIN_ID, "py/PythonKernelLauncher.py").getAbsolutePath();
    }

    /**
     * Copy constructor.
     *
     * @param other the options to copy
     */
    public PythonKernelOptions(final PythonKernelOptions other) {
        this(other.getPythonVersionOption(), other.getConvertMissingToPython(), other.getConvertMissingFromPython(),
            other.getSentinelOption(), other.getSentinelValue(), other.getChunkSize());
        this.m_serializationOptions = new SerializationOptions(other.getSerializationOptions());
        this.m_flowVariableOptions = new FlowVariableOptions(other.getFlowVariableOptions());
        this.m_additionalRequiredModules = new ArrayList<String>(other.getAdditionalRequiredModules());
        this.m_kernelScriptPath = other.getKernelScriptPath();
    }

    /**
     * Gets the python version option.
     *
     * @return the python version option
     */
    public PythonVersionOption getPythonVersionOption() {
        return m_usePython3;
    }

    /**
     * Add an additional required module. A check for that module is performed on kernel startup.
     *
     * @param name  the module name
     */
    public void addRequiredModule(final String name) {
        m_additionalRequiredModules.add(name);
    }

    /**
     * Returns a list of all additional required modules which are checked on kernel startup.
     *
     * @return a list of all additional required modules
     */
    public List<String> getAdditionalRequiredModules() {
        return m_additionalRequiredModules;
    }

    /**
     * Sets the python version option.
     *
     * @param usePython3 the new python version option
     */
    public void setPythonVersionOption(final PythonVersionOption usePython3) {
        this.m_usePython3 = usePython3;
    }

    /**
     * Gets the convert missing to python.
     *
     * @return the convert missing to python
     */
    public boolean getConvertMissingToPython() {
        return m_serializationOptions.getConvertMissingToPython();
    }

    /**
     * Sets the convert missing to python.
     *
     * @param convertMissingToPython the new convert missing to python
     */
    public void setConvertMissingToPython(final boolean convertMissingToPython) {
        this.m_serializationOptions.setConvertMissingToPython(convertMissingToPython);
    }

    /**
     * Gets the convert missing from python.
     *
     * @return the convert missing from python
     */
    public boolean getConvertMissingFromPython() {
        return m_serializationOptions.getConvertMissingFromPython();
    }

    /**
     * Sets the convert missing from python.
     *
     * @param convertMissingFromPython the new convert missing from python
     */
    public void setConvertMissingFromPython(final boolean convertMissingFromPython) {
        this.m_serializationOptions.setConvertMissingFromPython(convertMissingFromPython);
    }

    /**
     * Gets the sentinel option.
     *
     * @return the sentinel option
     */
    public SentinelOption getSentinelOption() {
        return m_serializationOptions.getSentinelOption();
    }

    /**
     * Sets the sentinel option.
     *
     * @param sentinelOption the new sentinel option
     */
    public void setSentinelOption(final SentinelOption sentinelOption) {
        this.m_serializationOptions.setSentinelOption(sentinelOption);
    }

    /**
     * Gets the sentinel value.
     *
     * @return the sentinel value
     */
    public int getSentinelValue() {
        return m_serializationOptions.getSentinelValue();
    }

    /**
     * Sets the sentinel value.
     *
     * @param sentinelValue the new sentinel value
     */
    public void setSentinelValue(final int sentinelValue) {
        this.m_serializationOptions.setSentinelValue(sentinelValue);
    }

    /**
     * Gets the serialization options.
     *
     * @return the serialization options
     */
    public SerializationOptions getSerializationOptions() {
        return m_serializationOptions;
    }

    /**
     * Gets the overrule preference page.
     *
     * @return the overrule preference page
     */
    public boolean getOverrulePreferencePage() {
        return m_flowVariableOptions.getOverrulePreferencePage();
    }

    /**
     * Sets the overrule preference page.
     *
     * @param overrulePreferencePage the new overrule preference page
     */
    public void setOverrulePreferencePage(final boolean overrulePreferencePage) {
        m_flowVariableOptions.setOverrulePreferencePage(overrulePreferencePage);
    }

    /**
     * Gets the serializer id.
     *
     * @return the serializer id
     */
    public String getSerializerId() {
        return m_flowVariableOptions.getSerializerId();
    }

    /**
     * Sets the serializer id.
     *
     * @param serializerId the new serializer id
     */
    public void setSerializerId(final String serializerId) {
        m_flowVariableOptions.setSerializerId(serializerId);
    }

    /**
     * Gets the flow variable options.
     *
     * @return the flow variable options
     */
    public FlowVariableOptions getFlowVariableOptions() {
        return m_flowVariableOptions;
    }

    /**
     * Sets the flow variable options.
     *
     * @param options the new flow variable options
     */
    public void setFlowVariableOptions(final FlowVariableOptions options) {
        m_flowVariableOptions = options;
    }

    /**
     * Enum holding supported python versions.
     */
    @SuppressWarnings("javadoc")
    public static enum PythonVersionOption {
        PYTHON2, PYTHON3;
    }

    /**
     * Gets the use python 3.
     *
     * @return the use python 3
     */
    public boolean getUsePython3() {
        if (m_usePython3 == PythonVersionOption.PYTHON3) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Get the default python version set in the preference page.
     *
     * @return the default python version
     */
    public PythonVersionOption getPreferencePythonVersion() {
        final String defaultPythonVersion = PythonPreferencePage.getDefaultPythonOption();
        if (defaultPythonVersion.contentEquals("python3")) {
            return PythonVersionOption.PYTHON3;
        } else if (defaultPythonVersion.contentEquals("python2")) {
            return PythonVersionOption.PYTHON2;
        } else {
            throw new IllegalStateException("No default python version available from preference page!");
        }
    }

    /**
     * Sets the chunk size.
     *
     * @param chunkSize the new chunk size
     */
    public void setChunkSize(final int chunkSize) {
        m_chunkSize = chunkSize;
    }

    /**
     * Gets the chunk size.
     *
     * @return the chunk size
     */
    public int getChunkSize() {
        return m_chunkSize;
    }

    /**
     * Gets the kernel script path.
     *
     * @return the kernel script path
     */
    public String getKernelScriptPath() {
        return m_kernelScriptPath;
    }

    /**
     * Sets the kernel script path.
     *
     * @param kernelScriptPath the new kernel script path
     */
    public void setKernelScriptPath(final String kernelScriptPath) {
        m_kernelScriptPath = kernelScriptPath;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_additionalRequiredModules == null) ? 0 : m_additionalRequiredModules.hashCode());
        result = prime * result + m_chunkSize;
        result = prime * result + ((m_flowVariableOptions == null) ? 0 : m_flowVariableOptions.hashCode());
        result = prime * result + ((m_serializationOptions == null) ? 0 : m_serializationOptions.hashCode());
        result = prime * result + ((m_usePython3 == null) ? 0 : m_usePython3.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PythonKernelOptions other = (PythonKernelOptions)obj;
        if (m_additionalRequiredModules == null) {
            if (other.m_additionalRequiredModules != null) {
                return false;
            }
        } else if (!m_additionalRequiredModules.equals(other.m_additionalRequiredModules)) {
            return false;
        }
        if (m_chunkSize != other.m_chunkSize) {
            return false;
        }
        if (m_flowVariableOptions == null) {
            if (other.m_flowVariableOptions != null) {
                return false;
            }
        } else if (!m_flowVariableOptions.equals(other.m_flowVariableOptions)) {
            return false;
        }
        if (m_serializationOptions == null) {
            if (other.m_serializationOptions != null) {
                return false;
            }
        } else if (!m_serializationOptions.equals(other.m_serializationOptions)) {
            return false;
        }
        if (m_usePython3 != other.m_usePython3) {
            return false;
        }
        return true;
    }

}
