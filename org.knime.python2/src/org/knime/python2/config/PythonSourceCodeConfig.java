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

package org.knime.python2.config;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelOptions.PythonVersionOption;

/**
 * A basic config for every node concerned with python scripting.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class PythonSourceCodeConfig extends SourceCodeConfig {

    private static final String CFG_PYTHON_VERSION_OPTION = "pythonVersionOption";

    private static final String CFG_CONVERT_MISSING_TO_PYTHON = "convertMissingToPython";

    private static final String CFG_CONVERT_MISSING_FROM_PYTHON = "convertMissingFromPython";

    private static final String CFG_SENTINEL_OPTION = "sentinelOption";

    private static final String CFG_SENTINEL_VALUE = "sentinelValue";

    private PythonKernelOptions m_kernelOptions = new PythonKernelOptions();

    @Override
    public void saveTo(final NodeSettingsWO settings) {
        super.saveTo(settings);
        settings.addString(CFG_PYTHON_VERSION_OPTION, m_kernelOptions.getPythonVersionOption().name());
        settings.addBoolean(CFG_CONVERT_MISSING_TO_PYTHON, m_kernelOptions.getConvertMissingToPython());
        settings.addBoolean(CFG_CONVERT_MISSING_FROM_PYTHON, m_kernelOptions.getConvertMissingFromPython());
        settings.addString(CFG_SENTINEL_OPTION, m_kernelOptions.getSentinelOption().name());
        settings.addInt(CFG_SENTINEL_VALUE, m_kernelOptions.getSentinelValue());
    }

    @Override
    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadFrom(settings);
        m_kernelOptions.setPythonVersionOption(PythonVersionOption.valueOf(
            settings.getString(CFG_PYTHON_VERSION_OPTION, m_kernelOptions.getPreferencePythonVersion().name())));
        m_kernelOptions.setConvertMissingToPython(
            settings.getBoolean(CFG_CONVERT_MISSING_TO_PYTHON, SerializationOptions.DEFAULT_CONVERT_MISSING_TO_PYTHON));
        m_kernelOptions.setConvertMissingFromPython(settings.getBoolean(CFG_CONVERT_MISSING_FROM_PYTHON,
            SerializationOptions.DEFAULT_CONVERT_MISSING_FROM_PYTHON));
        m_kernelOptions.setSentinelOption(SentinelOption
            .valueOf(settings.getString(CFG_SENTINEL_OPTION, SerializationOptions.DEFAULT_SENTINEL_OPTION.name())));
        m_kernelOptions
        .setSentinelValue(settings.getInt(CFG_SENTINEL_VALUE, SerializationOptions.DEFAULT_SENTINEL_VALUE));
    }

    @Override
    public void loadFromInDialog(final NodeSettingsRO settings) {
        super.loadFromInDialog(settings);
        m_kernelOptions.setPythonVersionOption(PythonVersionOption.valueOf(
            settings.getString(CFG_PYTHON_VERSION_OPTION, m_kernelOptions.getPreferencePythonVersion().name())));
        m_kernelOptions.setConvertMissingToPython(
            settings.getBoolean(CFG_CONVERT_MISSING_TO_PYTHON, SerializationOptions.DEFAULT_CONVERT_MISSING_TO_PYTHON));
        m_kernelOptions.setConvertMissingFromPython(settings.getBoolean(CFG_CONVERT_MISSING_FROM_PYTHON,
            SerializationOptions.DEFAULT_CONVERT_MISSING_FROM_PYTHON));
        m_kernelOptions.setSentinelOption(SentinelOption
            .valueOf(settings.getString(CFG_SENTINEL_OPTION, SerializationOptions.DEFAULT_SENTINEL_OPTION.name())));
        m_kernelOptions
        .setSentinelValue(settings.getInt(CFG_SENTINEL_VALUE, SerializationOptions.DEFAULT_SENTINEL_VALUE));
    }

    /**
     * Sets the internal {@link PythonKernelOptions} to a new object created using the specified parameters.
     *
     * @param versionOption the version options
     * @param convertToPython convert missing values to sentinel on the way to python
     * @param convertFromPython convert sentinel to missing values on the way from python to KNIME
     * @param sentinelOption the sentinel option
     * @param sentinelValue the sentinel value (only used if sentinelOption is CUSTOM)
     */
    public void setKernelOptions(final PythonVersionOption versionOption, final boolean convertToPython,
        final boolean convertFromPython, final SentinelOption sentinelOption, final int sentinelValue) {
        m_kernelOptions =
                new PythonKernelOptions(versionOption, convertToPython, convertFromPython, sentinelOption, sentinelValue);
    }

    /**
     * Gets the python kernel options.
     *
     * @return the python kernel options
     */
    public PythonKernelOptions getKernelOptions() {
        return new PythonKernelOptions(m_kernelOptions);
    }

    /**
     * Indicates if the use of python 3 is configured.
     *
     * @return use python3 yes/no
     */
    public boolean getUsePython3() {
        return m_kernelOptions.getUsePython3();
    }

}
