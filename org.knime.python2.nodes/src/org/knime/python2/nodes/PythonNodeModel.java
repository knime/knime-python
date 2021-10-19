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

package org.knime.python2.nodes;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.knime.base.node.util.exttool.ExtToolOutputNodeModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.PythonFlowVariableOptions;
import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.config.PythonVersionAndCommandConfig;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelQueue;
import org.knime.python2.prefs.PythonPreferences;

/**
 * Base model for all python related nodes. Provides methods for loading and saving settings and for pushing a
 * collection of {@link FlowVariable}s to the stack.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @param <C> a configuration type
 */
public abstract class PythonNodeModel<C extends PythonSourceCodeConfig> extends ExtToolOutputNodeModel {

    private C m_scriptConfig = createConfig();

    private final PythonVersionAndCommandConfig m_executableConfig = new PythonVersionAndCommandConfig(
        PythonPreferences.getPythonVersionPreference(), PythonPreferences::getCondaInstallationPath,
        PythonPreferences::getPython2CommandPreference, PythonPreferences::getPython3CommandPreference);

    /**
     * Constructor.
     *
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    public PythonNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * Creates the config.
     *
     * @return the config
     */
    protected abstract C createConfig();

    /**
     * Gets the config.
     *
     * @return the config
     */
    protected final C getConfig() {
        return m_scriptConfig;
    }

    /**
     * Gets the kernel specific options.
     *
     * @return the kernel specific options
     */
    protected PythonKernelOptions getKernelOptions() {
        final PythonVersion pythonVersion = m_executableConfig.getPythonVersionConfig().getPythonVersion();
        final PythonCommand python2Command = m_executableConfig.getPython2CommandConfig().getCommand();
        final PythonCommand python3Command = m_executableConfig.getPython3CommandConfig().getCommand();

        final C config = getConfig();
        final String serializerId =
            new PythonFlowVariableOptions(getAvailableFlowVariables()).getSerializerId().orElse(null);
        final SerializationOptions serializationOptions =
            new SerializationOptions(config.getChunkSize(), config.isConvertingMissingToPython(),
                config.isConvertingMissingFromPython(), config.getSentinelOption(), config.getSentinelValue())
                    .forSerializerId(serializerId);

        return new PythonKernelOptions(pythonVersion, python2Command, python3Command, serializationOptions);
    }

    protected PythonKernel getNextKernelFromQueue(final PythonCancelable cancelable)
        throws PythonCanceledExecutionException, PythonIOException {
        return getNextKernelFromQueue(Collections.emptySet(), Collections.emptySet(), cancelable);
    }

    protected PythonKernel getNextKernelFromQueue(final Set<PythonModuleSpec> requiredAdditionalModules,
        final PythonCancelable cancelable) throws PythonCanceledExecutionException, PythonIOException {
        return getNextKernelFromQueue(requiredAdditionalModules, Collections.emptySet(), cancelable);
    }

    protected PythonKernel getNextKernelFromQueue(final Set<PythonModuleSpec> requiredAdditionalModules,
        final Set<PythonModuleSpec> optionalAdditionalModules, final PythonCancelable cancelable)
        throws PythonCanceledExecutionException, PythonIOException {
        final PythonKernelOptions options = getKernelOptions();
        final PythonCommand command = options.getUsePython3() //
            ? options.getPython3Command() //
            : options.getPython2Command();
        return PythonKernelQueue.getNextKernel(command, requiredAdditionalModules, optionalAdditionalModules, options,
            cancelable);
    }

    /**
     * Push new variables to the stack.
     *
     * Only pushes new variables to the stack if they are new or changed in type or value.
     *
     * @param newVariables The flow variables to push
     */
    protected void addNewVariables(final Collection<FlowVariable> newVariables) {
        final Map<String, FlowVariable> oldVariables =
            getAvailableFlowVariables(VariableTypeRegistry.getInstance().getAllTypes());
        for (final FlowVariable variable : newVariables) {
            if (!Objects.equals(oldVariables.get(variable.getName()), variable)) {
                pushNewFlowVariable(variable);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pushNewFlowVariable(final FlowVariable variable) {
        pushFlowVariable(variable.getName(), (VariableType)variable.getVariableType(),
            variable.getValue(variable.getVariableType()));
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_scriptConfig.saveTo(settings);
        m_executableConfig.getPythonVersionConfig().saveSettingsTo(settings);
        m_executableConfig.getPython2CommandConfig().saveSettingsTo(settings);
        m_executableConfig.getPython3CommandConfig().saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final C config = createConfig();
        config.loadFrom(settings);
        m_executableConfig.getPythonVersionConfig().loadSettingsFrom(settings);
        m_executableConfig.getPython2CommandConfig().loadSettingsFrom(settings);
        m_executableConfig.getPython3CommandConfig().loadSettingsFrom(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        final C config = createConfig();
        config.loadFrom(settings);
        m_scriptConfig = config;
        m_executableConfig.getPythonVersionConfig().loadSettingsFrom(settings);
        m_executableConfig.getPython2CommandConfig().loadSettingsFrom(settings);
        m_executableConfig.getPython3CommandConfig().loadSettingsFrom(settings);
    }
}
