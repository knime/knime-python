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
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.config.PythonFlowVariableOptions;
import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelQueue;

/**
 * Base model for all python related nodes. Provides methods for loading and saving settings and for pushing a
 * collection of {@link FlowVariable}s to the stack.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @param <C> a configuration type
 */
public abstract class PythonNodeModel<C extends PythonSourceCodeConfig> extends ExtToolOutputNodeModel {

    private C m_config = createConfig();

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
        return m_config;
    }

    /**
     * Gets the kernel specific options.
     *
     * @return the kernel specific options
     */
    protected PythonKernelOptions getKernelOptions() {
        final PythonKernelOptions options = getConfig().getKernelOptions();
        final String serializerId =
            new PythonFlowVariableOptions(getAvailableFlowVariables()).getSerializerId().orElse(null);
        return options.forSerializationOptions(options.getSerializationOptions().forSerializerId(serializerId));
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
        final Map<String, FlowVariable> flowVariables = getAvailableFlowVariables();
        for (final FlowVariable variable : newVariables) {
            // Only push if variable is new or has changed type or value
            boolean push = true;
            if (flowVariables.containsKey(variable.getName())) {
                // Old variable with the name exists
                final FlowVariable oldVariable = flowVariables.get(variable.getName());
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
                        if (Objects.equals(oldVariable.getStringValue(), variable.getStringValue())) {
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

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.saveTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final C config = createConfig();
        config.loadFrom(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        final C config = createConfig();
        config.loadFrom(settings);
        m_config = config;
    }
}
