/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   May 21, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.util.List;
import java.util.Map;

import org.knime.core.node.workflow.ICredentials;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.proxy.PythonNodeViewProxy;
import org.knime.python3.nodes.proxy.PythonNodeViewProxy.PythonDataServiceProxy.PythonViewData;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.CredentialsProviderProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.PortMapProvider;

/**
 * Default implementation of the {@link PythonNodeViewProxy.PythonViewContext} interface.
 *
 * Used by the CloseablePythonNodeProxy to provide the context for Python views.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class DefaultViewContext implements PythonNodeViewProxy.PythonViewContext {

    private final ToolExecutor m_toolExecutor;

    private final PortMapProvider m_portMapProvider;

    private final CredentialsProviderProxy m_credentialsProvider;

    private final PythonViewData m_viewData;

    DefaultViewContext(final ToolExecutor toolExecutor, final PortMapProvider portMapProvider,
        final CredentialsProviderProxy credentialsProvider, final PythonViewData viewData) {
        m_toolExecutor = toolExecutor;
        m_portMapProvider = portMapProvider;
        m_credentialsProvider = credentialsProvider;
        m_viewData = viewData;
    }

    @Override
    public String[] get_credentials(final String identifier) {
        ICredentials credentials = m_credentialsProvider.getCredentials(identifier);
        return new String[]{credentials.getLogin(), credentials.getPassword(), credentials.getName()};
    }

    @Override
    public String[] get_credential_names() {
        return m_credentialsProvider.getCredentialNames();

    }

    @Override
    public Map<String, int[]> get_input_port_map() {
        return m_portMapProvider.getInputPortMap();
    }

    @Override
    public Map<String, int[]> get_output_port_map() {
        return m_portMapProvider.getOutputPortMap();
    }

    @Override
    public PythonToolResult execute_tool(final PurePythonTablePortObject toolTable, final String parameters,
        final List<PythonPortObject> inputs, final Map<String, String> executionHints) {
        return m_toolExecutor.executeTool(toolTable, parameters, inputs, executionHints);
    }

    @Override
    public CombinedToolsWorkflowInfo init_combined_tools_workflow(final List<PythonPortObject> inputs,
        final String execMode) {
        return m_toolExecutor.initCombinedToolsWorkflow(inputs, execMode);
    }

    @Override
    public PythonPortObject get_combined_tools_workflow() {
        throw new UnsupportedOperationException(
            "Getting the combined-tools workflow is not supported from within a view context.");
    }

    @Override
    public PythonToolResult execute_tool_in_combined_workflow(final PurePythonTablePortObject toolTable, final String parameters,
        final List<String> inputIds, final Map<String, String> executionHints) {
        return m_toolExecutor.executeToolInCombinedWorkflow(toolTable, parameters, inputIds, executionHints);
    }

    @Override
    public PythonViewData get_view_data() {
        return m_viewData;
    }
}
