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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.agentic.tool.ToolValue;
import org.knime.core.node.agentic.tool.ToolValue.ToolResult;
import org.knime.core.node.agentic.tool.WorkflowToolValue;
import org.knime.core.node.agentic.tool.WorkflowToolValue.WorkflowToolResult;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.gateway.impl.project.VirtualWorkflowProjects;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortTypeRegistry;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.proxy.PythonToolContext.PythonToolResult;

/**
 * Executes WorkflowTools used by agents.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ToolExecutor implements AutoCloseable {

    private final ExecutionContext m_exec;

    private NativeNodeContainer m_nodeContainer;

    private final PythonArrowTableConverter m_tableManager;

    private Collection<Runnable> m_onCloseRunnables;

    ToolExecutor(final ExecutionContext exec, final NativeNodeContainer nodeContainer,
        final PythonArrowTableConverter tableManager) {
        m_exec = exec;
        m_nodeContainer = nodeContainer;
        m_tableManager = tableManager;
        m_onCloseRunnables = new ArrayList<>();
    }

    PythonToolResult executeTool(final PurePythonTablePortObject pythonToolTable, final String parameters,
        final List<PythonPortObject> inputs, final Map<String, String> executionHints) {
        if (m_nodeContainer == null) {
            throw new IllegalStateException("ToolExecutor has already been closed");
        }
        // TODO AP-24410: Properly register output file stores
        Map<String, FileStore> dummyFileStoreMap = Map.of();
        var conversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableManager, m_exec);
        var inputPortObjects = inputs.stream()//
            .map(po -> PythonPortTypeRegistry.convertPortObjectFromPython(po, conversionContext))//
            .toArray(PortObject[]::new);

        var toolTable =
            (BufferedDataTable)PythonPortTypeRegistry.convertPortObjectFromPython(pythonToolTable, conversionContext);

        var tool = getTool(toolTable);

        NodeContext.pushContext(m_nodeContainer);
        var originalFileStoreHandler = m_nodeContainer.getNode().getFileStoreHandler();
        if (tool instanceof WorkflowToolValue && m_nodeContainer.getNodeContainerState().isExecuted()) {
            // Hack to make sure all nodes within the 'virtual workflow' used for the tool execution
            // use a suitable file store handler in case the host node is already executed (e.g. Agent Chat View).
            // We temporarily(!) replace the file store handler of the host node with a more suitable one (and make
            // sure its available via the WorkflowDataRepository) such that
            // FlowVirtualScopeContext.createFileStoreHandler returns it.
            final var temporaryFileStoreHandler =
                new NotInWorkflowWriteFileStoreHandler(UUID.randomUUID(), originalFileStoreHandler.getDataRepository());
            temporaryFileStoreHandler.open();
            m_nodeContainer.getNode().setFileStoreHandler(temporaryFileStoreHandler);
            // keep the temporary file store handler around until no more tool is executed - to still enable proper file store
            // exchange between java/python and different tools till then
            m_onCloseRunnables.add(() -> {
                temporaryFileStoreHandler.close();
                temporaryFileStoreHandler.clearAndDispose();
            });
        }
        try {
            var result = tool.execute(parameters, inputPortObjects, m_exec, executionHints);
            var viewNodeIds = getViewNodeIdsAndRegisterVirtualProject(result);
            var outputs = result.outputs();
            if (outputs == null) {
                return new PythonToolResult(result.message(), null, viewNodeIds);
            }
            var pyOutputs = Stream.of(result.outputs())//
                .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext))//
                .toArray(PythonPortObject[]::new);

            return new PythonToolResult(result.message(), pyOutputs, viewNodeIds);
        } finally {
            NodeContext.removeLastContext();
            m_nodeContainer.getNode().setFileStoreHandler(originalFileStoreHandler);
        }
    }

    /**
     * Extracts the view-node-ids from the tool-result (if available) and makes the 'virtual project' used for the
     * tool-execution with 'gateway' to make it accessible by Agent Chat View frontend. And also makes sure the 'virtual
     * project' is disposed when not needed anymore.
     *
     * @param viewNodeIds
     * @return view-node-ids in a special format as consumed by the Agent Chat View frontend
     */
    private String[] getViewNodeIdsAndRegisterVirtualProject(final ToolResult toolResult) {
        if (toolResult instanceof WorkflowToolResult workflowToolResult) {
            var virtualProject = workflowToolResult.virtualProject();
            var viewNodeIds = workflowToolResult.viewNodeIds();
            if (virtualProject != null && viewNodeIds != null && viewNodeIds.length > 0) {
                var projectId = VirtualWorkflowProjects.registerProject(virtualProject);
                for (int i = 0; i < viewNodeIds.length; i++) {
                    viewNodeIds[i] = projectId + "#" + viewNodeIds[i];
                }
                m_onCloseRunnables.add(() -> VirtualWorkflowProjects.removeProject(projectId));
                return viewNodeIds;
            } else if (viewNodeIds != null && viewNodeIds.length > 0) {
                for (int i = 0; i < viewNodeIds.length; i++) {
                    viewNodeIds[i] = "not-a-virtual-project#" + viewNodeIds[i];
                }
                return viewNodeIds;
            }
        }
        return new String[0];
    }

    private static ToolValue getTool(final BufferedDataTable toolTable) {
        try (var iterator = toolTable.iterator()) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("Tool table is empty");
            }
            var row = iterator.next();
            return (ToolValue)row.getCell(0);
        }
    }

    @Override
    public void close() throws Exception {
        m_nodeContainer = null;
        m_onCloseRunnables.forEach(Runnable::run);
        m_onCloseRunnables.clear();
    }

}