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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.agentic.tool.ToolValue;
import org.knime.core.node.agentic.tool.ToolValue.ToolResult;
import org.knime.core.node.agentic.tool.WorkflowToolValue;
import org.knime.core.node.agentic.tool.WorkflowToolValue.WorkflowToolResult;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeID.NodeIDSuffix;
import org.knime.core.node.workflow.capture.CombinedExecutor;
import org.knime.core.node.workflow.capture.CombinedExecutor.PortId;
import org.knime.core.node.workflow.capture.WorkflowPortObject;
import org.knime.core.node.workflow.capture.WorkflowPortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowSegment;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.ExecutionMode;
import org.knime.gateway.impl.project.VirtualWorkflowProjects;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.DelegatingNodeModel.ViewData.VirtualProject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortTypeRegistry;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.proxy.PythonToolContext.CombinedToolsWorkflowInfo;
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

    private VirtualProject m_combinedToolsWorkflow;

    private CombinedExecutor m_workflowExecutor;

    private boolean m_disposeCombinedToolsWorkflowOnClose;

    private FileStoreHandlerSwitcher m_fileStoreSwitcher = new FileStoreHandlerSwitcher();

    ToolExecutor(final ExecutionContext exec, final NativeNodeContainer nodeContainer,
        final PythonArrowTableConverter tableManager) {
        m_exec = exec;
        m_nodeContainer = nodeContainer;
        m_tableManager = tableManager;
        m_onCloseRunnables = new ArrayList<>();
    }

    ToolExecutor(final ExecutionContext exec, final NativeNodeContainer nodeContainer,
        final PythonArrowTableConverter tableManager, final VirtualProject combinedToolsWorkflowInfo) {
        m_exec = exec;
        m_nodeContainer = nodeContainer;
        m_tableManager = tableManager;
        m_onCloseRunnables = new ArrayList<>();
        m_combinedToolsWorkflow = combinedToolsWorkflowInfo;
        m_disposeCombinedToolsWorkflowOnClose = false;
    }

    PythonToolResult executeTool(final PurePythonTablePortObject pythonToolTable, final String parameters,
        final List<PythonPortObject> inputs, final Map<String, String> executionHints) {
        if (m_nodeContainer == null) {
            throw new IllegalStateException("ToolExecutor has already been closed");
        }

        m_fileStoreSwitcher.switchFileStoreHandler(m_nodeContainer, true);

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
        try {
            var result = tool.execute(parameters, inputPortObjects, m_exec, executionHints);
            var viewNodeIds = getViewNodeIdsAndRegisterVirtualProject(result);
            var outputs = result.outputs();
            if (outputs == null) {
                return new PythonToolResult(result.message(), null, null, viewNodeIds);
            }
            var pyOutputs = Stream.of(result.outputs())//
                .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext))//
                .toArray(PythonPortObject[]::new);

            return new PythonToolResult(result.message(), pyOutputs, null, viewNodeIds);
        } finally {
            NodeContext.removeLastContext();
        }
    }

    CombinedToolsWorkflowInfo initCombinedToolsWorkflow(final List<PythonPortObject> inputs,
        final String execModeString) {
        m_fileStoreSwitcher.switchFileStoreHandler(m_nodeContainer, false);

        Map<String, FileStore> dummyFileStoreMap = Map.of();
        List<String> inputPortIds = null;
        var execMode = ExecutionMode.valueOf(execModeString);
        if (m_combinedToolsWorkflow == null) {
            var conversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableManager, m_exec);
            var inputPortObjects = inputs.stream()//
                .map(po -> PythonPortTypeRegistry.convertPortObjectFromPython(po, conversionContext))//
                .toArray(PortObject[]::new);
            m_workflowExecutor =
                WorkflowSegmentExecutor.builder(m_nodeContainer, execMode, "Combined tools workflow", warning -> {
                }, m_exec, false).combined(inputPortObjects).build();
            var combinedToolsWorkflowId = m_workflowExecutor.getWorkflow().getID();
            m_disposeCombinedToolsWorkflowOnClose = true;
            m_combinedToolsWorkflow = new VirtualProject(combinedToolsWorkflowId);
            inputPortIds = m_workflowExecutor.getSourcePortIds().stream()
                .map(id -> id.nodeIDSuffix() + "#" + id.portIndex()).toList();
        } else {
            m_workflowExecutor =
                WorkflowSegmentExecutor.builder(m_nodeContainer, execMode, "Combined tools workflow", warning -> {
                }, m_exec, false).combined(m_combinedToolsWorkflow.loadAndGetWorkflow()).build();
            assert inputs.isEmpty();
            inputPortIds = List.of();
        }
        return new CombinedToolsWorkflowInfo(m_combinedToolsWorkflow.projectId(),
            m_combinedToolsWorkflow.workflowId().toString(), inputPortIds);
    }

    PythonToolResult executeToolInCombinedWorkflow(final PurePythonTablePortObject pythonToolTable, final String parameters,
        final List<String> inputIds, final Map<String, String> executionHints) {
        assert m_workflowExecutor != null && m_combinedToolsWorkflow != null;

        Map<String, FileStore> dummyFileStoreMap = Map.of();
        var conversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableManager, m_exec);
        var toolTable =
            (BufferedDataTable)PythonPortTypeRegistry.convertPortObjectFromPython(pythonToolTable, conversionContext);
        var tool = getTool(toolTable);
        if (!(tool instanceof WorkflowToolValue)) {
            // TODO enable this type of execution for non-workflow-based tools, too
            // (add a placeholder node to the combined tools workflow that outputs the tool's outputs)
            throw new UnsupportedOperationException(
                "Executing tools in a combined workflow only supported for workflow-based tools so far");
        }
        var workflowTool = (WorkflowToolValue)tool;
        var combinedToolsWorkflowId = m_combinedToolsWorkflow.workflowId();

        // map inputIds to ports
        var inputs = inputIds.stream().map(id -> createPortId(id)).toList();
        NodeContext.pushContext(m_nodeContainer);
        try {
            var result = workflowTool.execute(m_workflowExecutor, parameters, inputs, m_exec, executionHints);
            var viewNodeIds = result.viewNodeIds();
            var outputs = result.outputs();
            if (outputs == null) {
                return new PythonToolResult(result.message(), null, null, viewNodeIds);
            }
            var pyOutputs = Stream.of(result.outputs())//
                .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext))//
                .toArray(PythonPortObject[]::new);

            var outputIds = Stream.of(result.outputIds()).map(id -> id.replaceFirst(combinedToolsWorkflowId + ":", "")) // TODO re-visit?
                .toList();
            return new PythonToolResult(result.message(), pyOutputs, outputIds, viewNodeIds);
        } finally {
            NodeContext.removeLastContext();
        }
    }

    static PortId createPortId(final String id) {
        var split = id.split("#");
        return new PortId(NodeIDSuffix.fromString(split[0]), Integer.parseInt(split[1]));
    }

    PythonPortObject getCombinedToolsWorkflow() {
        // TODO remove virtual I/O nodes and properly set inputs/outputs on workflow segment
        var combinedToolsWorkflow = m_workflowExecutor.getWorkflow();
        var ws = new WorkflowSegment(combinedToolsWorkflow, List.of(), List.of(), Set.of());
        var spec = new WorkflowPortObjectSpec(ws, combinedToolsWorkflow.getName(), List.of(), List.of());
        var po = new WorkflowPortObject(spec);
        try {
            ws.serializeAndDisposeWorkflow();
        } catch (IOException ex) {
            // TODO
            throw new RuntimeException(ex);
        }
        var conversionContext = new PortObjectConversionContext(Map.of(), m_tableManager, m_exec);
        return PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext);
    }

    VirtualProject getCombinedToolsWorkflowAndMarkToNotDisposeOnClose() {
        m_disposeCombinedToolsWorkflowOnClose = false;
        return m_combinedToolsWorkflow;
    }

    /**
     * Extracts the view-node-ids from the tool-result (if available) and makes the 'virtual project' used for the
     * tool-execution with 'gateway' to make it accessible by Agent Chat View frontend. And also makes sure the 'virtual
     * project' is disposed when not needed anymore.
     *
     * NOTE: only relevant when tool is executed via
     * {@link ToolExecutor#executeTool(PurePythonTablePortObject, String, List, Map)} but NOT with
     * {@link ToolExecutor#executeToolInCombinedWorkflow(PurePythonTablePortObject, String, List, Map)}. In case of the
     * later, the virtual project life cycle is taken care of via {@link VirtualProject} and the view-ids are only
     * port-references (node-id + port-index) but doesn't include the project-id anymore.
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
        if (m_disposeCombinedToolsWorkflowOnClose) {
            m_combinedToolsWorkflow.dispose();
        }
        if (m_workflowExecutor != null) {
            m_workflowExecutor.dispose(false);
            m_workflowExecutor = null;
        }
        m_combinedToolsWorkflow = null;
        m_nodeContainer = null;
        m_onCloseRunnables.forEach(Runnable::run);
        m_onCloseRunnables.clear();

        m_fileStoreSwitcher.close();
        m_fileStoreSwitcher = null;
    }

    private static class FileStoreHandlerSwitcher implements AutoCloseable {

        private NativeNodeContainer m_nodeContainer;

        private IFileStoreHandler m_originalFileStoreHandler;

        private NotInWorkflowWriteFileStoreHandler m_temporaryFileStoreHandler;

        void switchFileStoreHandler(final NativeNodeContainer nodeContainer, final boolean createTempDirAlready) {
            if (m_nodeContainer != null) {
                return;
            }
            m_nodeContainer = nodeContainer;
            m_originalFileStoreHandler = m_nodeContainer.getNode().getFileStoreHandler();

            // Hack to make sure all nodes within the 'virtual workflow' used for the tool execution
            // use a suitable file store handler in case the host node is already executed (e.g. Agent Chat View).
            // We temporarily(!) replace the file store handler of the host node with a more suitable one (and make
            // sure its available via the WorkflowDataRepository) such that
            // FlowVirtualScopeContext.createFileStoreHandler returns it.
            m_temporaryFileStoreHandler = new NotInWorkflowWriteFileStoreHandler(UUID.randomUUID(),
                m_originalFileStoreHandler.getDataRepository());
            m_temporaryFileStoreHandler.open();
            m_nodeContainer.getNode().setFileStoreHandler(m_temporaryFileStoreHandler);

            if (createTempDirAlready) {
                // The FilestoreHandler is initialized lazily in the context of the workflow that first creates a filestore
                // in case of detached mode, this is the first tool workflow which gets removed after execution leading
                // all subsequent filestore creations to fail.
                // In particular, the first 'createFileStore' call creates the temporary directory (fs-handler's 'base-dir')
                // to write the file-stores into. The temporary directory is created within the workflow temp directory
                // determined via the NodeContext.
                try {
                    m_temporaryFileStoreHandler.createFileStore("dummy");
                } catch (IOException ex) {
                    NodeLogger.getLogger(ToolExecutor.class)
                        .error("Failed to pin temporary filestore handler to agent workflow.", ex);
                }
            }
        }

        @Override
        public void close() {
            if (m_temporaryFileStoreHandler == null) {
                return;
            }
            m_temporaryFileStoreHandler.close();
            m_temporaryFileStoreHandler.clearAndDispose();
            m_nodeContainer.getNode().setFileStoreHandler(m_originalFileStoreHandler);
        }
    }

}