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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.agentic.tool.WorkflowToolCell;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.capture.WorkflowSegment;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortTypeRegistry;
import org.knime.python3.nodes.ports.WorkflowSegmentExecutorErrorUtils;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.proxy.PythonToolContext.PythonToolResult;

/**
 * Executes WorkflowTools used by agents.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ToolExecutor {

    private final ExecutionContext m_exec;

    private final NodeContainer m_nodeContainer;

    private final PythonArrowTableConverter m_tableManager;

    ToolExecutor(final ExecutionContext exec, final NodeContainer nodeContainer,
        final PythonArrowTableConverter tableManager) {
        m_exec = exec;
        m_nodeContainer = nodeContainer;
        m_tableManager = tableManager;
    }

    PythonToolResult executeTool(final PurePythonTablePortObject pythonToolTable, final String parameters,
        final List<PythonPortObject> inputs) {
        // TODO if we want to support port types that need the filestoreMap for deserialization
        Map<String, FileStore> dummyFileStoreMap = Map.of();
        var conversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableManager, m_exec);
        var inputPortObjects = inputs.stream()//
            .map(po -> PythonPortTypeRegistry.convertPortObjectFromPython(po, conversionContext))//
            .toArray(PortObject[]::new);

        var toolTable =
            (BufferedDataTable)PythonPortTypeRegistry.convertPortObjectFromPython(pythonToolTable, conversionContext);

        var ws = loadWorkflowTool(toolTable);
        var name = ws.loadWorkflow().getName();

        var wsExecutor = createExecutor(m_nodeContainer, ws, name);
        try {
            wsExecutor.configureWorkflow(parameters);
            var result = wsExecutor.executeWorkflowAndCollectNodeMessages(inputPortObjects, m_exec);
            WorkflowSegmentExecutorErrorUtils.throwIfError(result);
            var outputs = result.portObjectCopies();
            var messageTable = getMessageTable(outputs, ws.getConnectedOutputs());
            var pyOutputs = Stream.of(outputs).filter(o -> o != messageTable)
                .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext))//
                .toArray(PythonPortObject[]::new);
            return new PythonToolResult(extractMessage(messageTable), pyOutputs);
        } catch (Exception ex) {
            // TODO
            throw new RuntimeException("Failed to execute tool: " + name, ex);
        } finally {
            wsExecutor.dispose();
        }
    }

    static WorkflowSegmentExecutor createExecutor(final NodeContainer nodeContainer, final WorkflowSegment ws,
        final String name) {
        try {
            return new WorkflowSegmentExecutor(ws, name, nodeContainer, true, true, warning -> {
            });
        } catch (KNIMEException e) {
            throw new RuntimeException("Failed to create workflow segment executor for tool " + name, e);
        }
    }

    private static WorkflowSegment loadWorkflowTool(final BufferedDataTable toolTable) {
        var workflowToolCell = getWorkflowToolCell(toolTable);
        try (var byteIn = new ByteArrayInputStream(workflowToolCell.getWorkflow());
                var zipIn = new ZipInputStream(byteIn)) {
            return WorkflowSegment.load(zipIn);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load workflow tool", e);
        }
    }

    private static WorkflowToolCell getWorkflowToolCell(final BufferedDataTable toolTable) {
        try (var iterator = toolTable.iterator()) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("Tool table is empty");
            }
            var row = iterator.next();
            return (WorkflowToolCell)row.getCell(0);
        }
    }

    private static BufferedDataTable getMessageTable(final PortObject[] outputs,
        final List<WorkflowSegment.Output> wsOutputs) {
        for (int i = 0; i < outputs.length; i++) {
            if (wsOutputs.get(i).getSpec().map(spec -> spec.getName().equals("message output")).orElse(false)) {
                return (BufferedDataTable)outputs[i];
            }
        }
        return null;
    }

    private static String extractMessage(final BufferedDataTable messageTable) {
        if (messageTable == null) {
            return "Tool executed successfully";
        }
        try (var cursor = messageTable.cursor()) {
            return cursor.forward().getAsDataCell(0).toString();
        }
    }

}