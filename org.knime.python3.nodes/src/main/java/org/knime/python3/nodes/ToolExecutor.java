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
import java.util.stream.Stream;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.agentic.tool.ToolValue;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;
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
        final List<PythonPortObject> inputs, final Map<String, String> executionHints) {
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
            var outputs = result.outputs();
            if (outputs == null) {
                return new PythonToolResult(result.message(), null);
            }
            var pyOutputs = Stream.of(result.outputs())//
                .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, conversionContext))//
                .toArray(PythonPortObject[]::new);

            return new PythonToolResult(result.message(), pyOutputs);
        } finally {
            NodeContext.removeLastContext();
        }
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

}