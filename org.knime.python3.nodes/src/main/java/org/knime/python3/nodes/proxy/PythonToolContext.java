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
 *   May 23, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.proxy;

import java.util.List;
import java.util.Map;

import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;

/**
 * Context for execution tools from Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface PythonToolContext {

    /**
     * Result of a tool execution.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param message of the tool execution
     * @param outputs of the tool execution
     * @param outputIds
     * @param viewNodeIds ids of nodes with views to be displayed after the tool execution
     */
    record PythonToolResult(String message, PythonPortObject[] outputs, List<String> outputIds, String[] viewNodeIds) {
    }

    /**
     * Executes a tool in Java
     *
     * @param toolTable holding a single tool to execute
     * @param parameters JSON with the parameters for the tool
     * @param inputs input data for the tool
     * @param executionHints additional, optional hints for the tool execution
     * @return the result of the tool execution
     */
    PythonToolResult execute_tool(PurePythonTablePortObject toolTable, String parameters,
        List<PythonPortObject> inputs, Map<String, String> executionHints);

    /**
     * Info about an initialized combined-tools workflow.
     *
     * @param projectId
     * @param workflowId
     * @param inputPortIds
     */
    public record CombinedToolsWorkflowInfo(String projectId, String workflowId, List<String> inputPortIds) {
    }

    /**
     * Initializes the combined-tools workflow for the given inputs.
     *
     * @param inputs the source inputs of the combined-tools workflow
     * @param execMode DEFAUTL, DETACHED or DEBUG
     * @return info about the initialized combined-tools workflow
     */
    CombinedToolsWorkflowInfo init_combined_tools_workflow(List<PythonPortObject> inputs, String execMode);

    /**
     * @return the combined-tools workflow as a Python port object
     */
    PythonPortObject get_combined_tools_workflow();

    /**
     * Executes a tool in Java within a combined-tools workflow.
     *
     * @param toolTable holding a single tool to execute
     * @param parameters JSON with the parameters for the tool
     * @param inputsIds port references (within the combined tools workflow) to use as inputs for the tool execution
     * @param executionHints additional, optional hints for the tool execution
     * @return the result of the tool execution
     */
    PythonToolResult execute_tool_in_combined_workflow(PurePythonTablePortObject toolTable, String parameters,
        List<String> inputsIds, Map<String, String> executionHints);

}