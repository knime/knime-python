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
 *   16 Aug 2023 (chaubold): created
 */
package org.knime.python3.scripting.nodes2;

import org.knime.core.webui.node.dialog.scripting.CodeGenerationRequest;
import org.knime.core.webui.node.dialog.scripting.CodeGenerationRequest.CodeRequestBody;
import org.knime.core.webui.node.dialog.scripting.CodeGenerationRequest.Inputs;
import org.knime.core.webui.node.dialog.scripting.CodeGenerationRequest.Outputs;
import org.knime.core.webui.node.dialog.scripting.InputOutputModel;
import org.knime.core.webui.node.dialog.scripting.InputOutputModelNameAndTypeUtils;

/**
 * This class provides methods to generate Python code with the help of AI
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCodeAssistant {
    private PythonCodeAssistant() {
    }

    /**
     * Create a request to generate Python code for the given prompt and the given inputs.
     *
     * @param userPrompt The user prompt to instruct the AI what to do
     * @param oldCode The current code. Should not be null, but may be an empty string.
     * @param inputOutputModels The input output models that give context to this AI query
     * @param hasView Whether a view should be populated
     * @return a request for the code generation endpoint
     */
    public static CodeGenerationRequest createCodeGenerationRequest( //
        final String userPrompt, //
        final String oldCode, //
        final InputOutputModel[] inputOutputModels, //
        final boolean hasView //
    ) {
        var inputTables = InputOutputModelNameAndTypeUtils.getTablesMatchingNamePrefix(inputOutputModels,
            String.format("%s %s", PythonScriptingInputOutputModelUtils.INPUT_PREFIX,
                PythonScriptingInputOutputModelUtils.INPUT_OUTPUT_TYPE_TABLE));
        var inputObjects = InputOutputModelNameAndTypeUtils.getModelsMatchingNamePrefix(inputOutputModels,
            InputOutputModel.OBJECT_PORT_TYPE_NAME,
            String.format("%s %s", PythonScriptingInputOutputModelUtils.INPUT_PREFIX,
                PythonScriptingInputOutputModelUtils.INPUT_OUTPUT_TYPE_OBJECT));

        var outputTables = InputOutputModelNameAndTypeUtils.getTablesMatchingNamePrefix(inputOutputModels,
            String.format("%s %s", PythonScriptingInputOutputModelUtils.OUTPUT_PREFIX,
                PythonScriptingInputOutputModelUtils.INPUT_OUTPUT_TYPE_TABLE));
        var outputObjects = InputOutputModelNameAndTypeUtils.getModelsMatchingNamePrefix(inputOutputModels,
            InputOutputModel.OBJECT_PORT_TYPE_NAME,
            String.format("%s %s", PythonScriptingInputOutputModelUtils.OUTPUT_PREFIX,
                PythonScriptingInputOutputModelUtils.INPUT_OUTPUT_TYPE_OBJECT));
        var outputImages = InputOutputModelNameAndTypeUtils.getModelsMatchingNamePrefix(inputOutputModels,
            InputOutputModel.OBJECT_PORT_TYPE_NAME, // Images are also of object type
            String.format("%s %s", PythonScriptingInputOutputModelUtils.OUTPUT_PREFIX,
                PythonScriptingInputOutputModelUtils.INPUT_OUTPUT_TYPE_IMAGE));

        return new CodeGenerationRequest( //
            "/code_generation/python", //
            new CodeRequestBody(//
                oldCode, //
                userPrompt, //
                new Inputs( //
                    inputTables, //
                    inputObjects.length, //
                    InputOutputModelNameAndTypeUtils.getSupportedFlowVariables(inputOutputModels) //
                ), //
                new Outputs( //
                    outputTables.length, //
                    outputObjects.length, //
                    outputImages.length, //
                    hasView //
                ) //
            ) //
        );
    }
}
