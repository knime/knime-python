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
package org.knime.python3.scripting.nodes2.script;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python2.port.PickledObjectPortObjectSpec;
import org.knime.scripting.editor.ai.AbstractCodeAssistant;

/**
 * This class provides methods to generate Python code with the help of AI
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class PythonCodeAssistant extends AbstractCodeAssistant {
    /**
     * Create an instance of the {@link PythonCodeAssistant}
     *
     * @param authenticationToken The token used to authenticate with the KNIME Hub
     */
    protected PythonCodeAssistant(final String authenticationToken) {
        super(authenticationToken);
    }

    /**
     * Query the AI to generate Python code for the given prompt
     *
     * @param userPrompt The user prompt to instruct the AI what to do
     * @param oldCode The current code. Should not be null, but may be an empty string.
     * @param inputPortSpecs The input port configuration of the node
     * @param outputPortTypes The output port types of the node
     * @param flowVariables The incoming flow variables
     * @return The newly generated code
     * @throws IOException
     */
    public String generateCode( //
        final String userPrompt, //
        final String oldCode, //
        final PortObjectSpec[] inputPortSpecs, //
        final PortType[] outputPortTypes, //
        final Collection<FlowVariable> flowVariables //
    ) throws IOException {
        var request = new CodeGenerationRequest(//
            oldCode, //
            userPrompt, //
            new Inputs(toTableSpecs(inputPortSpecs), //
                getNumObjects(inputPortSpecs), //
                toFlowVariableList(flowVariables) //
            ), //
            new Outputs(getNumTables(outputPortTypes), getNumObjects(outputPortTypes), getNumImages(outputPortTypes),
                false)
        );

        return sendRequest("/v1/code_generation/python", request);
    }

    private static NameAndType[] toColumnNameTypeList(final DataColumnSpec... dataColumnSpecs) {
        return Arrays.stream(dataColumnSpecs).map(dcs -> new NameAndType(dcs.getName(), dcs.getType().getName()))
            .toArray(NameAndType[]::new);
    }

    private static NameAndType[][] toTableSpecs(final PortObjectSpec... portObjects) {
        return Arrays.stream(portObjects).filter(po -> po.getClass() == DataTableSpec.class)
            .map(tableSpec -> toColumnNameTypeList(((DataTableSpec)tableSpec).stream().toArray(DataColumnSpec[]::new)))
            .toArray(NameAndType[][]::new);
    }

    private static long getNumTables(final PortType... portTypes) {
        return Arrays.stream(portTypes).filter(pt -> pt.acceptsPortObjectClass(BufferedDataTable.class)).count();
    }

    private static long getNumImages(final PortType... portTypes) {
        return Arrays.stream(portTypes).filter(pt -> pt.acceptsPortObjectClass(ImagePortObject.class)).count();
    }

    private static long getNumObjects(final PortType... portTypes) {
        return Arrays.stream(portTypes).filter(pt -> pt.acceptsPortObjectClass(PickledObjectFileStorePortObject.class))
            .count();
    }

    private static long getNumObjects(final PortObjectSpec... portObjectSpecs) {
        return Arrays.stream(portObjectSpecs).filter(PickledObjectPortObjectSpec.class::isInstance).count();
    }

    private static NameAndType[] toFlowVariableList(final Collection<FlowVariable> flowVariables) {
        return flowVariables.stream()
            .map(flowVar -> new NameAndType(flowVar.getName(), flowVar.getVariableType().getIdentifier()))
            .toArray(NameAndType[]::new);
    }

    private record NameAndType(String name, String type) {

    }

    private record Outputs(long num_tables, long num_objects, long num_images, boolean has_view) {

    }

    private record Inputs(NameAndType[][] tables, long num_objects, NameAndType[] flow_variables) { // NOSONAR: we don't need hash or equals here

    }

    private record CodeGenerationRequest(String code, String user_query, Inputs inputs, Outputs outputs) {

    }
}
