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
 *   Oct 26, 2023 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.webui.node.dialog.scripting.InputOutputModel;
import org.knime.core.webui.node.dialog.scripting.WorkflowControl.InputPortInfo;
import org.knime.pixi.port.PythonEnvironmentPortObject;
import org.knime.python2.port.PickledObjectFileStorePortObject;

/**
 * Utilities for providing the {@link InputOutputModel} for the scripting editor dialog.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class PythonScriptingInputOutputModelUtils {

    /**
     * The type string used for tables
     */
    static final String INPUT_OUTPUT_TYPE_TABLE = "Table";

    /**
     * The type string used for images
     */
    static final String INPUT_OUTPUT_TYPE_IMAGE = "Image";

    /**
     * The type string used for Objects
     */
    static final String INPUT_OUTPUT_TYPE_OBJECT = "Object";

    /**
     * The type prefix used for input ports
     */
    static final String INPUT_PREFIX = "Input";

    /**
     * The type prefix used for output ports
     */
    static final String OUTPUT_PREFIX = "Output";

    private static final String REQUIRED_IMPORT = "import knime.scripting.io as knio";

    private static final String CODE_ALIAS_FLOW_VARS = "knio.flow_variables";

    private static final String CODE_ALIAS_TEMPLATE_FLOW_VARS =
        "knio.flow_variables[\"{{{escapeDblQuotes subItems.[0].name}}}\"]";

    private static final String CODE_ALIAS_OUTPUT_VIEW = "knio.output_view";

    private PythonScriptingInputOutputModelUtils() {
        // utility class
    }

    static InputOutputModel getFlowVariableInputs(final Collection<FlowVariable> flowVariables) {
        return InputOutputModel.flowVariables() //
            .codeAlias(CODE_ALIAS_FLOW_VARS) //
            .subItemCodeAliasTemplate(CODE_ALIAS_TEMPLATE_FLOW_VARS) //
            .requiredImport(REQUIRED_IMPORT) //
            .subItems(flowVariables, PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES_SET::contains) //
            .build();
    }

    static List<InputOutputModel> getInputObjects(final InputPortInfo[] inputPorts) {
        final var inputInfos = new ArrayList<InputOutputModel>();

        var tableIdx = 0;
        var objectIdx = 0;
        for (int i = 0; i < inputPorts.length; i++) {
            final var type = inputPorts[i].portType();
            final var spec = inputPorts[i].portSpec();
            if (spec instanceof DataTableSpec dataTableSpec) {
                // Table with specs available
                inputInfos.add( //
                    InputOutputModel.table() //
                        .name(inputName(tableIdx, INPUT_OUTPUT_TYPE_TABLE)) //
                        .codeAlias(getInputObjectCodeAlias(tableIdx, INPUT_OUTPUT_TYPE_TABLE)) //
                        .subItemCodeAliasTemplate(getSubItemCodeAliasTemplate(tableIdx, INPUT_OUTPUT_TYPE_TABLE)) //
                        .multiSelection(true) //
                        .requiredImport(REQUIRED_IMPORT) //
                        .subItems(dataTableSpec, DataType::getName) //
                        .build() //
                );
                tableIdx++;
            } else if (type.acceptsPortObjectClass(BufferedDataTable.class)) {
                // Table but no spec available
                inputInfos.add(createInputModel(tableIdx, INPUT_OUTPUT_TYPE_TABLE));
                tableIdx++;
            } else if (type.acceptsPortObjectClass(PickledObjectFileStorePortObject.class)) {
                // Object (spec not used)
                inputInfos.add(createInputModel(objectIdx, INPUT_OUTPUT_TYPE_OBJECT));
                objectIdx++;
            } else if (type.acceptsPortObjectClass(PythonEnvironmentPortObject.class)) {
                // Skip Python environment ports - they are not data ports
            } else {
                throw new IllegalStateException("Unsupported input port. This is an implementation error.");
            }
        }
        return inputInfos;
    }

    static List<InputOutputModel> getOutputObjects(final PortType[] portTypes, final boolean hasView) {
        var relevantPortTypes =
            Stream.of(portTypes).filter(PythonScriptingInputOutputModelUtils::isNoFlowVariablePort).toList();
        var portTypeCounter = new HashMap<String, Integer>();
        var outputPortInfos = IntStream.range(0, relevantPortTypes.size()).mapToObj(i -> { // NOSONAR
            var type = portTypeToInputOutputType(relevantPortTypes.get(i));
            var index = portTypeCounter.computeIfAbsent(type, t -> 0);
            portTypeCounter.put(type, index + 1);
            return builderForType(type) //
                .name(outputName(index, type)) //
                .codeAlias(getOutputObjectCodeAlias(index, type)) //
                .requiredImport(REQUIRED_IMPORT) //
                .build();
        }).toList();
        if (hasView) {
            outputPortInfos = new ArrayList<>(outputPortInfos);
            outputPortInfos.add(InputOutputModel.view() //
                .name("Output View") //
                .codeAlias(CODE_ALIAS_OUTPUT_VIEW) //
                .requiredImport(REQUIRED_IMPORT) //
                .build() //
            );
        }
        return outputPortInfos;
    }

    private static String inputName(final int index, final String displayName) {
        return String.format("%s %s %d", INPUT_PREFIX, displayName, index + 1);
    }

    private static String outputName(final int index, final String displayName) {
        return String.format("%s %s %d", OUTPUT_PREFIX, displayName, index + 1);
    }

    private static InputOutputModel createInputModel(final int index, final String type) {
        return builderForType(type) //
            .name(inputName(index, type)) //
            .codeAlias(getInputObjectCodeAlias(index, type)) //
            .subItemCodeAliasTemplate(getSubItemCodeAliasTemplate(index, type)) //
            .requiredImport(REQUIRED_IMPORT) //
            .multiSelection(true) //
            .build();
    }

    private static boolean isNoFlowVariablePort(final PortType portType) {
        return !portType.acceptsPortObjectClass(FlowVariablePortObject.class);
    }

    private static String portTypeToInputOutputType(final PortType portType) {
        if (portType.acceptsPortObjectClass(BufferedDataTable.class)) {
            return INPUT_OUTPUT_TYPE_TABLE;
        } else if (portType.acceptsPortObjectClass(PickledObjectFileStorePortObject.class)) {
            return INPUT_OUTPUT_TYPE_OBJECT;
        } else if (portType.acceptsPortObjectClass(ImagePortObject.class)) {
            return INPUT_OUTPUT_TYPE_IMAGE;
        } else {
            throw new IllegalArgumentException("Unsupported port type: " + portType.getName());
        }
    }

    private static String appendIndexSuffix(final String prefix, final int index) {
        return String.format("%s[%d]", prefix, index);
    }

    private static String getSubItemCodeAliasTemplate(final int index, final String type) {
        var templateString = """
                knio.input_%s[%d][
                    {{~#if subItems.[1]~}}
                        [{{#each subItems}}"{{{escapeDblQuotes this.name}}}"{{#unless @last}},{{/unless}}{{/each}}]
                    {{~else~}}
                        "{{{escapeDblQuotes subItems.[0].name}}}"
                    {{~/if~}}
                ].to_pandas()""";
        switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE: {
                return String.format(templateString, "tables", index);
            }
            case INPUT_OUTPUT_TYPE_OBJECT: {
                return String.format(templateString, "objects", index);
            }
            default:
                throw new IllegalArgumentException("Unexpected input object type: " + type);
        }
    }

    private static String getInputObjectCodeAlias(final int index, final String type) {
        switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE: {
                return appendIndexSuffix("knio.input_tables", index) + ".to_pandas()";
            }
            case INPUT_OUTPUT_TYPE_OBJECT: {
                return appendIndexSuffix("knio.input_objects", index);
            }
            default:
                throw new IllegalArgumentException("Unexpected input object type: " + type);
        }
    }

    private static String getOutputObjectCodeAlias(final int index, final String type) {
        switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE: {
                return appendIndexSuffix("knio.output_tables", index);
            }
            case INPUT_OUTPUT_TYPE_OBJECT: {
                return appendIndexSuffix("knio.output_objects", index);
            }
            case INPUT_OUTPUT_TYPE_IMAGE: {
                return appendIndexSuffix("knio.output_images", index);
            }
            default:
                throw new IllegalArgumentException("Unexpected input object type: " + type);
        }
    }

    private static InputOutputModel.RequiresNameBuilder builderForType(final String type) {
        return switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE -> InputOutputModel.table();
            case INPUT_OUTPUT_TYPE_OBJECT -> InputOutputModel
                .portObject(toHexColor(PickledObjectFileStorePortObject.TYPE.getColor()));
            case INPUT_OUTPUT_TYPE_IMAGE -> InputOutputModel.portObject(toHexColor(ImagePortObject.TYPE.getColor()));
            default -> throw new IllegalArgumentException("Unexpected port object type: " + type);
        };
    }

    private static String toHexColor(final int color) {
        // Ignore alpha (if there is any)
        var rgb = color & 0x00FFFFFF;
        // Format with a "#" prefix and 6 hex digits
        return String.format("#%06X", rgb);
    }
}
