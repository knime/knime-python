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
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.scripting.editor.InputOutputModel;
import org.knime.scripting.editor.WorkflowControl.InputPortInfo;

/**
 * Utilities for providing the {@link InputOutputModel} for the scripting editor dialog.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction") // scripting editor API is still restricted
final class PythonScriptingInputOutputModelUtils {

    private static final String INPUT_OUTPUT_TYPE_TABLE = "Table";

    private static final String INPUT_OUTPUT_TYPE_IMAGE = "Image";

    private static final String INPUT_OUTPUT_TYPE_OBJECT = "Object";

    private static final String REQUIRED_IMPORT = "import knime.scripting.io as knio";

    private static final String CODE_ALIAS_FLOW_VARS = "knio.flow_variables";

    private static final String CODE_ALIAS_TEMPLATE_FLOW_VARS =
        "knio.flow_variables[\"{{{escapeDblQuotes subItems.[0].name}}}\"]";

    private static final String CODE_ALIAS_OUTPUT_VIEW = "knio.output_view";

    private PythonScriptingInputOutputModelUtils() {
        // utility class
    }

    static InputOutputModel getFlowVariableInputs(final Collection<FlowVariable> flowVariables) {
        return InputOutputModel.createFromFlowVariables(flowVariables, //
            CODE_ALIAS_FLOW_VARS, //
            CODE_ALIAS_TEMPLATE_FLOW_VARS, //
            REQUIRED_IMPORT, //
            false, //
            PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES_SET::contains //
        );
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
                inputInfos.add(InputOutputModel.createFromTableSpec( //
                    inputName(tableIdx, INPUT_OUTPUT_TYPE_TABLE), //
                    dataTableSpec, //
                    getInputObjectCodeAlias(tableIdx, INPUT_OUTPUT_TYPE_TABLE), //
                    getSubItemCodeAliasTemplate(tableIdx, INPUT_OUTPUT_TYPE_TABLE), //
                    true, //
                    REQUIRED_IMPORT, //
                    DataType::getName, //
                    t -> true //
                ));
                tableIdx++;
            } else if (type.acceptsPortObjectClass(BufferedDataTable.class)) {
                // Table but no spec available
                inputInfos.add(createInputModel(tableIdx, INPUT_OUTPUT_TYPE_TABLE));
                tableIdx++;
            } else if (type.acceptsPortObjectClass(PickledObjectFileStorePortObject.class)) {
                // Object (spec not used)
                inputInfos.add(createInputModel(objectIdx, INPUT_OUTPUT_TYPE_OBJECT));
                objectIdx++;
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
            return new InputOutputModel( //
                outputName(index, type), //
                getOutputObjectCodeAlias(index, type), //
                null, //
                REQUIRED_IMPORT, //
                false, //
                null, //
                getPortObjectTypeId(type), //
                getPortObjectColor(type) //
            );
        }).toList();
        if (hasView) {
            outputPortInfos = new ArrayList<>(outputPortInfos);
            outputPortInfos.add(new InputOutputModel("Output View", //
                CODE_ALIAS_OUTPUT_VIEW, //
                null, //
                REQUIRED_IMPORT, //
                false, //
                null, //
                InputOutputModel.VIEW_PORT_TYPE_NAME, //
                null //
            ));
        }
        return outputPortInfos;
    }

    private static String inputName(final int index, final String displayName) {
        return String.format("Input %s %d", displayName, index + 1);
    }

    private static String outputName(final int index, final String displayName) {
        return String.format("Output %s %d", displayName, index + 1);
    }

    private static InputOutputModel createInputModel(final int index, final String displayName) {
        return new InputOutputModel( //
            inputName(index, displayName), //
            getInputObjectCodeAlias(index, displayName), //
            getSubItemCodeAliasTemplate(index, displayName), //
            REQUIRED_IMPORT, //
            true, //
            null, //
            getPortObjectTypeId(displayName), //
            getPortObjectColor(displayName) //
        );
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

    private static String getPortObjectTypeId(final String type) {
        return switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE -> InputOutputModel.TABLE_PORT_TYPE_NAME;
            case INPUT_OUTPUT_TYPE_OBJECT, INPUT_OUTPUT_TYPE_IMAGE -> InputOutputModel.OBJECT_PORT_TYPE_NAME;
            default -> throw new IllegalArgumentException("Unexpected input object type: " + type);
        };
    }

    private static String getPortObjectColor(final String type) {
        return switch (type) {
            case INPUT_OUTPUT_TYPE_TABLE -> null;
            case INPUT_OUTPUT_TYPE_OBJECT -> toHexColor(PickledObjectFileStorePortObject.TYPE.getColor());
            case INPUT_OUTPUT_TYPE_IMAGE -> toHexColor(ImagePortObject.TYPE.getColor());
            default -> throw new IllegalArgumentException("Unexpected input object type: " + type);
        };
    }

    private static String toHexColor(final int color) {
        // Ignore alpha (if there is any)
        var rgb = color & 0x00FFFFFF;
        // Format with a "#" prefix and 6 hex digits
        return String.format("#%06X", rgb);
    }
}
