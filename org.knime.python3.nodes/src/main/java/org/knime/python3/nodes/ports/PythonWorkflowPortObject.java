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
 *   Feb 13, 2024 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.capture.IsolatedExecutor;
import org.knime.core.node.workflow.capture.WorkflowPortObject;
import org.knime.core.node.workflow.capture.WorkflowPortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowSegment.IOInfo;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.ExecutionMode;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.WorkflowSegmentNodeMessage;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.shared.workflow.storage.text.util.ObjectMapperUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * A Python wrapper for a {@link WorkflowPortObject} that allows to trigger workflow execution from the Python side.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonWorkflowPortObject implements PythonPortObject {

    private final WorkflowPortObject m_workflow;

    private final PythonArrowTableConverter m_tableConverter;

    /**
     * @param workflow to make available in Python
     * @param tableConverter for converting the inputs and outputs during workflow execution
     */
    public PythonWorkflowPortObject(final WorkflowPortObject workflow, final PythonArrowTableConverter tableConverter) {
        m_workflow = workflow;
        m_tableConverter = tableConverter;
    }

    /**
     * @return the spec of the workflow
     */
    public PythonWorkflowPortObjectSpec getSpec() {
        return new PythonWorkflowPortObjectSpec(m_workflow.getSpec());
    }

    @Override
    public String getJavaClassName() {
        return WorkflowPortObject.class.getName();
    }

    /**
     * Executes the workflow on the given inputs.
     *
     * NOTE: Currently only table inputs and outputs are fully supported.
     *
     * @param inputs to execute the workflow on
     * @param warningConsumer callback that is invoked when the workflow emits a warning
     * @return the result of the workflow execution
     * @throws Exception if the inputs are incompatible or the workflow execution throws an exception
     */
    public WorkflowExecutionResult execute(final Map<String, PythonPortObject> inputs,
        final Consumer<String> warningConsumer) throws Exception {
        checkPortCompatibility(inputs);
        // TODO Allow the Python side to create sub-execs for fine-granular progress
        var exec = createExecutionContext();
        // TODO if we want to support port types that need the filestoreMap for deserialization
        Map<String, FileStore> dummyFileStoreMap = Map.of();
        var workflowInputIDs = m_workflow.getSpec().getInputIDs();
        var inportObjectConversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableConverter, exec);
        PortObject[] portObjects = workflowInputIDs.stream()//
            .map(inputs::get)// get inputs in order defined by workflow spec
            .map(p -> PythonPortTypeRegistry.convertPortObjectFromPython(p, inportObjectConversionContext))
            .toArray(PortObject[]::new);
        var executor = createExecutor(warningConsumer, exec);
        try {
            var result = executor.execute(m_workflow.getSpec().getWorkflowSegment(), portObjects, null, null);
            if (result.outputs() == null) {
                // a null array currently indicates a failed execution
                throw new WorkflowExecutionException(
                    WorkflowSegmentNodeMessage.compileSingleErrorMessage(result.nodeMessages()));
            }
            var outportConversionContext = new PortObjectConversionContext(dummyFileStoreMap, m_tableConverter, null);
            var outputs = Stream.of(result.outputs())//
                .map(p -> PythonPortTypeRegistry.convertPortObjectToPython(p, outportConversionContext))//
                .toArray(PythonPortObject[]::new);
            var flowVariables = FlowVariableUtils.convertToMap(result.flowVariables());
            return new WorkflowExecutionResult(outputs, flowVariables);
        } finally {
            executor.dispose();
        }
    }

    // NOSONAR only used as transport container to Python
    record WorkflowExecutionResult(PythonPortObject[] outputs, Map<String, Object> flowVariables) {
    }

    private static ExecutionContext createExecutionContext() {
        var nodeContainer = (NativeNodeContainer)NodeContext.getContext().getNodeContainer();
        return nodeContainer.createExecutionContext().createSilentSubExecutionContext(0);
    }

    private IsolatedExecutor createExecutor(final Consumer<String> warningConsumer, final ExecutionContext exec)
        throws KNIMEException {
        var nc = (NativeNodeContainer)NodeContext.getContext().getNodeContainer();
        CheckUtils.checkArgumentNotNull(nc, "Not a local workflow");
        return WorkflowSegmentExecutor
            .builder(nc, ExecutionMode.DEFAULT, m_workflow.getSpec().getWorkflowName(), warningConsumer, exec, true)
            .isolated(false).build();
    }

    private void checkPortCompatibility(final Map<String, PythonPortObject> inputs) {
        var expectedInputs = m_workflow.getSpec().getInputs();
        CheckUtils.checkArgument(inputs.size() == expectedInputs.size(),
            "The number of provided (%s) and the number of expected inputs (%s) differ.", inputs.size(),
            expectedInputs.size());
        var portTypeRegistry = PortTypeRegistry.getInstance();
        for (var providedInput : inputs.entrySet()) {
            var inputKey = providedInput.getKey();
            var expectedInput = expectedInputs.get(inputKey);
            CheckUtils.checkArgumentNotNull(expectedInput,
                "The provided input key '%s' does not exist in the workflow.", inputKey);
            var expectedPortType = expectedInput.getType().orElseThrow();
            var pythonObj = providedInput.getValue();
            var portObjClass = portTypeRegistry.getObjectClass(pythonObj.getJavaClassName())//
                .orElseThrow(() -> new IllegalArgumentException(
                    "No port type is registered for the object class '%s'.".formatted(pythonObj.getJavaClassName())));
            CheckUtils.checkArgument(expectedPortType.acceptsPortObjectClass(portObjClass), getJavaClassName(),
                "The workflow does not accept the port object provided for input '%s'.", inputKey);
        }
    }

    /**
     * Python representation of the workflow spec.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class PythonWorkflowPortObjectSpec implements PythonPortObjectSpec {

        private final WorkflowPortObjectSpec m_workflowSpec;

        /**
         * @param workflowSpec the spec of the workflow
         */
        public PythonWorkflowPortObjectSpec(final WorkflowPortObjectSpec workflowSpec) {
            m_workflowSpec = workflowSpec;
        }

        @Override
        public String getJavaClassName() {
            return WorkflowPortObjectSpec.class.getName();
        }

        @Override
        public String toJsonString() {
            var mapper = ObjectMapperUtil.getInstance().getObjectMapper();

            try {
                return mapper.writeValueAsString(WorkflowSpecDef.fromWorkflowPortObjectSpec(m_workflowSpec));
            } catch (JsonProcessingException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private record WorkflowSpecDef(String name, Map<String, WorkflowPortDef> inputs,
        Map<String, WorkflowPortDef> outputs) {
        static WorkflowSpecDef fromWorkflowPortObjectSpec(final WorkflowPortObjectSpec workflowSpec) {
            return new WorkflowSpecDef(workflowSpec.getWorkflowName(), parsePortInfo(workflowSpec.getInputs()),
                parsePortInfo(workflowSpec.getOutputs()));
        }

        private static <T extends IOInfo> Map<String, WorkflowPortDef> parsePortInfo(final Map<String, T> portInfo) {
            return portInfo.entrySet().stream()//
                .collect(Collectors.toMap(Map.Entry::getKey, e -> WorkflowPortDef.fromIOInfo(e.getValue())));
        }
    }

    private record WorkflowPortDef(@JsonProperty("type_name") String typeName, @JsonProperty("type_id") String typeId,
        @JsonRawValue @JsonProperty("table_spec") String tableSpec) {
        static WorkflowPortDef fromIOInfo(final IOInfo ioInfo) {
            var portType = ioInfo.getType().orElseThrow();
            var tableSpec = ioInfo.getSpec()//
                .map(PythonPortTypeRegistry::convertPortObjectSpecToPython)//
                .map(PythonPortObjectSpec::toJsonString)//
                .orElse(null);
            return new WorkflowPortDef(portType.getName(), portType.getPortObjectClass().getName(), tableSpec);
        }
    }

}