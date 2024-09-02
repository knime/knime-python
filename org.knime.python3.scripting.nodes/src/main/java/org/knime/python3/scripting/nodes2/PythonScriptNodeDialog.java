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
 *   Jun 30, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.knime.core.node.workflow.NodeContext;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.page.Page;
import org.knime.scripting.editor.GenericInitialDataBuilder;
import org.knime.scripting.editor.GenericInitialDataBuilder.DataSupplier;
import org.knime.scripting.editor.ScriptingNodeSettingsService;
import org.knime.scripting.editor.WorkflowControl;

/**
 * The node dialog implementation of the Python Scripting nodes.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction") // the UIExtension node dialog API is still restricted
public final class PythonScriptNodeDialog implements NodeDialog {

    private final PythonScriptingService m_scriptingService;

    private final boolean m_hasView;

    /**
     * Create a new scripting editor dialog
     *
     * @param hasView if the node has an output view
     */
    public PythonScriptNodeDialog(final boolean hasView) {
        m_hasView = hasView;
        m_scriptingService = new PythonScriptingService(hasView);
    }

    @Override
    public Set<SettingsType> getSettingsTypes() {
        return Set.of(SettingsType.MODEL);
    }

    @Override
    public Page getPage() {
        return Page //
            .builder(PythonScriptNodeDialog.class, "js-src/dist", "index.html") //
            .addResourceDirectory("assets") //
            .addResourceDirectory("monacoeditorwork") //
            .addResource(m_scriptingService::openHtmlPreview, "preview.html") //
            .build();
    }

    @Override
    public Optional<RpcDataService> createRpcDataService() {
        return Optional.of(RpcDataService.builder() //
            .addService("ScriptingService", m_scriptingService.getJsonRpcService()) //
            .onDeactivate(m_scriptingService::onDeactivate) //
            .build());
    }

    @Override
    public NodeSettingsService getNodeSettingsService() {
        var workflowControl = new WorkflowControl(NodeContext.getContext().getNodeContainer());

        DataSupplier inputObjectSupplier =
            () -> PythonScriptingInputOutputModelUtils.getInputObjects(workflowControl.getInputInfo());

        DataSupplier flowVariableSupplier = () -> {
            var flowVariables = Optional.ofNullable(workflowControl.getFlowObjectStack()) //
                .map(stack -> stack.getAllAvailableFlowVariables().values()) //
                .orElseGet(List::of);
            return PythonScriptingInputOutputModelUtils.getFlowVariableInputs(flowVariables);
        };

        DataSupplier outputObjectSupplier = () -> PythonScriptingInputOutputModelUtils
            .getOutputObjects(workflowControl.getOutputPortTypes(), m_hasView);

        DataSupplier executableOptionsListSupplier = () -> {
            var executableOptions = ExecutableSelectionUtils.getExecutableOptions(workflowControl.getFlowObjectStack());

            return executableOptions.values().stream() //
                .sorted((o1, o2) -> o1.type == o2.type ? o1.id.compareTo(o2.id) : o1.type.compareTo(o2.type)) //
                .toList();
        };

        var initialData = GenericInitialDataBuilder //
            .createDefaultInitialDataBuilder(NodeContext.getContext()) //
            .addDataSupplier("inputObjects", inputObjectSupplier) //
            .addDataSupplier("flowVariables", flowVariableSupplier) //
            .addDataSupplier("outputObjects", outputObjectSupplier) //
            .addDataSupplier("hasPreview", () -> m_hasView) //
            .addDataSupplier("executableOptionsList", executableOptionsListSupplier);

        // We grab this here instead of inside the settings supplier, because the node context gets disposed
        // when the node is closed, which happens BEFORE the settings are saved.
        final PythonScriptPortsConfiguration portsConfiguration =
            PythonScriptPortsConfiguration.fromCurrentNodeContext();

        return new ScriptingNodeSettingsService( //
            () -> new PythonScriptNodeSettings(portsConfiguration), //
            initialData //
        );
    }

    @Override
    public boolean canBeEnlarged() {
        return true;
    }
}
