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
 *   Feb 15, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.proxy.model;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy;
import org.knime.python3.nodes.proxy.VersionedProxy;
import org.knime.python3.nodes.settings.JsonNodeSettings;

/**
 * A {@link PythonNodeModelProxy} that can be closed to release its resources (i.e. the Python process/connection)
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface NodeConfigurationProxy extends NodeModelProxy, VersionedProxy {

    /**
     * Performs configure with the proxy. In order to retrieve changed settings call
     * {@link NodeModelProxy#getParameters()}.
     *
     * @param inSpecs the incoming port specs
     * @param flowVariableProxy for flow variable access
     * @param credentialsProviderProxy to access credential identifiers
     * @param workflowPropertiesProxy to query workflow information
     * @param warningConsumer for setting warning messages
     * @return the output specs of the node
     * @throws InvalidSettingsException if the node can't be configured because the settings are invalid
     */
    PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, FlowVariablesProxy flowVariableProxy,
        CredentialsProviderProxy credentialsProviderProxy, WorkflowPropertiesProxy workflowPropertiesProxy,
        WarningConsumer warningConsumer) throws InvalidSettingsException;

    /**
     * Validates the provided settings.
     *
     * @param settings to validate
     * @throws InvalidSettingsException if the settings are invalid
     */
    void validateSettings(JsonNodeSettings settings) throws InvalidSettingsException;

}
