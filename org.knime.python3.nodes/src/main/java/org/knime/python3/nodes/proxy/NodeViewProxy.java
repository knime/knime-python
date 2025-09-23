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
 *   May 12, 2025 (hornm): created
 */
package org.knime.python3.nodes.proxy;

import org.knime.core.node.port.PortObject;
import org.knime.core.util.asynclose.AsynchronousCloseable;
import org.knime.python3.nodes.DelegatingNodeModel;
import org.knime.python3.nodes.proxy.PythonNodeViewProxy.PythonViewContext;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.CredentialsProviderProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.PortMapProvider;
import org.knime.python3.nodes.settings.JsonNodeSettings;

/**
 * Provides methods and interfaces used by views to create data service instances that are powered by a remote proxy.
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 */
public interface NodeViewProxy extends AsynchronousCloseable<RuntimeException> {

    /**
     * Data service that is powered by a remote proxy.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface DataServiceProxy extends AutoCloseable {
        String handleJsonRpcRequest(String param);
    }

    /**
     * Context needed by a NodeViewProxy.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface ViewEnvironment extends PortMapProvider, CredentialsProviderProxy {
    }

    /**
     * Creates a data service that is powered by a remote proxy.
     *
     * @param settings of the node
     * @param portObjects provided as input to the node
     * @param internalViewData the internal view data of the node (see {@link DelegatingNodeModel#getInternalViewData()}
     *            which is being made available to the python-side via the {@link PythonViewContext}; {@code null} if
     *            there is no view data
     * @param portMapProvider provides the map from port groups to their indices
     * @param credentialsProvider provides access to credentials
     *
     * @return a data service that is powered by a remote proxy
     */
    DataServiceProxy getDataServiceProxy(JsonNodeSettings settings, final PortObject[] portObjects,
        String internalViewData, final PortMapProvider portMapProvider,
        final CredentialsProviderProxy credentialsProvider);

}
