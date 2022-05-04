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
 *   Jan 21, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python3.PythonGateway;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.proxy.CloseableNodeDialogProxy;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;
import org.knime.python3.nodes.proxy.NodeProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.Callback;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages the lifecycle of a Python based NodeProxy and its associated process. Invoking {@link Closeable#close()}
 * shuts down the Python process and the proxy is no longer usable.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CloseablePythonNodeProxy
    implements CloseableNodeModelProxy, CloseableNodeFactoryProxy, CloseableNodeDialogProxy {

    private final NodeProxy m_proxy;

    private final PythonGateway<?> m_gateway;

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

    private PythonArrowTableConverter m_tableManager;

    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-node-%d").build()));
    // END STUFF FOR CONFIG AND EXEC

    CloseablePythonNodeProxy(final NodeProxy proxy, final PythonGateway<?> gateway) {
        m_proxy = proxy;
        m_gateway = gateway;
    }

    private static IWriteFileStoreHandler getFileStoreHandler() {
        var context = NodeContext.getContext();
        if (context == null) {
            throw new IllegalStateException("No NodeContext available");
        }
        var nativeNodeContainer = (NativeNodeContainer)context.getNodeContainer();
        return (IWriteFileStoreHandler)nativeNodeContainer.getNode().getFileStoreHandler();
    }

    @Override
    public void close() {
        try {
            // TODO close asynchronously for performance (but keep the Windows pitfalls with file deletion in mind)
            m_gateway.close();
            if (m_tableManager != null) {
                m_tableManager.close();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to shutdown Python gateway.", ex);
        }
    }

    @Override
    public String getParameters() {
        return m_proxy.getParameters();
    }

    @Override
    public String getDialogRepresentation(final String parameters, final String version, final String[] specs) {
        return m_proxy.getDialogRepresentation(parameters, version, specs);
    }

    @Override
    public void validateSettings(final JsonNodeSettings settings) throws InvalidSettingsException {
        var error = m_proxy.validateParameters(settings.getParameters(), settings.getCreationVersion());
        CheckUtils.checkSetting(error == null, "%s", error);
    }

    @Override
    public void loadValidatedSettings(final JsonNodeSettings settings) {
        m_proxy.setParameters(settings.getParameters(), settings.getCreationVersion());
    }

    @Override
    public JsonNodeSettings saveSettings() {
        return new JsonNodeSettings(m_proxy.getParameters(), m_proxy.getSchema());
    }

    private void initTableManager() {
        if (m_tableManager == null) {
            m_tableManager =
                new PythonArrowTableConverter(m_executorService, ARROW_STORE_FACTORY, getFileStoreHandler());
        }
    }

    @Override
    public BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        initTableManager();
        final var sources = m_tableManager.createSources(inData, exec.createSubExecutionContext(0.1));

        // TODO: populate those properly
        String[] inputObjectPaths = new String[0];
        String[] outputObjectPaths = new String[0];

        final PythonNodeModelProxy.Callback callback = new Callback() {

            @Override
            public String resolve_knime_url(final String knimeUrl) {
                return Python2KernelBackend.resolveKnimeUrl(knimeUrl, /*m_nodeContextManager*/null);
            }

            @Override
            public PythonArrowDataSink create_sink() throws IOException {
                return m_tableManager.createSink();
            }
        };
        m_proxy.initializeJavaCallback(callback);

        var progressMonitor = exec.createSubProgress(0.8);

        final var execContext = new PythonNodeModelProxy.PythonExecutionContext() {

            @Override
            public void set_progress(final double progress) {
                progressMonitor.setProgress(progress);
            }

            @Override
            public boolean is_canceled() {
                try {
                    progressMonitor.checkCanceled();
                } catch (CanceledExecutionException e) {//NOSONAR
                    return true;
                }
                return false;
            }
        };

        List<PythonArrowDataSink> sinks = m_proxy.execute(sources, inputObjectPaths, outputObjectPaths, execContext);

        return m_tableManager.convertToTables(sinks, exec.createSubExecutionContext(0.1));
    }

    @Override
    public DataTableSpec[] configure(final DataTableSpec[] inSpecs) {
        final String[] serializedInSchemas = TableSpecSerializationUtils.serializeTableSpecs(inSpecs);
        final List<String> serializedOutSchemas = m_proxy.configure(serializedInSchemas);
        return TableSpecSerializationUtils.deserializeTableSpecs(serializedOutSchemas);
    }

    @Override
    public String getSchema() {
        return m_proxy.getSchema();
    }
}
