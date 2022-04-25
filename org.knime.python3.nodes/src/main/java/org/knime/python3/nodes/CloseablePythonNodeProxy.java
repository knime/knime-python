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
import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore;
import org.knime.core.data.columnar.table.ColumnarContainerTable;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowDataRepository;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.Node;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.util.PythonUtils;
import org.knime.python3.PythonGateway;
import org.knime.python3.arrow.DefaultPythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;
import org.knime.python3.nodes.proxy.CloseableNodeDialogProxy;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;
import org.knime.python3.nodes.proxy.NodeProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.Callback;
import org.knime.python3.scripting.SinkManager;

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

    // STUFF FOR CONFIG AND EXEC:
    // TODO: clean up, see https://knime-com.atlassian.net/browse/AP-18640
    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

    private SinkManager m_sinkManager = new SinkManager(this::getDataRepository, ARROW_STORE_FACTORY);

    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-node-%d").build()));
    // END STUFF FOR CONFIG AND EXEC

    CloseablePythonNodeProxy(final NodeProxy proxy, final PythonGateway<?> gateway) {
        m_proxy = proxy;
        m_gateway = gateway;
    }

    @Override
    public void close() {
        try {
            // TODO close asynchronously for performance (but keep the Windows pitfalls with file deletion in mind)
            m_gateway.close();
            m_sinkManager.close();
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

    @Override
    public BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        final var columnReadStores = new ColumnarBatchReadStore[inData.length];
        final var sources = new PythonArrowDataSource[inData.length];
        for (int inputIndex = 0; inputIndex < inData.length; inputIndex++) {
            columnReadStores[inputIndex] = extractColumnStore(inData[inputIndex]);
            sources[inputIndex] = convertStoreIntoSource(columnReadStores[inputIndex],
                inData[inputIndex].getDataTableSpec().getColumnNames());
        }

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
                return m_sinkManager.create_sink();
            }
        };
        m_proxy.initializeJavaCallback(callback);

        final var execContext = new PythonNodeModelProxy.PythonExecutionContext() {

            @Override
            public void set_progress(final double progress) {
                exec.setProgress(progress);
            }

            @Override
            public boolean is_canceled() {
                try {
                    exec.checkCanceled();
                } catch (CanceledExecutionException e) {
                    return true;
                }
                return false;
            }
        };

        List<PythonArrowDataSink> sinks = m_proxy.execute(sources, inputObjectPaths, outputObjectPaths, execContext);

        var tables = new BufferedDataTable[sinks.size()];
        int i = 0;
        for (var s : sinks) {
            tables[i] = performCancelable(new GetDataTableTask(s, exec),
                new PythonExecutionMonitorCancelable(exec.createSubProgress(0.2)));
            i++;
        }
        return tables;
    }

    private <T> T performCancelable(final Callable<T> task, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(task, m_executorService::submit, cancelable);
        } catch (final PythonCanceledExecutionException ex) {
            final var ex1 = new CanceledExecutionException(ex.getMessage());
            ex1.initCause(ex);
            throw ex1;
        }
    }

    // Store will be closed along with table. If it is a copy, it will have already been closed.
    // TODO: COPIED AND STRIPPED FROM Python3KernelBackend. Refactor and reuse!
    //       See https://knime-com.atlassian.net/browse/AP-18640

    // TODO: Then remove Python2 dependency.
    @SuppressWarnings("resource")
    private static ColumnarBatchReadStore extractColumnStore(final BufferedDataTable table) throws IOException {
        final KnowsRowCountTable delegate = Node.invokeGetDelegate(table);
        if (delegate instanceof ColumnarContainerTable) {
            var columnarTable = (ColumnarContainerTable)delegate;
            final var baseStore = columnarTable.getStore().getDelegateBatchReadStore();
            final boolean isLegacyArrow;
            if (baseStore instanceof ArrowBatchReadStore) {
                isLegacyArrow = ((ArrowBatchReadStore)baseStore).isUseLZ4BlockCompression()
                    || ColumnarValueSchemaUtils.storesDataCellSerializersSeparately(columnarTable.getSchema());
            } else if (baseStore instanceof ArrowBatchStore) {
                // Write stores shouldn't be using the old compression format or the old ValueSchema anymore
                isLegacyArrow = false;
            } else {
                // Not Arrow at all (= a new storage back end), treat like legacy, i.e. copy.
                isLegacyArrow = true;
            }
            if (!isLegacyArrow) {
                return ((ColumnarContainerTable)delegate).getStore();
            }
        }
        throw new IOException("Python nodes can only work with saved Arrow tables for now");
    }

    // Store will be closed along with table. If it is a copy, it will have already been closed.
    @SuppressWarnings("resource")
    private static PythonArrowDataSource convertStoreIntoSource(final ColumnarBatchReadStore columnarStore,
        final String[] columnNames) throws IOException {
        // Unwrap the underlying physical Arrow store from the table. Along the way, flush any cached table
        // content to disk to make it available to Python.
        //
        // TODO: ideally, we want to be able to flush per batch/up to some batch index. Once this is supported,
        // defer flushing until actually needed (i.e. when Python pulls data).
        if (columnarStore instanceof Flushable) {
            ((Flushable)columnarStore).flush();
        }
        final var baseStore = columnarStore.getDelegateBatchReadStore();
        if (baseStore instanceof ArrowBatchReadStore) {
            final ArrowBatchReadStore store = (ArrowBatchReadStore)baseStore;
            return PythonArrowDataUtils.createSource(store, columnNames);
        } else if (baseStore instanceof ArrowBatchStore) {
            final ArrowBatchStore store = (ArrowBatchStore)baseStore;
            return PythonArrowDataUtils.createSource(store, store.numBatches(), columnNames);
        } else {
            // Any non-Arrow store should already have been copied into an Arrow store further above.
            throw new IllegalStateException(
                "Unrecognized store type: " + baseStore.getClass().getName() + ". This is an implementation error.");
        }
    }

    @Override
    public DataTableSpec[] configure(final DataTableSpec[] inSpecs) {
        final String[] serializedInSchemas = TableSpecSerializationUtils.serializeTableSpecs(inSpecs);
        final List<String> serializedOutSchemas = m_proxy.configure(serializedInSchemas);
        return TableSpecSerializationUtils.deserializeTableSpecs(serializedOutSchemas);
    }

    /**
     * TODO: Refactor and reuse! Copied and stripped from Python3KernelBackend See
     * https://knime-com.atlassian.net/browse/AP-18640
     *
     * TODO: Then remove Python2 dependency.
     */
    private final class GetDataTableTask implements Callable<BufferedDataTable> {

        private final PythonArrowDataSink m_pythonSink;

        private final ExecutionContext m_exec;

        public GetDataTableTask(final PythonArrowDataSink pythonSink, final ExecutionContext exec) {
            m_pythonSink = pythonSink;
            m_exec = exec;
        }

        @Override
        public BufferedDataTable call() throws Exception {
            final PythonArrowDataSink pythonSink = m_pythonSink;
            assert m_sinkManager.contains(
                pythonSink) : "Sink was not created by Python3KernelBackend#createSink. This is a coding issue.";
            // Must be a DefaultPythonarrowDataSink because it was created by #createSink
            final DefaultPythonArrowDataSink sink = (DefaultPythonArrowDataSink)pythonSink;

            checkRowKeys(sink);
            final var domainAndMetadata = getDomain(sink);
            final IDataRepository dataRepository = Node.invokeGetDataRepository(m_exec);
            @SuppressWarnings("resource") // Closed by the framework when the table is not needed anymore
            final BufferedDataTable table = PythonArrowDataUtils
                .createTable(sink, domainAndMetadata, ARROW_STORE_FACTORY, dataRepository).create(m_exec);

            m_sinkManager.markUsed(sink);
            return table;
        }

        @SuppressWarnings("resource") // All rowKeyCheckers are closed at #close
        private void checkRowKeys(final DefaultPythonArrowDataSink sink) throws InterruptedException, IOException {
            final var rowKeyChecker = m_sinkManager.getRowKeyChecker(sink);
            if (!rowKeyChecker.allUnique()) {
                throw new IOException(rowKeyChecker.getInvalidCause());
            }
        }

        @SuppressWarnings("resource") // All domainCalculators are closed at #close
        private TableDomainAndMetadata getDomain(final DefaultPythonArrowDataSink sink) throws InterruptedException {
            final var domainCalc = m_sinkManager.getDomainCalculator(sink);
            return domainCalc.getTableDomainAndMetadata();
        }
    }

    @SuppressWarnings("static-method")
    private IFileStoreHandler getFileStoreHandler() {
        return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode().getFileStoreHandler();
    }

    private IDataRepository getDataRepository() {
        var fsHandler = getFileStoreHandler();
        if (fsHandler == null) {
            return NotInWorkflowDataRepository.newInstance();
        } else {
            return fsHandler.getDataRepository();
        }
    }

    @Override
    public String getSchema() {
        return m_proxy.getSchema();
    }
}
