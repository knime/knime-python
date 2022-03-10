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
 *   Jan 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore;
import org.knime.core.data.columnar.table.ColumnarContainerTable;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowDataRepository;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.Node;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.table.schema.AnnotatedColumnarSchema;
import org.knime.core.table.schema.DefaultAnnotatedColumnarSchema;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.util.PythonUtils;
import org.knime.python3.arrow.DefaultPythonArrowDataSink;
import org.knime.python3.arrow.DomainCalculator;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;
import org.knime.python3.arrow.RowKeyChecker;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;
import org.knime.python3.nodes.proxy.NodeModelProxy;
import org.knime.python3.nodes.proxy.NodeModelProxy.Callback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import py4j.Py4JException;

/**
 * NodeModel that delegates its operations to a proxy implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class DelegatingNodeModel extends NodeModel {

    private final Supplier<CloseableNodeModelProxy> m_proxySupplier;

    // STUFF FOR CONFIG AND EXEC:
    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

    private SinkManager m_sinkManager = new SinkManager();

    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-node-%d").build()));
    // END STUFF FOR CONFIG AND EXEC

    private JsonNodeSettings m_settings;

    // TODO retrieve the expected in and outputs from Python
    protected DelegatingNodeModel(final Supplier<CloseableNodeModelProxy> pythonNodeSupplier,
        final JsonNodeSettings initialSettings) {
        super(1, 1);
        m_proxySupplier = pythonNodeSupplier;
        m_settings = initialSettings;
    }

    @SuppressWarnings("static-method")
    private IFileStoreHandler getFileStoreHandler() {
        return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode().getFileStoreHandler();
    }

    private IWriteFileStoreHandler getWriteFileStoreHandler() {
        final IFileStoreHandler nodeFsHandler = getFileStoreHandler();
        IWriteFileStoreHandler fsHandler = null;
        if (nodeFsHandler instanceof IWriteFileStoreHandler) {
            fsHandler = (IWriteFileStoreHandler)nodeFsHandler;
        } else {
            // TODO: copied from Python3KernelBackend but removed the logic to close temporary FS handlers for now -> re-add that!
            fsHandler = NotInWorkflowWriteFileStoreHandler.create();
        }
        return fsHandler;
    }

    private AnnotatedColumnarSchema specToSchema(final DataTableSpec spec) {
        // TODO: do this without a IWriteFileStoreHandler!!
        final var vs = ValueSchemaUtils.create(spec, RowKeyType.CUSTOM, getWriteFileStoreHandler());
        final var cvs = ColumnarValueSchemaUtils.create(vs);
        // TODO: pass metadata as well?
        var columnNames = new String[spec.getNumColumns() + 1];
        columnNames[0] = "RowKey";
        System.arraycopy(spec.getColumnNames(), 0, columnNames, 1, spec.getNumColumns());
        return DefaultAnnotatedColumnarSchema.annotate(cvs, columnNames);
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        try (var node = m_proxySupplier.get()) {
            node.setParameters(m_settings.getParameters(), m_settings.getCreationVersion());
            final AnnotatedColumnarSchema[] inSchemas =
                Arrays.stream(inSpecs).map(this::specToSchema).toArray(AnnotatedColumnarSchema[]::new);

            final String[] serializedInSchemas =
                Arrays.stream(inSchemas).map(DelegatingNodeModel::serializeColumnarValueSchema).toArray(String[]::new);
            final List<String> serializedOutSchemas = node.configure(serializedInSchemas);
            AnnotatedColumnarSchema[] outSchemas = serializedOutSchemas.stream()
                .map(DelegatingNodeModel::deserializeAnnotatedColumnarSchema).toArray(AnnotatedColumnarSchema[]::new);

            return Arrays.stream(outSchemas).map(acs -> {
                return PythonArrowDataUtils.createDataTableSpec(acs.getColumnarSchema(), acs.getColumnNames());
            }).toArray(DataTableSpec[]::new);
        }
    }

    private static String serializeColumnarValueSchema(final AnnotatedColumnarSchema schema) {
        return AnnotatedColumnarSchemaSerializer.save(schema, JsonNodeFactory.instance).toString();
    }

    private static AnnotatedColumnarSchema deserializeAnnotatedColumnarSchema(final String serializedSchema) {
        final ObjectMapper om = new ObjectMapper();
        try {
            return AnnotatedColumnarSchemaSerializer.load(om.readTree(serializedSchema));
        } catch (JsonMappingException ex) {
            throw new IllegalStateException("Python node returned invalid serialized columnar schema", ex);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Python node returned invalid serialized columnar schema", ex);
        }
    }

    // Store will be closed along with table. If it is a copy, it will have already been closed.
    // NOTE: COPIED AND STRIPPED FROM Python3KernelBackend. Refactor and reuse!!
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

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        try (var node = m_proxySupplier.get()) {
            node.setParameters(m_settings.getParameters(), m_settings.getCreationVersion());

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

            final NodeModelProxy.Callback callback = new Callback() {

                @Override
                public String resolve_knime_url(final String knimeUrl) {
                    return Python2KernelBackend.resolveKnimeUrl(knimeUrl, /*m_nodeContextManager*/null);
                }

                @Override
                public PythonArrowDataSink create_sink() throws IOException {
                    return m_sinkManager.create_sink();
                }
            };
            node.initializeJavaCallback(callback);

            final var execContext = new NodeModelProxy.PythonExecutionContext() {

                @Override
                public void set_progress(final double progress) {
                    exec.setProgress(progress);
                }

                @Override
                public boolean is_cancelled() {
                    try {
                        exec.checkCanceled();
                    } catch (CanceledExecutionException e) {
                        return true;
                    }
                    return false;
                }
            };

            List<PythonArrowDataSink> sinks = node.execute(sources, inputObjectPaths, outputObjectPaths, execContext);

            var tables = new BufferedDataTable[sinks.size()];
            int i = 0;
            for (var s : sinks) {
                tables[i] = performCancelable(new GetDataTableTask(s, exec),
                    new PythonExecutionMonitorCancelable(exec.createSubProgress(0.2)));
                i++;
            }
            return tables;
        }
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
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        var pythonSettings = new JsonNodeSettings(settings);
        try (var node = m_proxySupplier.get()) {
            var error = node.validateParameters(pythonSettings.getParameters(), pythonSettings.getCreationVersion());
            CheckUtils.checkSetting(error == null, "%s", error);
        }
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings = new JsonNodeSettings(settings);
        try (var node = m_proxySupplier.get()) {
            setParameters(node);
        } catch (Py4JException ex) {
            throw new InvalidSettingsException("Invalid parameters.", ex);
        }
    }

    private void setParameters(final CloseableNodeModelProxy node) {
        node.setParameters(m_settings.getParameters(), m_settings.getCreationVersion());
    }

    @Override
    protected void reset() {
        // nothing to reset
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to load
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to save
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

    /**
     * NOTE: COPIED AND STRIPPED FROM Python3KernelBackend. Refactor and reuse!!
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

    /**
     * A class for managing a set of sinks with rowKeyCheckers and domainCalculators
     *
     * NOTE: COPIED AND STRIPPED FROM Python3KernelBackend. Refactor and reuse!!
     */
    private final class SinkManager implements AutoCloseable {

        private final Set<DefaultPythonArrowDataSink> m_sinks = new HashSet<>();

        private final Set<DefaultPythonArrowDataSink> m_usedSinks = new HashSet<>();

        private final Map<DefaultPythonArrowDataSink, RowKeyChecker> m_rowKeyCheckers = new HashMap<>();

        private final Map<DefaultPythonArrowDataSink, DomainCalculator> m_domainCalculators = new HashMap<>();

        @SuppressWarnings("resource") // The resources are remembered and closed in #close
        private synchronized PythonArrowDataSink create_sink() throws IOException {//NOSONAR used by Python
            final var path = DataContainer.createTempFile(".knable").toPath();
            final var sink = PythonArrowDataUtils.createSink(path);

            final IFileStoreHandler fileStoreHandler = getFileStoreHandler();
            final IDataRepository dataRepository;
            if (fileStoreHandler != null) {
                dataRepository = fileStoreHandler.getDataRepository();
            } else {
                dataRepository = NotInWorkflowDataRepository.newInstance();
            }

            // Check row keys and compute the domain as soon as anything is written to the sink
            final var rowKeyChecker = PythonArrowDataUtils.createRowKeyChecker(sink, ARROW_STORE_FACTORY);
            final var domainCalculator = PythonArrowDataUtils.createDomainCalculator(sink, ARROW_STORE_FACTORY,
                DataContainerSettings.getDefault().getMaxDomainValues(), dataRepository);

            // Remember the sink, rowKeyChecker and domainCalc for cleaning up later
            m_sinks.add(sink);
            m_rowKeyCheckers.put(sink, rowKeyChecker);
            m_domainCalculators.put(sink, domainCalculator);

            return sink;
        }

        public boolean contains(final Object sink) {
            return m_sinks.contains(sink);
        }

        public RowKeyChecker getRowKeyChecker(final DefaultPythonArrowDataSink sink) {
            return m_rowKeyCheckers.get(sink);
        }

        public DomainCalculator getDomainCalculator(final DefaultPythonArrowDataSink sink) {
            return m_domainCalculators.get(sink);
        }

        public void markUsed(final DefaultPythonArrowDataSink sink) {
            m_usedSinks.add(sink);
        }

        @Override
        public void close() throws Exception {
            m_rowKeyCheckers.values();
            m_domainCalculators.values();
            deleteUnusedSinkFiles();
        }

        /** Deletes the temporary files of all sinks that have not been used */
        private void deleteUnusedSinkFiles() {
            m_sinks.removeAll(m_usedSinks);
            for (var s : m_sinks) {
                try {
                    Files.deleteIfExists(Path.of(s.getAbsolutePath()));
                } catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }
}
