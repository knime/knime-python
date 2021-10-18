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
 *   Jul 22, 2021 (marcel): created
 */
package org.knime.python3for2;

import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore;
import org.knime.core.data.columnar.table.ColumnarContainerTable;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.kernel.NodeContextManager;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonInstallationTestException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelBackend;
import org.knime.python2.kernel.PythonKernelCleanupException;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonOutputListeners;
import org.knime.python2.port.PickledObject;
import org.knime.python2.util.PythonUtils;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowExtension;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * New back end of {@link PythonKernel}. "New" means that this back end is part of Columnar Table Backend-enabled
 * version 3 of the KNIME Python integration (org.knime.python3). "Python3" in the name of this class also refers to
 * this version, not the version of the Python language (the back end supports both language versions Python 2 and
 * Python 3).
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class Python3KernelBackend implements PythonKernelBackend {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Python3KernelBackend.class);

    private final PythonCommand m_command;

    private final PythonGateway<Python3KernelBackendProxy> m_gateway;

    private final PythonOutputListeners m_outputListeners;

    private final Python3KernelBackendProxy m_proxy;

    /**
     * Used to make kernel operations cancelable.
     */
    private final ExecutorService m_executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-worker-%d").build());

    /**
     * Properly initialized by {@link #setOptions(PythonKernelOptions)}. Holds the node context that was active at the
     * time when that method was called (if any).
     */
    private final NodeContextManager m_nodeContextManager = new NodeContextManager();

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private PythonKernelOptions m_currentOptions;

    /**
     * Creates a new Python kernel back end by starting a Python process and connecting to it.
     * <P>
     * Important: call the {@link #close()} method when this back end is no longer needed to shut down the underlying
     * Python process.
     *
     * @param command The {@link PythonCommand} that is used to launch the Python process.
     * @throws PythonInstallationTestException If the Python environment represented by the given {@link PythonCommand}
     *             is not capable of running the Python kernel (e.g. because it misses essential Python modules or there
     *             are version mismatches).
     * @throws IOException If the kernel could not be set up for any reason. This includes the
     *             {@link PythonInstallationTestException} described above which subclasses {@link PythonIOException}.
     *             Other possible cases include: process creation problems, socket connection problems, exceptions on
     *             Python side during setup, communication errors between the Java and the Python side.
     */
    public Python3KernelBackend(final PythonCommand command) throws IOException {
        if (command.getPythonVersion() == PythonVersion.PYTHON2) {
            throw new IllegalArgumentException("The new Python kernel back end does not support Python 2 anymore. If " +
                "you still want to use Python 2, please change your settings to use the legacy kernel back end.");
        }
        m_command = command;

        // TODO: perform an in-kernel installation test. We do not want to spawn an extra Python process just for
        // testing. Instead, make testing part of launcher. Keep dependencies of launcher completely optional.

        final String launcherPath = Python3for2SourceDirectory.getPath().resolve("knime_kernel.py").toString();
        final List<PythonExtension> extensions = Collections.singletonList(PythonArrowExtension.INSTANCE);
        final PythonPath pythonPath = new PythonPathBuilder() //
            .add(Python3SourceDirectory.getPath()) //
            .add(Python3ArrowSourceDirectory.getPath()) //
            .add(Python3for2SourceDirectory.getPath()) //
            .build();

        m_gateway = new PythonGateway<>(command.createProcessBuilder(), launcherPath, Python3KernelBackendProxy.class,
            extensions, pythonPath);

        @SuppressWarnings("resource") // Will be closed along with gateway.
        final InputStream stdoutStream = m_gateway.getStandardOutputStream();
        @SuppressWarnings("resource") // Will be closed along with gateway.
        final InputStream stderrStream = m_gateway.getStandardErrorStream();
        m_outputListeners = new PythonOutputListeners(stdoutStream, stderrStream, m_nodeContextManager);
        m_outputListeners.startListening();

        m_proxy = m_gateway.getEntryPoint();

        // TODO: Allow users to enable debugging via VM argument? We want devs to be able to debug their Python code
        // outside of eclipse using only KNIME + their favorite Python editor.
        // TODO: Also figure out how we can support debugpy in addition to pydev.
        // m_proxy.enableDebugging();
    }

    @Override
    public PythonCommand getPythonCommand() {
        return m_command;
    }

    @Override
    public PythonOutputListeners getOutputListeners() {
        return m_outputListeners;
    }

    @Override
    public PythonKernelOptions getOptions() {
        return m_currentOptions;
    }

    @Override
    public void setOptions(final PythonKernelOptions options) throws PythonIOException {
        m_currentOptions = options;
        throw new IllegalStateException("not yet implemented"); // TODO: NYI
    }

    @Override
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables)
        throws PythonIOException {
        throw new IllegalStateException("not yet implemented"); // TODO: NYI
    }

    @Override
    public Collection<FlowVariable> getFlowVariables(final String name) throws PythonIOException {
        throw new IllegalStateException("not yet implemented"); // TODO: NYI
    }

    @Override
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final int rowLimit) throws PythonIOException, CanceledExecutionException {
        putDataTable(name, table, executionMonitor, (long)rowLimit);
    }

    @Override
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        putDataTable(name, table, executionMonitor, table.size());
    }

    /**
     * Note that "cancellation" in the context of this method only means that we stop waiting for the task to complete
     * and also interrupt its thread. Whether any underlying operations (such as the flushing of caches in the columnar
     * table back end) respond to that interrupt is left to their discretion. We, in particular, also do not forcefully
     * terminate any time-consuming operations on the Python side (e.g. the conversion of the Arrow table to pandas).
     * Instead it is expected that clients terminate the entire kernel right after canceling one of its tasks, as stated
     * in the documentation of {@link PythonKernel#putDataTable(String, BufferedDataTable, ExecutionMonitor)}.
     */
    private void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final long numRows) throws PythonIOException, CanceledExecutionException {
        performCancelable(
            new PutDataTableTask(m_proxy, name, table, numRows, m_currentOptions.getSerializationOptions()),
            executionMonitor);
    }

    @Override
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public void putObject(final String name, final PickledObject object) throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public void putObject(final String name, final PickledObject object, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public PickledObject getObject(final String name, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public ImageContainer getImage(final String name) throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public ImageContainer getImage(final String name, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public List<Map<String, String>> listVariables() throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
        throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public String[] execute(final String sourceCode) throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public String[] execute(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public String[] executeAsync(final String sourceCode) throws PythonIOException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    @Override
    public String[] executeAsync(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        throw new UnsupportedOperationException("not yet implemented"); // TODO: NYI
    }

    private <T> T performCancelable(final Callable<T> task, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(task, m_executorService::submit,
                new PythonExecutionMonitorCancelable(executionMonitor));
        } catch (final PythonCanceledExecutionException ex) {
            final var ex1 = new CanceledExecutionException(ex.getMessage());
            ex1.initCause(ex);
            throw ex1;
        }
    }

    @Override
    public void close() throws PythonKernelCleanupException {
        if (m_closed.compareAndSet(false, true)) {
            new Thread(() -> {
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_outputListeners);
                PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executorService);
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_gateway);
            }).start();
        }
    }

    private static final class PutDataTableTask implements Callable<Void> {

        private final Python3KernelBackendProxy m_proxy;

        private final String m_name;

        private final BufferedDataTable m_table;

        private final long m_numRows;

        private final SerializationOptions m_options;

        public PutDataTableTask(final Python3KernelBackendProxy proxy, final String name, final BufferedDataTable table,
            final long numRows, final SerializationOptions options) {
            m_proxy = proxy;
            m_name = name;
            m_table = table;
            m_numRows = numRows;
            m_options = options;
        }

        @Override
        public Void call() throws Exception {
            final PythonArrowDataSource source = tableToSource(m_table);
            if (m_options.getConvertMissingToPython()) {
                switch (m_options.getSentinelOption()) {
                    case MIN_VAL:
                        m_proxy.putTableIntoWorkspace(m_name, source, m_numRows, "min");
                        break;
                    case MAX_VAL:
                        m_proxy.putTableIntoWorkspace(m_name, source, m_numRows, "max");
                        break;
                    case CUSTOM:
                        m_proxy.putTableIntoWorkspace(m_name, source, m_numRows, m_options.getSentinelValue());
                        break;
                }
            } else {
                m_proxy.putTableIntoWorkspace(m_name, source, m_numRows);
            }
            return null;
        }

        @SuppressWarnings("resource") // The store will be closed along with the table by the client using the kernel.
        private static PythonArrowDataSource tableToSource(final BufferedDataTable table) throws PythonIOException {
            // Unwrap the underlying physical Arrow store from the table. Along the way, flush any cached table content
            // to disk to make it available to Python.
            //
            // TODO: ideally, we want to be able to flush per batch/up to some batch index. Once this is supported, defer
            // flushing until actually needed (i.e. when Python pulls data).
            BatchReadStore readStore = null;
            final KnowsRowCountTable delegate = Node.invokeGetDelegate(table);
            if (delegate instanceof ColumnarContainerTable) {
                final ColumnarBatchReadStore columnarReadStore = ((ColumnarContainerTable)delegate).getStore();
                if (columnarReadStore instanceof Flushable) {
                    try {
                        ((Flushable)columnarReadStore).flush();
                    } catch (final IOException ex) {
                        throw new PythonIOException(ex);
                    }
                }
                readStore = columnarReadStore.getDelegateBatchReadStore();
            }
            final String[] columnNames = table.getDataTableSpec().getColumnNames();
            if (readStore instanceof ArrowBatchReadStore) {
                return PythonArrowDataUtils.createSource((ArrowBatchReadStore)readStore, columnNames);
            } else if (readStore instanceof ArrowBatchStore) {
                final ArrowBatchStore store = (ArrowBatchStore)readStore;
                return PythonArrowDataUtils.createSource(store, store.numBatches(), columnNames);
            } else {
                throw new IllegalStateException("The new Python kernel back end requires any input tables to be " +
                    "stored using the Columnar Table back end.");
            }
        }
    }
}
