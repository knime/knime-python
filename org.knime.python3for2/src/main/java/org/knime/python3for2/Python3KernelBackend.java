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

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore;
import org.knime.core.data.columnar.table.ColumnarContainerTable;
import org.knime.core.data.columnar.table.ColumnarRowReadTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.util.FileUtil;
import org.knime.core.util.Pair;
import org.knime.core.util.Version;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.kernel.NodeContextManager;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonInstallationTestException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelBackend;
import org.knime.python2.kernel.PythonKernelBackendUtils;
import org.knime.python2.kernel.PythonKernelCleanupException;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonOutputListeners;
import org.knime.python2.port.PickledObjectFile;
import org.knime.python2.util.PythonUtils;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.arrow.DefaultPythonArrowDataSink;
import org.knime.python3.arrow.DomainCalculator;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;
import org.knime.python3.arrow.PythonArrowExtension;
import org.knime.python3.arrow.RowKeyChecker;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import py4j.Py4JException;

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

    private static final List<PythonModuleSpec> REQUIRED_MODULES =
        List.of(new PythonModuleSpec("py4j"), new PythonModuleSpec("pyarrow", new Version(5, 0, 0), true));

    private static final Set<Class<?>> KNOWN_FLOW_VARIABLE_TYPES = Set.of( //
        Boolean.class, //
        Boolean[].class, //
        Double.class, //
        Double[].class, //
        Integer.class, //
        Integer[].class, //
        Long.class, //
        Long[].class, //
        String.class, //
        String[].class //
    );

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

    private final PythonCommand m_command;

    private final PythonGateway<Python3KernelBackendProxy> m_gateway;

    private final PythonOutputListeners m_outputListeners;

    /**
     * Set to {@code null} in {@link #close()} just to make sure that any Java instances that have been referenced by
     * the Python side (and therefore by py4j on the Java side) can be garbage collected in a timely manner.
     */
    private /* final */ Python3KernelBackendProxy m_proxy;

    /**
     * Used to make kernel operations cancelable.
     */
    private final ExecutorService m_executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-worker-%d").build());

    /**
     * Initialized by {@link #setOptions(PythonKernelOptions)}.
     */
    private PythonKernelOptions m_currentOptions;

    /**
     * Properly initialized by {@link #setOptions(PythonKernelOptions)}. Holds the node context that was active at the
     * time when that method was called (if any).
     */
    private final NodeContextManager m_nodeContextManager = new NodeContextManager();

    /**
     * Holds {@link NotInWorkflowWriteFileStoreHandler temporary} file store handlers that have to be created when
     * converting legacy and virtual tables into Arrow-backed tables while no in-workflow file store handler is
     * available (i.e. in the node dialog). They can only be cleared upon closing of the kernel since we do not know for
     * how long the Python side references their respective file stores.
     */
    private final Set<IFileStoreHandler> m_temporaryFsHandlers = new HashSet<>(1);

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private final Set<DefaultPythonArrowDataSink> m_sinks = new HashSet<>();

    private final Set<DefaultPythonArrowDataSink> m_usedSinks = new HashSet<>();

    private final Map<DefaultPythonArrowDataSink, RowKeyChecker> m_rowKeyCheckers = new HashMap<>();

    private final Map<DefaultPythonArrowDataSink, DomainCalculator> m_domainCalculators = new HashMap<>();

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
            throw new IllegalArgumentException("The new Python kernel back end does not support Python 2 anymore. If "
                + "you still want to use Python 2, please change your settings to use the legacy kernel back end.");
        }
        try {
            m_command = command;

            // TODO: perform installation testing in the running process. We do not want to spawn an extra Python
            // process just for testing. Instead, make testing part of launching the process.
            PythonKernel.testInstallation(command, REQUIRED_MODULES);

            final String launcherPath = Python3for2SourceDirectory.getPath().resolve("knime_kernel.py").toString();
            final List<PythonExtension> extensions = Collections.singletonList(PythonArrowExtension.INSTANCE);
            final PythonPath pythonPath = new PythonPathBuilder() //
                .add(Python3SourceDirectory.getPath()) //
                .add(Python3ArrowSourceDirectory.getPath()) //
                .add(Python3for2SourceDirectory.getPath()) //
                .build();

            m_gateway = new PythonGateway<>(command.createProcessBuilder(), launcherPath,
                Python3KernelBackendProxy.class, extensions, pythonPath);

            @SuppressWarnings("resource") // Will be closed along with gateway.
            final InputStream stdoutStream = m_gateway.getStandardOutputStream();
            @SuppressWarnings("resource") // Will be closed along with gateway.
            final InputStream stderrStream = m_gateway.getStandardErrorStream();
            m_outputListeners = new PythonOutputListeners(stdoutStream, stderrStream, m_nodeContextManager);
            m_outputListeners.startListening();

            m_proxy = m_gateway.getEntryPoint();
            final Python3KernelBackendProxy.Callback callback =
                knimeUrl -> Python2KernelBackend.resolveKnimeUrl(knimeUrl, m_nodeContextManager);
            m_proxy.initializeJavaCallback(callback);

            // TODO: Allow users to enable debugging via VM argument? We want devs to be able to debug their Python code
            // outside of eclipse using only KNIME + their favorite Python editor.
            // TODO: Also figure out how we can support debugpy in addition to pydev.
            // m_proxy.enableDebugging();
        } catch (final Throwable th) { // NOSONAR We cannot risk leaking the Python process or any other held resources.
            close();
            if (th instanceof Error || th instanceof IOException) {
                throw th;
            } else {
                throw new IOException(th);
            }
        }
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
        // TODO: perform installation testing in the running process. We do not want to spawn an extra Python process
        // just for testing. At this point, we can even communicate the test requirements and test results via py4j.
        PythonKernel.testInstallation(m_command, options.getAdditionalRequiredModules());

        m_currentOptions = options;
        m_nodeContextManager.setNodeContext(NodeContext.getContext());
        initializeExternalCustomPath(options.getExternalCustomPath());
        initializeCurrentWorkingDirToWorkflowDir();
    }

    private void initializeExternalCustomPath(final String externalCustomPath) {
        if (!Strings.isNullOrEmpty(externalCustomPath)) {
            m_proxy.initializeExternalCustomPath(externalCustomPath);
        }
    }

    private void initializeCurrentWorkingDirToWorkflowDir() {
        final Optional<String> workflowDir = Python2KernelBackend
            .getWorkflowDirectoryForSettingWorkingDirectory(m_nodeContextManager.getNodeContext(), LOGGER);
        if (workflowDir.isPresent()) {
            m_proxy.initializeCurrentWorkingDirectory(workflowDir.get());
        }
    }

    @Override
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables)
        throws PythonIOException {
        final LinkedHashMap<String, Object> flowVariablesMap = new LinkedHashMap<>(flowVariables.size());
        for (final FlowVariable variable : flowVariables) {
            // Flow variables typically contain Java primitives or strings as values (or arrays of these). We simply let
            // py4j handle the conversion of the values into their Python equivalents. Values and array elements that
            // are not Java primitives or strings are converted into their string representations beforehand, which
            // follows the behavior of the legacy Python back end.
            // Note that the legacy Python back end only supports double, int, and string flow variables and converts
            // all other variable values, including arrays, into strings. So this simple implementation here is already
            // an improvement over the legacy implementation.
            //
            // TODO: the conversion of values of unknown type into strings might be a problem in terms of forward
            // compatibility: what if we want to provide a "proper" mapping of the values in the future? Users might
            // already rely on the string representation in their scripts. Should we skip unknown values entirely?
            final VariableType<?> type = variable.getVariableType();
            final Class<?> simpleType = type.getSimpleType();
            Object value = variable.getValue(type);
            if (!KNOWN_FLOW_VARIABLE_TYPES.contains(simpleType)) {
                if (simpleType.isArray()) {
                    value = Arrays.stream((Object[])value) //
                        .map(v -> Objects.toString(v, null)) //
                        .toArray(String[]::new);
                } else {
                    value = Objects.toString(value, null);
                }
            }
            flowVariablesMap.put(variable.getName(), value);
        }
        m_proxy.putFlowVariablesIntoWorkspace(name, flowVariablesMap);
    }

    @Override
    public Collection<FlowVariable> getFlowVariables(final String name) throws PythonIOException {
        final Map<String, Object> flowVariablesMap = m_proxy.getFlowVariablesFromWorkspace(name);
        final VariableType<?>[] allVariableTypes = VariableTypeRegistry.getInstance().getAllTypes();
        final Set<FlowVariable> flowVariables = new LinkedHashSet<>(flowVariablesMap.size());
        for (final var entry : flowVariablesMap.entrySet()) {
            final String variableName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                // py4j returns lists instead of arrays, convert them manually.
                value = convertIntoArrayValue(variableName, (List<?>)value);
                if (value == null) {
                    continue;
                }
            }
            if (value != null) {
                final VariableType<?> matchingType = findMatchingVariableType(variableName, value, allVariableTypes);
                if (matchingType != null
                    // Reserved flow variables like "knime.workspace" are also passed through the node, filter them out.
                    && isValidVariableName(variableName)) {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    final var variable = new FlowVariable(variableName, (VariableType)matchingType, value);
                    flowVariables.add(variable);
                }
            } else {
                LOGGER.warn("Flow variable '" + variableName + "' is empty. The variable will be ignored.");
            }
        }
        return flowVariables;
    }

    private static Object convertIntoArrayValue(final String variableName, final List<?> listValue) {
        if (!listValue.isEmpty()) {
            try {
                return listValue.toArray(size -> (Object[])Array.newInstance(listValue.get(0).getClass(), size));
            } catch (final ArrayStoreException ex) {
                LOGGER.warn(
                    "Array-typed flow variable '" + variableName
                        + "' contains elements of different types, which is not allowed. The variable will be ignored",
                    ex);
            }
        } else {
            LOGGER.warn("Array-typed flow variable '" + variableName + "' is empty and will be ignored.");
        }
        return null;
    }

    private static VariableType<?> findMatchingVariableType(final String variableName, final Object value,
        final VariableType<?>[] variableTypes) {
        VariableType<?> matchingType = null;
        for (final var type : variableTypes) {
            if (type.getSimpleType().isInstance(value)) {
                matchingType = type;
                break;
            }
        }
        if (matchingType == null) {
            LOGGER.warn("KNIME offers no flow variable types that match the type of flow variable '" + variableName
                + "'. The variable will be ignored. Please change its type to something KNIME understands.");
            LOGGER.debug(
                "The Java type of flow variable '" + variableName + "' is '" + value.getClass().getTypeName() + "'.");
        }
        return matchingType;
    }

    private static boolean isValidVariableName(final String variableName) {
        return !(variableName.startsWith(FlowVariable.Scope.Global.getPrefix())
            || variableName.startsWith(FlowVariable.Scope.Local.getPrefix()));
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
        performCancelable(new PutDataTableTask(name, table, numRows, m_currentOptions.getSerializationOptions()),
            new PythonExecutionMonitorCancelable(executionMonitor));
    }

    @Override
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws PythonIOException, CanceledExecutionException {
        return performCancelable(new GetDataTableTask(name, exec),
            new PythonExecutionMonitorCancelable(executionMonitor));
    }

    @Override
    public void putObject(final String name, final PickledObjectFile object) throws PythonIOException {
        m_proxy.loadPickledObjectIntoWorkspace(name, object.getFile().getAbsolutePath());
    }

    @Override
    public void putObject(final String name, final PickledObjectFile object, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        performVoidCancelable(() -> putObject(name, object), new PythonExecutionMonitorCancelable(executionMonitor));
    }

    @Override
    public PickledObjectFile getObject(final String name, final File file, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        return performCancelable(() -> getObject(name, file), new PythonExecutionMonitorCancelable(executionMonitor));
    }

    private PickledObjectFile getObject(final String name, final File file)
        throws PythonIOException, CanceledExecutionException {
        final var type = m_proxy.getObjectType(name);
        final var representation = m_proxy.getObjectStringRepresentation(name);
        m_proxy.pickleObjectToFile(name, file.getAbsolutePath());
        return new PickledObjectFile(file, type, representation);
    }

    @Override
    public ImageContainer getImage(final String name) throws PythonIOException {
        File tempDir = null;
        try {
            tempDir = FileUtil.createTempDir("images");
            var imgPath = tempDir.toPath().resolve("image");
            m_proxy.writeImageFromWorkspaceToPath(name, imgPath.toAbsolutePath().toString());
            return PythonKernelBackendUtils.createImage(() -> Files.newInputStream(imgPath));
        } catch (IOException ex) {
            throw new PythonIOException(ex);
        } finally {
            if (tempDir != null) {
                FileUtil.deleteRecursively(tempDir);
            }
        }
    }

    @Override
    public ImageContainer getImage(final String name, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        return performCancelable(() -> getImage(name), new PythonExecutionMonitorCancelable(executionMonitor));
    }

    @Override
    public List<Map<String, String>> listVariables() throws PythonIOException {
        return m_proxy.listVariablesInWorkspace();
    }

    @Override
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
        throws PythonIOException {
        return m_proxy.autoComplete(sourceCode, line, column);
    }

    @Override
    public String[] execute(final String sourceCode) throws PythonIOException {
        return beautifyPythonTraceback(
            () -> m_proxy.executeOnMainThread(sourceCode, this::createSink).toArray(String[]::new));
    }

    @Override
    public String[] execute(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return performCancelable(() -> beautifyPythonTraceback(
            () -> m_proxy.executeOnMainThread(sourceCode, this::createSink).toArray(String[]::new)), cancelable);
    }

    @SuppressWarnings("resource") // The resources are remembered and closed in #close
    private synchronized PythonArrowDataSink createSink() throws IOException {
        final var path = DataContainer.createTempFile(".knable").toPath();
        final var sink = PythonArrowDataUtils.createSink(path);

        // Check row keys and compute the domain as soon as anything is written to the sink
        final var rowKeyChecker = PythonArrowDataUtils.createRowKeyChecker(sink, ARROW_STORE_FACTORY);
        final var domainCalculator = PythonArrowDataUtils.createDomainCalculator(sink, ARROW_STORE_FACTORY,
            DataContainerSettings.getDefault().getMaxDomainValues());

        // Remember the sink, rowKeyChecker and domainCalc for cleaning up later
        m_sinks.add(sink);
        m_rowKeyCheckers.put(sink, rowKeyChecker);
        m_domainCalculators.put(sink, domainCalculator);

        return sink;
    }

    @Override
    public String[] executeAsync(final String sourceCode) throws PythonIOException {
        return beautifyPythonTraceback(
            () -> m_proxy.executeOnCurrentThread(sourceCode, this::createSink).toArray(String[]::new));
    }

    @Override
    public String[] executeAsync(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return performCancelable(() -> beautifyPythonTraceback(
            () -> m_proxy.executeOnCurrentThread(sourceCode, this::createSink).toArray(String[]::new)), cancelable);
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

    private interface Task {

        void run() throws Exception;//NOSONAR

    }

    private void performVoidCancelable(final Task task, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        performCancelable(() -> {
            task.run();
            return null;
        }, cancelable);
    }

    private static <T> T beautifyPythonTraceback(final Supplier<T> task) throws PythonIOException {
        try {
            return task.get();
        } catch (final Py4JException ex) {
            // First strip py4j's standard prefix for such kinds of errors.
            final var pythonTraceback = StringUtils.removeStart(ex.getMessage(),
                "An exception was raised by the Python Proxy. Return Message: ");
            // Then strip the parts of the trace back that refer to kernel code rather than user code.
            final String beautifiedTraceback = Python2KernelBackend.beautifyPythonTraceback(pythonTraceback);
            final var errorMessage = "Executing the Python script failed: " + beautifiedTraceback;
            // Discard the original exception if the trace back is formatted as expected (i.e. the error really
            // originated in user code and not in kernel code). This keeps logging more concise.
            if (beautifiedTraceback != pythonTraceback) { // NOSONAR We're interested in reference equality.
                throw new PythonIOException(errorMessage);
            } else {
                throw new PythonIOException(errorMessage, ex);
            }
        }
    }

    @Override
    public void close() throws PythonKernelCleanupException {
        if (m_closed.compareAndSet(false, true)) {
            new Thread(() -> {
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_outputListeners);
                PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executorService);
                m_proxy = null;
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_gateway);
                synchronized (m_temporaryFsHandlers) {
                    PythonUtils.Misc.invokeSafely(LOGGER::debug, IFileStoreHandler::clearAndDispose,
                        m_temporaryFsHandlers);
                }
                deleteUnusedSinkFiles();
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_rowKeyCheckers.values());
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_domainCalculators.values());
            }).start();
        }
    }

    /** Deletes the temporary files of all sinks that have not been used */
    private void deleteUnusedSinkFiles() {
        m_sinks.removeAll(m_usedSinks);
        PythonUtils.Misc.invokeSafely(LOGGER::debug, s -> {
            try {
                Files.deleteIfExists(Path.of(s.getAbsolutePath()));
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }, m_sinks);
    }

    private final class PutDataTableTask implements Callable<Void> {

        private final String m_name;

        private final BufferedDataTable m_table;

        private final long m_numRows;

        private final SerializationOptions m_options;

        public PutDataTableTask(final String name, final BufferedDataTable table, final long numRows,
            final SerializationOptions options) {
            m_name = name;
            m_table = table;
            m_numRows = numRows;
            m_options = options;
        }

        // Store will be closed along with table. If it is a copy, it will have already been closed.
        @SuppressWarnings("resource")
        @Override
        public Void call() throws Exception {
            final Pair<ColumnarBatchReadStore, Boolean> p = extractStoreCopyTableIfNecessary(m_table);
            final ColumnarBatchReadStore columnarStore = p.getFirst();
            final boolean storeIsCopy = p.getSecond();
            try {
                final PythonArrowDataSource source =
                    convertStoreIntoSource(columnarStore, m_table.getDataTableSpec().getColumnNames());
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
            } finally {
                if (storeIsCopy) {
                    Files.deleteIfExists(columnarStore.getPath());
                }
            }
            return null;
        }

        // Store will be closed along with table. If it is a copy, it will have already been closed.
        @SuppressWarnings("resource")
        private Pair<ColumnarBatchReadStore, Boolean> extractStoreCopyTableIfNecessary(final BufferedDataTable table)
            throws IOException {
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
                    final ColumnarBatchReadStore columnarStore = ((ColumnarContainerTable)delegate).getStore();
                    return new Pair<>(columnarStore, false);
                }
            }
            // Fallback for legacy and virtual tables.
            final ColumnarBatchReadStore columnarStore = copyTableToArrowStore(table);
            return new Pair<>(columnarStore, true);
        }

        private ColumnarBatchReadStore copyTableToArrowStore(final BufferedDataTable table) throws IOException {
            final IFileStoreHandler nodeFsHandler =
                ((NativeNodeContainer)m_nodeContextManager.getNodeContext().getNodeContainer()).getNode()
                    .getFileStoreHandler();
            IWriteFileStoreHandler fsHandler = null;
            if (nodeFsHandler instanceof IWriteFileStoreHandler) {
                fsHandler = (IWriteFileStoreHandler)nodeFsHandler;
            } else {
                // The node's file store handler will be null or an EmptyFileStoreHandler if we are in the dialog.
                synchronized (m_temporaryFsHandlers) {
                    if (!m_closed.get()) {
                        fsHandler = NotInWorkflowWriteFileStoreHandler.create();
                        // Since "putDataTable" and "execute" are independent from one another, we do not know for how
                        // long the temporary file stores are going to be used. That is why we need to keep their
                        // handler alive for the entire lifetime of the kernel.
                        // If the kernel is already (being) closed, we will not allocate a new handler. Any errors in
                        // the conversion resulting from this need to be handled by the client who (willingly) called
                        // "putDataTable" and closed the kernel concurrently.
                        m_temporaryFsHandlers.add(fsHandler);
                    }
                }
            }
            final var schema =
                ColumnarValueSchemaUtils.create(ValueSchemaUtils.create(table.getSpec(), RowKeyType.CUSTOM, fsHandler));
            ColumnarRowReadTable tableCopy;
            try (final var columnarTable = new ColumnarRowWriteTable(schema, ARROW_STORE_FACTORY,
                new ColumnarRowWriteTableSettings(true, false, -1, false, false, false))) {
                try (final RowCursor inCursor = table.cursor();
                        final RowWriteCursor outCursor = columnarTable.createCursor()) {
                    while (inCursor.canForward()) {
                        outCursor.forward().setFrom(inCursor.forward());
                    }
                    tableCopy = columnarTable.finish();
                }
            }
            try (tableCopy) {
                return tableCopy.getStore();
            }
        }

        // Store will be closed along with table. If it is a copy, it will have already been closed.
        @SuppressWarnings("resource")
        private PythonArrowDataSource convertStoreIntoSource(final ColumnarBatchReadStore columnarStore,
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
                throw new IllegalStateException("Unrecognized store type: " + baseStore.getClass().getName()
                    + ". This is an implementation error.");
            }
        }
    }

    private final class GetDataTableTask implements Callable<BufferedDataTable> {

        private final String m_name;

        private final ExecutionContext m_exec;

        public GetDataTableTask(final String name, final ExecutionContext exec) {
            m_name = name;
            m_exec = exec;
        }

        @Override
        public BufferedDataTable call() throws Exception {
            final PythonArrowDataSink pythonSink = m_proxy.getTableFromWorkspace(m_name);
            assert m_sinks.contains(
                pythonSink) : "Sink was not created by Python3KernelBackend#createSink. This is a coding issue.";
            // Must be a DefaultPythonarrowDataSink because it was created by #createSink
            final DefaultPythonArrowDataSink sink = (DefaultPythonArrowDataSink)pythonSink;

            checkRowKeys(sink);
            final var domainAndMetadata = getDomain(sink);
            final IDataRepository dataRepository = Node.invokeGetDataRepository(m_exec);
            @SuppressWarnings("resource") // Closed by the framework when the table is not needed anymore
            final BufferedDataTable table = PythonArrowDataUtils
                .createTable(sink, domainAndMetadata, ARROW_STORE_FACTORY, dataRepository).create(m_exec);

            m_usedSinks.add(sink);
            return table;
        }

        @SuppressWarnings("resource") // All rowKeyCheckers are closed at #close
        private void checkRowKeys(final PythonArrowDataSink sink) throws InterruptedException, PythonIOException {
            final var rowKeyChecker = m_rowKeyCheckers.get(sink);
            if (!rowKeyChecker.allUnique()) {
                throw new PythonIOException(rowKeyChecker.getInvalidCause());
            }
        }

        @SuppressWarnings("resource") // All domainCalculators are closed at #close
        private TableDomainAndMetadata getDomain(final PythonArrowDataSink sink) throws InterruptedException {
            final var domainCalc = m_domainCalculators.get(sink);
            return domainCalc.getTableDomainAndMetadata();
        }
    }
}
