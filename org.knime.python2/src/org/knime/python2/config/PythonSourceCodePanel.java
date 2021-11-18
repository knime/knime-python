/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.config;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.VariableType.StringType;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.generic.SourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.kernel.PythonException;
import org.knime.python2.kernel.PythonInstallationTestException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelManager;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonOutputListener;
import org.knime.python2.kernel.PythonOutputLogger;
import org.knime.python2.kernel.messaging.PythonKernelResponseHandler;
import org.knime.python2.port.PickledObjectFile;

/**
 * Source code panel for Python code.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
@SuppressWarnings("serial") // Not intended for serialization.
public class PythonSourceCodePanel extends SourceCodePanel {

    // TODO: set serialization options here and in PySpark panel

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonSourceCodePanel.class);

    private final NodeDialogPane m_parent; // NOSONAR Not intended for serialization.

    private final PythonSourceCodeOptionsPanel m_optionsPanel;

    private final PythonExecutableSelectionPanel m_executablePanel;

    private final ConcurrentLinkedDeque<PythonKernelManagerWrapper> m_kernelManagerQueue =
        new ConcurrentLinkedDeque<>();


    private BufferedDataTable[] m_inputData = new BufferedDataTable[0]; // NOSONAR Not intended for serialization.

    private PickledObjectFile[] m_pythonInputObjects = new PickledObjectFile[0]; // NOSONAR Not intended for serialization.

    private final Lock m_lock = new ReentrantLock();

    private int m_kernelRestarts = 0;

    private JProgressBarProgressMonitor m_progressMonitor; // NOSONAR Not intended for serialization.

    private PythonKernelOptions m_kernelOptions = new PythonKernelOptions(); // NOSONAR Not intended for serialization.

    private final List<WorkspacePreparer> m_workspacePreparers = new ArrayList<>();

    private final PythonOutputListener m_stdoutToConsole = // NOSONAR Not intended for serialization.
        new PythonOutputLogger(this::messageToConsole, this::warningToConsole, null);

    private final PythonOutputListener m_stderrorToConsole = // NOSONAR Not intended for serialization.
        new PythonOutputLogger(this::messageToConsole, this::warningToConsole, null);

    private final AtomicBoolean m_resetInProgress = new AtomicBoolean(false);

    private Variable[] m_variables; // NOSONAR Not intended for serialization.

    /**
     * @param parent parent the enclosing node dialog
     * @param variableNames an object managing the known variable names in the Python workspace (the "magic variables")
     * @param optionsPanel the options panel of the dialog
     * @param executablePanel the executable selection panel of the dialog
     */
    public PythonSourceCodePanel(final NodeDialogPane parent, final VariableNames variableNames,
        final PythonSourceCodeOptionsPanel optionsPanel, final PythonExecutableSelectionPanel executablePanel) {
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames, optionsPanel);
        m_parent = parent;
        m_optionsPanel = optionsPanel;
        m_executablePanel = executablePanel;
        optionsPanel.addSerializationOptionsChangeListener(this::setSerializationOptions);
        executablePanel.addChangeListener(
            e -> setPythonCommand(executablePanel.getPythonVersion(), executablePanel.getPythonCommand()));
    }

    @Override
    public void loadSettingsFrom(final SourceCodeConfig config, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(config, specs);
        setSerializationOptions(m_optionsPanel.getSerializationOptions());
        setPythonCommand(m_executablePanel.getPythonVersion(), m_executablePanel.getPythonCommand());
    }

    @Override
    public void open() {
        super.open();
        startKernelManagerAsync(m_kernelOptions);
    }

    private void startKernelManagerAsync(final PythonKernelOptions kernelOptions) {
        ThreadUtils.threadWithContext(new PythonKernelManagerStartTask(kernelOptions)).start();
    }

    @Override
    public void close() {
        super.close();
        new Thread(new PythonKernelManagerCloseTask()).start();
    }

    /**
     * Update input data tables and objects and put them into the Python workspace.
     *
     * @param inTableSpecs The specs of the new input tables.
     * @param inTables The new input tables. Must match the given table specs but may contain {@code null} entries in
     *            case there is no data available at the corresponding input port.
     * @param inPickledObjects The new input objects.
     */
    public void updateData(final DataTableSpec[] inTableSpecs, final BufferedDataTable[] inTables,
        final PickledObjectFile[] inPickledObjects) {
        super.updateSpec(inTableSpecs);
        m_inputData = inTables;
        m_pythonInputObjects = inPickledObjects;
    }

    @Override
    protected void runExec(final String sourceCode) {
        @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
        final PythonKernelManager kernelManagerBeforeLock = getKernelManager();
        if (kernelManagerBeforeLock != null) {
            m_lock.lock();
            try {
                @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
                final PythonKernelManager kernelManager = getKernelManager();
                if (kernelManager != null) {
                    getProgressBar().setIndeterminate(true);
                    getProgressBar().setStringPainted(false);
                    // Keep number of restarts to later know if this execution still belongs to the current kernel
                    // instance.
                    final int kernelRestarts = m_kernelRestarts;
                    // Enables the stop button.
                    setRunning(true);
                    setStatusMessage("Executing...");
                    setStopCallback(new PythonKernelManagerStopTask());
                    kernelManager.execute(sourceCode, new ExecuteResponseHandler(kernelManager, kernelRestarts));
                }
            } finally {
                m_lock.unlock();
            }
        }
    }

    @Override
    public void updateVariables() {
        @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
        final PythonKernelManager kernelManagerBeforeLock = getKernelManager();
        if (kernelManagerBeforeLock != null) {
            m_lock.lock();
            try {
                @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
                final PythonKernelManager kernelManager = getKernelManager();
                if (kernelManager != null) {
                    kernelManager.listVariables(new ListVariablesResponseHandler());
                }
            } finally {
                m_lock.unlock();
            }
        } else {
            m_lock.lock();
            try {
                // If there is no kernel running we cannot have variables defined
                m_variables = new Variable[0];
                setVariables(m_variables);
            } finally {
                m_lock.unlock();
            }
        }
    }

    @Override
    protected void runReset() {
        if (getKernelManagerWrapper() != null) {
            m_lock.lock();
            try {
                if (getKernelManagerWrapper() != null) {
                    switchToNewKernel(m_kernelOptions);
                }
            } finally {
                m_lock.unlock();
            }
        }
    }

    @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
    @Override
    protected List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column) {
        final PythonKernelManager kernelManagerBeforeLock = getKernelManager();
        if (kernelManagerBeforeLock != null) {
            m_lock.lock();
            final FutureTask<List<Completion>> completionsFuture;
            try {
                final var completionsCallable = new CompletionsCallable();
                completionsFuture = new FutureTask<>(completionsCallable);
                final PythonKernelManager kernelManager = getKernelManager();
                if (kernelManager != null) {
                    kernelManager.autoComplete(sourceCode, line, column, (response, exception) -> {
                        completionsCallable.m_provider = provider;
                        completionsCallable.m_response = response;
                        completionsCallable.m_exception = exception;
                        completionsFuture.run();
                    });
                }
            } finally {
                m_lock.unlock();
            }
            try {
                return completionsFuture.get(5, TimeUnit.SECONDS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (final ExecutionException ex) {
                LOGGER.debug(ex);
            } catch (final TimeoutException ex) { // NOSONAR
                // Ignore, timeout is to be expected.
            }
        }
        return Collections.emptyList();
    }

    @Deprecated
    @Override
    protected void finalize() throws Throwable { // NOSONAR
        close();
        super.finalize();
    }

    /**
     * Logs the given error in the console as error and optionally sets a status message.
     *
     * @param exception The exception to log
     * @param statusMessage The new status message or null if it should not be changed
     */
    private void logError(final Exception exception, final String statusMessage) {
        if (exception instanceof SocketException) {
            setInteractive(false);
            close();
            errorToConsole("Connection to Python lost");
        } else {
            if ((exception.getMessage() != null) && !exception.getMessage().isEmpty()) {
                errorToConsole(exception.getMessage());
            }
        }
        if (statusMessage != null) {
            setStatusMessage(statusMessage);
        }
    }

    @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
    @Override
    protected ImageContainer getOutImage(final String name) {
        if (getKernelManager() != null) {
            m_lock.lock();
            try {
                final PythonKernelManager kernelManager = getKernelManager();
                return getImageFromKernel(name, kernelManager);
            } catch (final IOException ex) {
                LOGGER.debug(ex);
                return null;
            } finally {
                m_lock.unlock();
            }
        }
        return null;
    }

    private ImageContainer getImageFromKernel(final String name, final PythonKernelManager kernelManager)
        throws IOException {
        if (kernelManager != null && m_variables != null) {
            for (final Variable v : m_variables) {
                if (v.getName().contentEquals(name)) {
                    return kernelManager.getImage(name);
                }
            }
        }
        return null;
    }

    @Override
    protected String createVariableAccessString(final String variable, final String field) {
        return variable + "['" + field.replace("\\", "\\\\").replace("'", "\\'") + "']";
    }

    /**
     * Register a workspace preparer to be used before user code is executed.
     *
     * @param workspacePreparer the workspace preparer
     */
    public void registerWorkspacePreparer(final WorkspacePreparer workspacePreparer) {
        m_workspacePreparers.add(workspacePreparer);
    }

    /**
     * Unregister a workspace preparer.
     *
     * @param workspacePreparer the workspace preparer
     * @return success yes/no
     */
    public boolean unregisterWorkspacePreparer(final WorkspacePreparer workspacePreparer) {
        return m_workspacePreparers.remove(workspacePreparer); // NOSONAR No performance hot spot.
    }

    @SuppressWarnings("deprecation")
    private synchronized void setPythonCommand(final PythonVersion pythonVersion, final PythonCommand pythonCommand) {
        final PythonCommand oldCommand = m_kernelOptions.getUsePython3() //
            ? m_kernelOptions.getPython3Command() //
            : m_kernelOptions.getPython2Command();
        if (m_kernelOptions.getPythonVersion() != pythonVersion || !Objects.equals(oldCommand, pythonCommand)) {
            m_kernelOptions = m_kernelOptions.forPythonVersion(pythonVersion);
            m_kernelOptions = m_kernelOptions.getUsePython3() //
                ? m_kernelOptions.forPython3Command(pythonCommand) //
                : m_kernelOptions.forPython2Command(pythonCommand);
            runResetJob();
        }
    }

    private synchronized void setSerializationOptions(final SerializationOptions serializationOptions) {
        final String serializerId =
            new PythonFlowVariableOptions(m_parent.getAvailableFlowVariables(StringType.INSTANCE)).getSerializerId()
                .orElse(null);
        final PythonKernelOptions kernelOptions =
            m_kernelOptions.forSerializationOptions(serializationOptions.forSerializerId(serializerId));
        if (!kernelOptions.equals(m_kernelOptions)) {
            m_kernelOptions = kernelOptions;
            runResetJob();
        }
    }

    /**
     * Runs the reset job for the dialog if its not already running.
     */
    private void runResetJob() {
        if (getKernelManagerWrapper() != null && !m_resetInProgress.get()) {
            m_resetInProgress.set(true);
            setRunning(true);
            ThreadUtils.threadWithContext(this::runReset).start();
        }
    }

    /**
     * Add a required module in order for the caller to work properly.
     *
     * @param name the module name
     */
    public void addAdditionalRequiredModule(final String name) {
        m_kernelOptions = m_kernelOptions.forAddedAdditionalRequiredModuleNames(Arrays.asList(name));
    }

    private PythonKernelManager getKernelManager() {
        if (m_kernelManagerQueue.peekLast() != null) {
            return m_kernelManagerQueue.peekLast().getManager();
        }
        return null;
    }

    private PythonKernelManagerWrapper getKernelManagerWrapper() {
        return m_kernelManagerQueue.peekLast();
    }

    private void switchToNewKernel(final PythonKernelOptions kernelOptions) {
        final PythonKernelManagerWrapper managerWrapper = m_kernelManagerQueue.peekLast();
        if (managerWrapper != null) {
            close();
            startKernelManagerAsync(kernelOptions);
        }
    }

    @Override
    protected void setRowLimit(final int rowLimit) {
        super.setRowLimit(rowLimit);
        runResetJob();
    }

    private static final class PythonKernelManagerWrapper {

        private final PythonKernelManager m_manager;

        PythonKernelManagerWrapper(final PythonKernelManager manager) {
            m_manager = manager;
        }

        boolean holdsManager() {
            return m_manager != null;
        }

        PythonKernelManager getManager() {
            return m_manager;
        }
    }

    private final class PythonKernelManagerStartTask implements Runnable {

        private final PythonKernelOptions m_kernelOptionsAtStartTime;

        private PythonKernelManagerStartTask(final PythonKernelOptions kernelOptions) {
            m_kernelOptionsAtStartTime = kernelOptions;
        }

        @Override
        public void run() {
            setInteractive(false);
            setRunning(false);
            m_lock.lock();
            try {
                startKernelManager();
            } finally {
                m_lock.unlock();
            }
            @SuppressWarnings("resource") // Will be closed by one of the stop/close tasks below.
            final PythonKernelManager kernelManager = getKernelManager();
            if (kernelManager != null) {
                putData();
                setInteractive(true);
            }
        }

        private void startKernelManager() {
            setStatusMessage("Starting Python...");
            m_resetInProgress.set(false);
            m_kernelRestarts++;
            PythonKernelManager manager = null;
            try { // NOSONAR We do not want to close the manager here.
                manager = new PythonKernelManager(m_kernelOptionsAtStartTime);
            } catch (final Exception ex) { // NOSONAR Exception will otherwise be lost at the end of the thread.
                String error;
                if (ex instanceof PythonException) {
                    // Errors like failing installation tests should be passed through to the user.
                    error = ex.getMessage();
                    if (ex instanceof PythonInstallationTestException) {
                        error += "\nNote: you can create a new Conda environment that contains all required packages "
                            + "via the Preferences.";
                    }
                } else {
                    LOGGER.error(ex);
                    error = "Could not start Python. Please refer to the KNIME console and log file for details.";
                }
                errorToConsole(error);
                setStopped();
                setStatusMessage("Error while starting Python");
            } finally {
                m_kernelManagerQueue.addLast(new PythonKernelManagerWrapper(manager));
            }
            if (manager != null) {
                // Stream Python's stdout/stderr output to the dialog's console.
                m_stdoutToConsole.setDisabled(false);
                manager.addStdoutListener(m_stdoutToConsole);
                m_stderrorToConsole.setDisabled(false);
                manager.addStderrorListener(m_stderrorToConsole);
                setStatusMessage("Python started successfully");
            }
        }

        /**
         * Puts the input data into Python (if input data is available and the Python kernel is running).
         */
        @SuppressWarnings("resource") // The kernel manager will be closed along with this panel at the latest.
        private void putData() {
            final PythonKernelManager kernelManagerBeforeWaitForEDT = getKernelManager();
            if (kernelManagerBeforeWaitForEDT != null) {
                setStopCallback(new PythonKernelManagerStopTask());
                setRunning(true);
                setStatusMessage("Loading input data into Python");
                m_progressMonitor = new JProgressBarProgressMonitor(getProgressBar());
                final int kernelRestartsBeforeLock = m_kernelRestarts;
                final PythonKernelManager kernelManagerBeforeLock = getKernelManager();
                if (kernelManagerBeforeLock != null) {
                    m_lock.lock();
                    try {
                        final PythonKernelManager kernelManager = getKernelManager();
                        putDataIntoKernel(kernelManager, kernelRestartsBeforeLock);
                    } finally {
                        m_lock.unlock();
                    }
                }
            }
        }

        private void putDataIntoKernel(final PythonKernelManager kernelManager, final int kernelRestartsBeforeLock) {
            if (kernelManager != null) {
                // Don't push data if we are resetting the kernel anyways
                if (m_resetInProgress.get()) {
                    return;
                }
                kernelManager.putData(getVariableNames().getInputTables(), m_inputData,
                    getVariableNames().getFlowVariables(), getFlowVariables(), getVariableNames().getInputObjects(),
                    m_pythonInputObjects, new PutDataResponseHandler(kernelRestartsBeforeLock),
                    new ExecutionMonitor(m_progressMonitor), getRowLimit());
                for (final WorkspacePreparer workspacePreparer : m_workspacePreparers) {
                    @SuppressWarnings("resource") // Kernel will be closed by manager.
                    final PythonKernel kernel = kernelManager.getKernel();
                    workspacePreparer.prepareWorkspace(kernel);
                }
            }
        }
    }

    private final class PythonKernelManagerStopTask implements Runnable {

        @Override
        public void run() {
            m_lock.lock();
            setStatusMessage("Stopping Python...");
            try {
                m_kernelRestarts++;
                if (m_progressMonitor != null) {
                    m_progressMonitor.setCanceled(true);
                }
                @SuppressWarnings("resource") // Closed below.
                final PythonKernelManager kernelManager = getKernelManager();
                if (kernelManager != null) {
                    try {
                        kernelManager.close();
                    } catch (final IllegalStateException ex) {
                        LOGGER.error(ex);
                        errorToConsole(ex.getMessage());
                        setStatusMessage("Error while stopping Python...");
                    }
                }
                // Disable interactivity while we restart the kernel
                setRunning(false);
                setInteractive(false);
                setStopped();
                setStatusMessage("Stopped Python. Restart by resetting the workspace.");
            } finally {
                m_lock.unlock();
            }
        }
    }

    private final class PythonKernelManagerCloseTask implements Runnable {

        @Override
        public void run() {
            m_lock.lock();
            try {
                PythonKernelManagerWrapper managerWrapper = null;
                while (managerWrapper == null) {
                    managerWrapper = m_kernelManagerQueue.pollFirst();
                    if (managerWrapper == null) {
                        Thread.sleep(1000);
                    }
                }
                setInteractive(false);
                m_stdoutToConsole.setDisabled(true);
                m_stderrorToConsole.setDisabled(true);
                if (m_progressMonitor != null) {
                    m_progressMonitor.setCanceled(true);
                }
                if (managerWrapper.holdsManager()) {
                    try (final PythonKernelManager manager = managerWrapper.getManager()) {
                        manager.removeStdoutListener(m_stdoutToConsole);
                        manager.removeStderrorListener(m_stderrorToConsole);
                    }
                }
            } catch (final InterruptedException ex) {
                LOGGER.warn(ex);
                Thread.currentThread().interrupt();
            } finally {
                m_lock.unlock();
            }
        }
    }

    private final class PutDataResponseHandler implements PythonKernelResponseHandler<Void> {

        private final int m_kernelRestartsAtPutDataStartTime;

        private PutDataResponseHandler(final int kernelRestartsAtPutDataStartTime) {
            m_kernelRestartsAtPutDataStartTime = kernelRestartsAtPutDataStartTime;
        }

        @Override
        public void handleResponse(final Void response, final Exception exception) {
            m_lock.lock();
            try {
                if (exception != null) {
                    setErrorMessage(exception);
                } else {
                    updateVariables();
                    setStatusMessage("Successfully loaded input data into Python");
                }
                setRunning(false);
            } finally {
                m_lock.unlock();
            }
        }

        private void setErrorMessage(final Exception exception) {
            @SuppressWarnings("resource") // Handled by the panel.
            final PythonKernelManager kernelManager = getKernelManager();
            if (kernelManager != null
                // Check if we're still in the same kernel or if it has been restarted in the meantime.
                && m_kernelRestarts == m_kernelRestartsAtPutDataStartTime) {
                if (m_progressMonitor != null) {
                    m_progressMonitor.setCanceled(true);
                }
                setInteractive(false);
                if (!(exception.getCause() instanceof CanceledExecutionException)) {
                    logError(exception, "Error while loading input data into Python");
                }
            }
        }
    }

    private final class ExecuteResponseHandler implements PythonKernelResponseHandler<String[]> {

        private final PythonKernelManager m_kernelManager;

        private final int m_kernelRestartsAtExecutionStartTime;

        private ExecuteResponseHandler(final PythonKernelManager kernelManager,
            final int kernelRestartsAtExecutionStartTime) {
            m_kernelManager = kernelManager;
            m_kernelRestartsAtExecutionStartTime = kernelRestartsAtExecutionStartTime;
        }

        @Override
        public void handleResponse(final String[] response, final Exception exception) {
            m_lock.lock();
            // Check if kernel was restarted since start of the
            // execution, if it was we don't care about the response
            // anymore
            try {
                if (m_kernelManager != null
                    // Check if we're still in the same kernel or if it has been restarted in the meantime.
                    && m_kernelRestarts == m_kernelRestartsAtExecutionStartTime) {
                    if (exception != null) {
                        logError(exception, "Error during execution");
                    } else {
                        setStatusMessage("Execution successful");
                    }
                    // Setting running to false will also update the
                    // variables
                    setRunning(false);
                }
            } finally {
                m_lock.unlock();
            }
        }
    }

    private final class ListVariablesResponseHandler implements PythonKernelResponseHandler<List<Map<String, String>>> {

        @Override
        public void handleResponse(final List<Map<String, String>> response, final Exception exception) {
            if (exception != null) {
                setVariables(new Variable[0]);
            } else {
                m_variables = new Variable[response.size()];
                for (int i = 0; i < m_variables.length; i++) {
                    final Map<String, String> variable = response.get(i);
                    m_variables[i] = new Variable(variable.get("name"), variable.get("type"), variable.get("value"));
                }
                setVariables(m_variables);
            }
        }
    }

    private static final class CompletionsCallable implements Callable<List<Completion>> {

        private CompletionProvider m_provider;

        private List<Map<String, String>> m_response;

        private Exception m_exception;

        @Override
        public List<Completion> call() throws Exception {
            final List<Completion> completions = new ArrayList<>();
            if (m_exception == null) {
                for (final Map<String, String> completion : m_response) {
                    String name = completion.get("name");
                    final String type = completion.get("type");
                    String doc = completion.get("doc").trim();
                    if (type.equals("function")) {
                        name += "()";
                    }
                    doc = "<html><body><pre>" + doc.replace("\n", "<br />") + "</pre></body></html>";
                    completions.add(new BasicCompletion(m_provider, name, type, doc));
                }
            }
            return completions;
        }
    }
}
