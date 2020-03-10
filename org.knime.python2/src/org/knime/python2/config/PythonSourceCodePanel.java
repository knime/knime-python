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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.DefaultPythonCommand;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonKernelTester;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.generic.SourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.kernel.PythonKernelManager;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonOutputListener;
import org.knime.python2.kernel.PythonOutputLogger;
import org.knime.python2.kernel.messaging.PythonKernelResponseHandler;
import org.knime.python2.port.PickledObject;

/**
 * Source code panel for python code.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class PythonSourceCodePanel extends SourceCodePanel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonSourceCodePanel.class);

    private static final long serialVersionUID = -3111905445745421972L;

    private final NodeDialogPane m_parent;

    private final ConcurrentLinkedDeque<PythonKernelManagerWrapper> m_kernelManagerQueue;

    private BufferedDataTable[] m_inputData = new BufferedDataTable[0];

    private PickledObject[] m_pythonInputObjects = new PickledObject[0];

    private final Lock m_lock = new ReentrantLock();

    private int m_kernelRestarts = 0;

    private JProgressBarProgressMonitor m_progressMonitor;

    private PythonKernelOptions m_kernelOptions;

    private final List<WorkspacePreparer> m_workspacePreparers = new ArrayList<WorkspacePreparer>();

    private final PythonOutputListener m_stdoutToConsole;

    private final PythonOutputListener m_stderrorToConsole;

    private final AtomicBoolean m_resetInProgress;

    private Variable[] m_variables;

    private final Runnable m_stopCallback = new Runnable() {

        @Override
        public void run() {
            m_lock.lock();
            setStatusMessage("Stopping python...");
            try {
                m_kernelRestarts++;
                if (m_progressMonitor != null) {
                    m_progressMonitor.setCanceled(true);
                }
                if (getKernelManager() != null) {
                    try {
                        getKernelManager().close();
                    } catch (IllegalStateException ex) {
                        errorToConsole(ex.getMessage());
                        setStatusMessage("Error while stopping python...");
                    }
                }
                // Disable interactivity while we restart
                // the kernel
                setRunning(false);
                setInteractive(false);
                setStopped();
                setStatusMessage("Stopped python");
            } finally {
                m_lock.unlock();
            }
        }
    };

    /**
     * Create a source code panel.
     *
     * @param parent parent NodeDialogPane
     *
     * @param variableNames an object managing the known variable names in the python workspace (the "magic variables")
     */
    public PythonSourceCodePanel(final NodeDialogPane parent, final VariableNames variableNames) {
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames);
        m_parent = parent;
        m_kernelOptions = new PythonKernelOptions();

        m_stdoutToConsole = new PythonOutputLogger(this::messageToConsole, this::warningToConsole, null);
        m_stderrorToConsole = new PythonOutputLogger(this::messageToConsole, this::warningToConsole, null);

        m_kernelManagerQueue = new ConcurrentLinkedDeque<PythonKernelManagerWrapper>();
        m_resetInProgress = new AtomicBoolean(false);

        initVariableModels(parent);
    }

    /**
     * @param parent
     */
    private void initVariableModels(final NodeDialogPane parent) {
        final FlowVariableModel python2CommandVariable =
            parent.createFlowVariableModel(PythonSourceCodeConfig.CFG_PYTHON2COMMAND, Type.STRING);
        python2CommandVariable.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                Optional<FlowVariable> fv = ((FlowVariableModel)(e.getSource())).getVariableValue();
                final String command;
                if (fv.isPresent()) {
                    command = fv.get().getStringValue();
                } else {
                    command = "";
                }
                setPython2Command(command);
            }
        });

        final FlowVariableModel python3CommandVariable =
            parent.createFlowVariableModel(PythonSourceCodeConfig.CFG_PYTHON3COMMAND, Type.STRING);
        python3CommandVariable.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                Optional<FlowVariable> fv = ((FlowVariableModel)(e.getSource())).getVariableValue();
                final String command;
                if (fv.isPresent()) {
                    command = fv.get().getStringValue();
                } else {
                    command = "";
                }
                setPython3Command(command);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() {
        super.open();
        setStatusMessage("Starting python...");
        startKernelManagerAsync(m_kernelOptions);
    }

    private void startKernelManagerAsync(/* FIXME -unused- */final PythonKernelOptions kernelOptions) {
        // Start python in another thread, this might take a few seconds
        ThreadUtils.threadWithContext(new Runnable() {
            @Override
            public void run() {
                setInteractive(false);
                setRunning(false);
                // Test if local python installation is capable of running
                // the kernel
                // This will return immediately if the test result was
                // positive before
                final PythonKernelTestResult result = m_kernelOptions.getUsePython3()
                    ? PythonKernelTester.testPython3Installation(m_kernelOptions.getPython3Command(),
                        m_kernelOptions.getAdditionalRequiredModules(), false)
                    : PythonKernelTester.testPython2Installation(m_kernelOptions.getPython2Command(),
                        m_kernelOptions.getAdditionalRequiredModules(), false);
                // Display result message (this might just be a warning
                // about missing optional modules)
                if (result.hasError()) {
                    errorToConsole(result.getErrorLog() + "\nPlease refer to the log file for more details.");
                    m_kernelManagerQueue.addLast(new PythonKernelManagerWrapper(null));
                    m_resetInProgress.set(false);
                    setStopped();
                    setStatusMessage("Error during python start.");
                } else {
                    // Start kernel manager which will start the actual
                    // kernel
                    m_lock.lock();
                    try {
                        setStatusMessage("Starting python...");
                        m_resetInProgress.set(false);
                        m_kernelRestarts++;
                        PythonKernelManager manager = null;
                        try {
                            manager = new PythonKernelManager(m_kernelOptions);
                        } catch (final Exception ex) {
                            errorToConsole("Could not start python kernel. Please refer to the KNIME console"
                                + " and log file for details.");
                            setStopped();
                            setStatusMessage("Error during python start");
                        } finally {
                            m_kernelManagerQueue.addLast(new PythonKernelManagerWrapper(manager));
                        }
                        if (manager != null) {
                            //Push python stdout content to console live
                            m_stdoutToConsole.setDisabled(false);
                            manager.addStdoutListener(m_stdoutToConsole);
                            m_stderrorToConsole.setDisabled(false);
                            manager.addStderrorListener(m_stderrorToConsole);
                            setStatusMessage("Python successfully started");
                        }
                    } finally {
                        m_lock.unlock();
                    }
                    if (getKernelManager() != null) {
                        putDataIntoPython();
                        setInteractive(true);
                    }
                }
            }
        }).start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        super.close();
        new Thread(new Runnable() {

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
                        final PythonKernelManager manager = managerWrapper.getManager();
                        manager.removeStdoutListener(m_stdoutToConsole);
                        manager.removeStderrorListener(m_stderrorToConsole);
                        manager.close();
                    }
                } catch (final InterruptedException ex) {
                    LOGGER.warn("Interrupted close method!");
                } finally {
                    m_lock.unlock();
                }
            }
        }).start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateData(final BufferedDataTable[] inputData) {
        super.updateData(inputData);
        m_inputData = inputData;
        //putDataIntoPython();
    }

    /**
     * Get updated input data tables and put them into the python workspace.
     */
    public void updateData() {
        super.updateData(m_inputData);
        //putDataIntoPython();
    }

    /**
     * Update input data tables and objects and put them into the python workspace.
     *
     * @param inputData the new input tables
     * @param inputObjects the new input objects
     */
    public void updateData(final BufferedDataTable[] inputData, final PickledObject[] inputObjects) {
        super.updateData(inputData);
        m_inputData = inputData;
        m_pythonInputObjects = inputObjects;
        //putDataIntoPython();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runExec(final String sourceCode) {
        if (getKernelManager() != null) {
            m_lock.lock();
            try {
                if (getKernelManager() != null) {
                    getProgressBar().setIndeterminate(true);
                    getProgressBar().setStringPainted(false);
                    // Keep number of restarts to later know if this execution still
                    // belongs to the current kernel instance
                    final int kernelRestarts = m_kernelRestarts;
                    // Enables the stop button
                    setRunning(true);
                    setStatusMessage("Executing...");
                    setStopCallback(m_stopCallback);

                    // Execute will be run in a separate thread by the kernel manager
                    getKernelManager().execute(sourceCode, new PythonKernelResponseHandler<String[]>() {
                        @Override
                        public void handleResponse(final String[] response, final Exception exception) {
                            m_lock.lock();
                            // Check if kernel was restarted since start of the
                            // execution, if it was we don't care about the response
                            // anymore
                            try {
                                if (getKernelManager() != null && kernelRestarts == m_kernelRestarts) {
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
                    });
                }
            } finally {
                m_lock.unlock();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateVariables() {
        if (getKernelManager() != null) {
            m_lock.lock();
            try {
                if (getKernelManager() != null) {
                    getKernelManager().listVariables(new PythonKernelResponseHandler<List<Map<String, String>>>() {

                        @Override
                        public void handleResponse(final List<Map<String, String>> response,
                            final Exception exception) {
                            if (exception != null) {
                                setVariables(new Variable[0]);
                            } else {
                                // Create Variable array from response
                                m_variables = new Variable[response.size()];
                                for (int i = 0; i < m_variables.length; i++) {
                                    final Map<String, String> variable = response.get(i);
                                    m_variables[i] =
                                        new Variable(variable.get("name"), variable.get("type"), variable.get("value"));
                                }
                                // Fill variable table
                                setVariables(m_variables);
                            }
                        }
                    });
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column) {
        // This list will be filled from another thread
        final List<Completion> completions = new ArrayList<Completion>();
        // If no kernel is running we will simply return the empty list of
        // completions
        if (getKernelManager() != null) {
            m_lock.lock();
            try {
                final PythonKernelManager kernelManager = getKernelManager();
                if (kernelManager != null) {
                    kernelManager.autoComplete(sourceCode, line, column, (response, exception) -> {
                        if (exception == null) {
                            for (final Map<String, String> completion : response) {
                                String name = completion.get("name");
                                final String type = completion.get("type");
                                String doc = completion.get("doc").trim();
                                if (type.equals("function")) {
                                    name += "()";
                                }
                                doc = "<html><body><pre>" + doc.replace("\n", "<br />") + "</pre></body></html>";
                                completions.add(new BasicCompletion(provider, name, type, doc));
                            }
                        }
                        synchronized (completions) {
                            completions.notifyAll();
                        }
                    });
                }
            } finally {
                m_lock.unlock();
            }
            // We have to wait for the other thread to fill the list
            synchronized (completions) {
                try {
                    // Since this is run in Swing's UI thread, we don't want to wait for too long.
                    completions.wait(2000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return completions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * Puts the input data into python (if input data is available and the python kernel is running).
     */
    private void putDataIntoPython() {
        if (getKernelManager() != null) {
            setStopCallback(m_stopCallback);
            setRunning(true);
            setStatusMessage("Loading input data into python");
            m_progressMonitor = new JProgressBarProgressMonitor(getProgressBar());
            final int kernelRestarts = m_kernelRestarts;
            if (getKernelManager() != null) {
                m_lock.lock();
                try {
                    if (getKernelManager() != null) {
                        //Don't push data if we are resetting the kernel anyways
                        if (m_resetInProgress.get()) {
                            return;
                        }
                        getKernelManager().putData(getVariableNames().getInputTables(), m_inputData,
                            getVariableNames().getFlowVariables(), getFlowVariables(),
                            getVariableNames().getInputObjects(), m_pythonInputObjects,
                            new PythonKernelResponseHandler<Void>() {
                                @Override
                                public void handleResponse(final Void response, final Exception exception) {
                                    m_lock.lock();
                                    try {
                                        if (exception != null) {
                                            if (getKernelManager() != null && kernelRestarts == m_kernelRestarts) {
                                                if (m_progressMonitor != null) {
                                                    m_progressMonitor.setCanceled(true);
                                                }
                                                setInteractive(false);
                                                if ((exception.getCause() == null)
                                                    || !(exception.getCause() instanceof CanceledExecutionException)) {
                                                    logError(exception, "Error while loading input data into python");
                                                }
                                            }
                                        } else {
                                            updateVariables();
                                            setStatusMessage("Successfully loaded input data into python");
                                        }
                                        setRunning(false);
                                    } finally {
                                        m_lock.unlock();
                                    }
                                }
                            }, new ExecutionMonitor(m_progressMonitor), getRowLimit());
                        for (final WorkspacePreparer workspacePreparer : m_workspacePreparers) {
                            workspacePreparer.prepareWorkspace(getKernelManager().getKernel());
                        }
                    }
                } finally {
                    m_lock.unlock();
                }
            }
        }
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
                // Print exception message to console
                errorToConsole(exception.getMessage());
            }
        }
        // Set status message (if not null)
        if (statusMessage != null) {
            setStatusMessage(statusMessage);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ImageContainer getOutImage(final String name) {
        if (getKernelManager() != null) {
            m_lock.lock();
            try {
                if (getKernelManager() != null && m_variables != null) {
                    for (final Variable v : m_variables) {
                        if (v.getName().contentEquals(name)) {
                            return getKernelManager().getImage(name);
                        }
                    }
                }
            } catch (final IOException e) {
                return null;
            } finally {
                m_lock.unlock();
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
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
        return m_workspacePreparers.remove(workspacePreparer);
    }

    /**
     * Update the internal PythonKernelOptions with the new command path
     *
     * @param python2Command the python 2 command
     */
    public synchronized void setPython2Command(final String python2Command) {
        final PythonCommand python2CommandObject = python2Command != null && !python2Command.equals("") //
            ? new DefaultPythonCommand(PythonVersion.PYTHON2, python2Command) //
            : null;
        if (!m_resetInProgress.get() && !m_kernelOptions.getUsePython3()
            && !m_kernelOptions.getPython2Command().equals(python2CommandObject)) {
            m_kernelOptions = m_kernelOptions.forPython2Command(python2CommandObject);
            runResetJob();
        }
    }

    /**
     * Update the internal PythonKernelOptions with the new command path
     *
     * @param python3Command python 3 command
     */
    public synchronized void setPython3Command(final String python3Command) {
        final PythonCommand python3CommandObject = python3Command != null && !python3Command.equals("") //
            ? new DefaultPythonCommand(PythonVersion.PYTHON3, python3Command) //
            : null;
        if (!m_resetInProgress.get() && m_kernelOptions.getUsePython3()
            && !m_kernelOptions.getPython3Command().equals(python3CommandObject)) {
            m_kernelOptions = m_kernelOptions.forPython3Command(python3CommandObject);
            runResetJob();
        }
    }

    /**
     * Update the internal PythonKernelOptions object with the current configuration.
     *
     * @param kernelOptions the currently configured {@link PythonKernelOptions}
     */
    public void setKernelOptions(PythonKernelOptions kernelOptions) {
        final String serializerId =
            new PythonFlowVariableOptions(m_parent.getAvailableFlowVariables()).getSerializerId().orElse(null);
        // First, set serializer id of old options because it influences the returned additional required modules. We
        // don't want to carry around dependencies of a serializer we won't use.
        final Set<PythonModuleSpec> additionalRequiredModules = m_kernelOptions
            .forSerializationOptions(kernelOptions.getSerializationOptions().forSerializerId(serializerId))
            .getAdditionalRequiredModules();
        kernelOptions =
            kernelOptions.forSerializationOptions(kernelOptions.getSerializationOptions().forSerializerId(serializerId))
                .forAddedAdditionalRequiredModules(additionalRequiredModules);
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
            new Thread(new Runnable() {

                @Override
                public void run() {
                    runReset();
                }

            }).start();
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

    private class PythonKernelManagerWrapper {

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
}
