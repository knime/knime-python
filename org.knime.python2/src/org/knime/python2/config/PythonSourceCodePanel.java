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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python2.PythonKernelTester;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.generic.SourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.kernel.FlowVariableOptions;
import org.knime.python2.kernel.PythonKernelManager;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelResponseHandler;
import org.knime.python2.kernel.PythonOutputListener;
import org.knime.python2.port.PickledObject;

/**
 * Source code panel for python code.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class PythonSourceCodePanel extends SourceCodePanel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonSourceCodePanel.class);

    private static final long serialVersionUID = -3111905445745421972L;

    //private PythonKernelManager m_kernelManager;

    private ConcurrentLinkedDeque<PythonKernelManager> m_kernelManagerQueue;

    private BufferedDataTable[] m_inputData = new BufferedDataTable[0];

    private PickledObject[] m_pythonInputObjects = new PickledObject[0];

    private final Lock m_lock = new ReentrantLock();

    private int m_kernelRestarts = 0;

    private JProgressBarProgressMonitor m_progressMonitor;

    private final FlowVariableOptions m_flowVariableOptions;

    private PythonKernelOptions m_kernelOptions;

    private boolean m_pythonOptionsHaveChanged = false;

    private final List<WorkspacePreparer> m_workspacePreparers = new ArrayList<WorkspacePreparer>();

    private final PythonOutputListener m_stdoutToConsole;

    private final ConfigurablePythonOutputListener m_stderrorToConsole;

    /**
     * Create a python source code panel.
     *
     * @param variableNames an object managing the known variable names in the python workspace (the "magic variables")
     * @param options options that may be set via flow variables
     */
    public PythonSourceCodePanel(final VariableNames variableNames, final FlowVariableOptions options) {
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames);
        m_flowVariableOptions = options;
        m_kernelOptions = new PythonKernelOptions();
        m_kernelOptions.setFlowVariableOptions(m_flowVariableOptions);
        m_stdoutToConsole = new PythonOutputListener() {

            @Override
            public void messageReceived(final String msg) {
                messageToConsole(msg);
            }
        };

        m_stderrorToConsole = new ConfigurablePythonOutputListener();
        m_kernelManagerQueue = new ConcurrentLinkedDeque<PythonKernelManager>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() {
        super.open();

            setStatusMessage("Starting python...");
            // Start python in another thread, this might take a few seconds
            new Thread(new Runnable() {
                @Override
                public void run() {
                    // Test if local python installation is capable of running
                    // the kernel
                    // This will return immediately if the test result was
                    // positive before
                    final PythonKernelTestResult result = m_kernelOptions.getUsePython3()
                            ? PythonKernelTester.testPython3Installation(m_kernelOptions.getAdditionalRequiredModules(), false) :
                                PythonKernelTester.testPython2Installation(m_kernelOptions.getAdditionalRequiredModules(), false);
                            // Display result message (this might just be a warning
                            // about missing optional modules)
                            if (result.hasError()) {
                                errorToConsole(result.getErrorLog()
                                    + "\nPlease refer to the log file for more details.");
                                setStatusMessage("Error during python start.");
                            } else {
                                try {
                                    // Start kernel manager which will start the actual
                                    // kernel
                                    m_lock.lock();
                                    try {
                                        m_kernelRestarts++;
                                        m_kernelManagerQueue.addLast(new PythonKernelManager(m_kernelOptions));
                                        //Push python stdout content to console live
                                        m_kernelManagerQueue.peekLast().addStdoutListener(m_stdoutToConsole);
                                        m_kernelManagerQueue.peekLast().addStderrorListener(m_stderrorToConsole);
                                        setStatusMessage("Python successfully started");
                                    } finally {
                                        m_lock.unlock();
                                    }
                                    putDataIntoPython();
                                    setInteractive(true);
                                } catch (final IOException e) {
                                    logError(e, "Error during python start");
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
                    PythonKernelManager manager = null;
                    while(manager==null) {
                        manager = m_kernelManagerQueue.pollFirst();
                        if(manager == null) {
                            Thread.sleep(1000);
                        }
                    }
                    setInteractive(false);
                    if (m_progressMonitor != null) {
                        m_progressMonitor.setCanceled(true);
                    }
                    manager.removeStdoutListener(m_stdoutToConsole);
                    manager.removeStderrorListener(m_stderrorToConsole);
                    manager.close();
                } catch (InterruptedException ex) {
                    LOGGER.warn("Interrupted close method!");
                }finally {
                    m_lock.unlock();
                }
            }}).start();

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
        if (m_kernelManagerQueue.peekLast() != null) {
            m_lock.lock();
            try {
                if (m_kernelManagerQueue.peekLast() != null) {
                    getProgressBar().setIndeterminate(true);
                    getProgressBar().setStringPainted(false);
                    // Keep number of restarts to later know if this execution still
                    // belongs to the current kernel instance
                    final int kernelRestarts = m_kernelRestarts;
                    // Enables the stop button
                    setRunning(true);
                    setStatusMessage("Executing...");
                    setStopCallback(new Runnable() {
                        @Override
                        public void run() {
                            // The kernel restarts is necessary to know
                            // if a response belongs to an old kernel
                            // instance
                            m_lock.lock();
                            try {
                                m_kernelRestarts++;
                                // Disable interactivity while we restart
                                // the kernel
                                setInteractive(false);
                                setStatusMessage("Restarting python...");
                                setRunning(false);
                                // Kernel manager will stop old python
                                // kernel and start a new one
                                if (m_kernelManagerQueue.peekLast() != null) {
                                    m_kernelManagerQueue.peekLast().switchToNewKernel(m_kernelOptions);
                                }
                                setStatusMessage("Python successfully restarted");
                                putDataIntoPython();
                                setInteractive(true);
                            } catch (final IOException e) {
                                logError(e, "Error during python restart");
                            } finally {
                                m_lock.unlock();
                            }
                        }
                    });

                    // Execute will be run in a separate thread by the kernel manager
                    m_stderrorToConsole.setAllWarnings(true);
                    m_kernelManagerQueue.peekLast().execute(sourceCode, new PythonKernelResponseHandler<String[]>() {
                        @Override
                        public void handleResponse(final String[] response, final Exception exception) {
                            m_lock.lock();
                            // Check if kernel was restarted since start of the
                            // execution, if it was we don't care about the response
                            // anymore
                            try {
                                if (m_kernelManagerQueue.peekLast() != null && kernelRestarts == m_kernelRestarts) {
                                    if (exception != null) {
                                        logError(exception, "Error during execution");
                                    } else {
                                        if (!response[1].isEmpty()) {
                                            errorToConsole(response[1]);
                                            setStatusMessage("Error during execution");
                                        } else {
                                            setStatusMessage("Execution successful");
                                        }
                                    }
                                    // Setting running to false will also update the
                                    // variables
                                    setRunning(false);
                                }
                                m_stderrorToConsole.setAllWarnings(false);
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
        if (m_kernelManagerQueue.peekLast() != null) {
            m_lock.lock();
            try {
                if (m_kernelManagerQueue.peekLast() != null) {
                    m_kernelManagerQueue.peekLast()
                        .listVariables(new PythonKernelResponseHandler<List<Map<String, String>>>() {
                            @Override
                            public void handleResponse(final List<Map<String, String>> response,
                                final Exception exception) {
                                if (exception != null) {
                                    setVariables(new Variable[0]);
                                } else {
                                    // Create Variable array from response
                                    final Variable[] variables = new Variable[response.size()];
                                    for (int i = 0; i < variables.length; i++) {
                                        final Map<String, String> variable = response.get(i);
                                        variables[i] = new Variable(variable.get("name"), variable.get("type"),
                                            variable.get("value"));
                                    }
                                    // Fill variable table
                                    setVariables(variables);
                                }
                            }
                        });
                }
            } finally {
                m_lock.unlock();
            }
        } else {
            // If there is no kernel running we cannot have variables defined
            setVariables(new Variable[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runReset() {
        if (m_kernelManagerQueue.peekLast() != null) {
            m_lock.lock();
            try {
                if (m_kernelManagerQueue.peekLast() != null) {
                    if (m_pythonOptionsHaveChanged) {
                        m_pythonOptionsHaveChanged = false;
                        try {
                            m_kernelManagerQueue.peekLast().switchToNewKernel(m_kernelOptions);
                        } catch (final IOException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                    m_kernelManagerQueue.peekLast().resetWorkspace(new PythonKernelResponseHandler<Void>() {
                        @Override
                        public void handleResponse(final Void response, final Exception exception) {
                            if (exception != null) {
                                logError(exception, null);
                            }
                            // Update list of variables once the reset has finished
                            updateVariables();
                            // Put input data into python again
                            putDataIntoPython();
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
    protected List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column) {
        // This list will be filled from another thread
        final List<Completion> completions = new ArrayList<Completion>();
        // If no kernel is running we will simply return the empty list of
        // completions
        if (m_kernelManagerQueue.peekLast() != null) {
            m_lock.lock();
            try {
                if (m_kernelManagerQueue.peekLast() != null) {
                    m_kernelManagerQueue.peekLast().autoComplete(sourceCode, line, column,
                        new PythonKernelResponseHandler<List<Map<String, String>>>() {
                            @Override
                            public void handleResponse(final List<Map<String, String>> response,
                                final Exception exception) {
                                if (exception == null) {
                                    for (final Map<String, String> completion : response) {
                                        String name = completion.get("name");
                                        final String type = completion.get("type");
                                        String doc = completion.get("doc").trim();
                                        if (type.equals("function")) {
                                            name += "()";
                                        }
                                        doc =
                                            "<html><body><pre>" + doc.replace("\n", "<br />") + "</pre></body></html>";
                                        completions.add(new BasicCompletion(provider, name, type, doc));
                                    }
                                }
                                synchronized (completions) {
                                    completions.notify();
                                }
                            }
                        });
                }
            } finally {
                m_lock.unlock();
            }
            // We have to wait for the other thread to fill the list
            synchronized (completions) {
                try {
                    // Since this is run in Swings UI thread, we don't want to
                    // wait for to long
                    completions.wait(2000);
                } catch (final InterruptedException e) {
                    //
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
        if (m_kernelManagerQueue.peekLast() != null) {
            setStopCallback(new Runnable() {
                @Override
                public void run() {
                    // The kernel restarts is necessary to know
                    // if a response belongs to an old kernel
                    // instance
                    m_lock.lock();
                    try {
                        m_kernelRestarts++;
                        if (m_progressMonitor != null) {
                            m_progressMonitor.setCanceled(true);
                        }
                        if(m_kernelManagerQueue.peekLast() != null) {
                            m_kernelManagerQueue.peekLast().close();
                        }
                        // Disable interactivity while we restart
                        // the kernel
                        setInteractive(false);
                        setStatusMessage("Stopped python");
                        setRunning(false);
                    } finally {
                        m_lock.unlock();
                    }
                }
            });
            setRunning(true);
            setStatusMessage("Loading input data into python");
            m_progressMonitor = new JProgressBarProgressMonitor(getProgressBar());
            int kernelRestarts = m_kernelRestarts;
            if (m_kernelManagerQueue.peekLast() != null) {
                m_lock.lock();
                try {
                    if (m_kernelManagerQueue.peekLast() != null) {
                        m_kernelManagerQueue.peekLast().putData(getVariableNames().getInputTables(), m_inputData,
                            getVariableNames().getFlowVariables(), getFlowVariables(),
                            getVariableNames().getInputObjects(), m_pythonInputObjects,
                            new PythonKernelResponseHandler<Void>() {
                                @Override
                                public void handleResponse(final Void response, final Exception exception) {
                                    m_lock.lock();
                                    try {
                                        if (exception != null) {
                                            if (m_kernelManagerQueue.peekLast() != null
                                                && kernelRestarts == m_kernelRestarts) {
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
                                            setStatusMessage("Successfully loaded input data into python");
                                        }
                                        setRunning(false);
                                    } finally {
                                        m_lock.unlock();
                                    }
                                }
                            }, new ExecutionMonitor(m_progressMonitor), getRowLimit());
                        for (final WorkspacePreparer workspacePreparer : m_workspacePreparers) {
                            workspacePreparer.prepareWorkspace(m_kernelManagerQueue.peekLast().getKernel());
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
        if (m_kernelManagerQueue.peekLast() != null) {
            m_lock.lock();
            try {
                if (m_kernelManagerQueue.peekLast() != null) {
                    return m_kernelManagerQueue.peekLast().getImage(name);
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
     * Update the internal PythonKernelOptions object with the current configuration.
     *
     * @param pko the currently configured {@link PythonKernelOptions}
     */
    public void setKernelOptions(final PythonKernelOptions pko) {
        pko.setFlowVariableOptions(m_flowVariableOptions);
        for(String module:m_kernelOptions.getAdditionalRequiredModules()) {
            pko.addRequiredModule(module);
        }
        if (pko.equals(m_kernelOptions)) {
            m_pythonOptionsHaveChanged = false;
        } else {
            m_kernelOptions = pko;
            m_pythonOptionsHaveChanged = true;
        }
    }

    /**
     * Add a required module in order for the caller to work properly.
     *
     * @param name  the module name
     */
    public void addAdditionalRequiredModule(final String name) {
        m_kernelOptions.addRequiredModule(name);
    }

    private class ConfigurablePythonOutputListener implements PythonOutputListener {

        private boolean m_allWarnings = false;

        /**
         * Enables special handling of the stderror stream when custom source code is executed.
         * @param on turn handling on / off
         */
        private void setAllWarnings(final boolean on) {
            m_allWarnings = on;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void messageReceived(final String msg) {
            if(!m_allWarnings) {
                errorToConsole(msg);
            } else {
                warningToConsole(msg);
            }
        }
    }
}
