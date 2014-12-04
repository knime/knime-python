/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package org.knime.code.python;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.code.generic.ImageContainer;
import org.knime.code.generic.SourceCodePanel;
import org.knime.code.generic.VariableNames;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.python.Activator;
import org.knime.python.PythonKernelTestResult;
import org.knime.python.kernel.PythonKernelManager;
import org.knime.python.kernel.PythonKernelResponseHandler;
import org.knime.python.port.PickledObject;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Source code panel for python code.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonSourceCodePanel extends SourceCodePanel {

	private static final long serialVersionUID = -3111905445745421972L;

	private PythonKernelManager m_kernelManager;
	private BufferedDataTable[] m_inputData = new BufferedDataTable[0];
	private PickledObject[] m_inputObjects = new PickledObject[0];
	private Lock m_lock = new ReentrantLock();
	private int m_kernelRestarts = 0;
	private JProgressBarProgressMonitor m_progressMonitor;

	/**
	 * Create a python source code panel.
	 */
	public PythonSourceCodePanel(final VariableNames variableNames) {
		super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open() {
		super.open();
		if (m_kernelManager == null) {
			setStatusMessage("Starting python...");
			// Start python in another thread, this might take a few seconds
			new Thread(new Runnable() {
				@Override
				public void run() {
					// Test if local python installation is capable of running
					// the kernel
					// This will return immediately if the test result was
					// positive before
					final PythonKernelTestResult result = Activator.testPythonInstallation();
					// Display result message (this might just be a warning
					// about missing optional modules)
					if (!result.getMessage().isEmpty()) {
						errorToConsole(result.getMessage());
					}
					// Check if python kernel can run or not
					if (result.hasError()) {
						setStatusMessage("Error during python start");
					} else {
						try {
							// Start kernel manager which will start the actual
							// kernel
							m_kernelManager = new PythonKernelManager();
							setStatusMessage("Python successfully started");
							putDataIntoPython();
							setInteractive(true);
						} catch (IOException e) {
							logError(e, "Error during python start");
						}
					}
				}
			}).start();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		super.close();
		if (m_kernelManager != null) {
			setInteractive(false);
			if (m_progressMonitor != null) {
				m_progressMonitor.setCanceled(true);
			}
			m_kernelManager.close();
			m_kernelManager = null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateData(BufferedDataTable[] inputData) {
		super.updateData(inputData);
		m_inputData = inputData;
		putDataIntoPython();
	}

	/**
	 * {@inheritDoc}
	 */
	public void updateData(BufferedDataTable[] inputData, PickledObject[] inputObjects) {
		super.updateData(inputData);
		m_inputData = inputData;
		m_inputObjects = inputObjects;
		putDataIntoPython();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void runExec(final String sourceCode) {
		if (m_kernelManager != null) {
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
					m_kernelRestarts++;
					// Disable interactivity while we restart
					// the kernel
					setInteractive(false);
					try {
						setStatusMessage("Restarting python...");
						setRunning(false);
						// Kernel manager will stop old python
						// kernel and start a new one
						m_kernelManager.switchToNewKernel();
						setStatusMessage("Python successfully restarted");
						putDataIntoPython();
						setInteractive(true);
					} catch (IOException e) {
						logError(e, "Error during python restart");
					}
					m_lock.unlock();
				}
			});
			// Execute will be run in a separate thread by the kernel manager
			m_kernelManager.execute(sourceCode, new PythonKernelResponseHandler<String[]>() {
				@Override
				public void handleResponse(String[] response, Exception exception) {
					m_lock.lock();
					// Check if kernel was restarted since start of the
					// execution, if it was we don't care about the response
					// anymore
					if (kernelRestarts == m_kernelRestarts) {
						if (exception != null) {
							logError(exception, "Error during execution");
						} else {
							messageToConsole(response[0]);
							errorToConsole(response[1]);
							setStatusMessage("Execution successful");
						}
						// Setting running to false will also update the
						// variables
						setRunning(false);
					}
					m_lock.unlock();
				}
			});
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void updateVariables() {
		if (m_kernelManager != null) {
			m_kernelManager.listVariables(new PythonKernelResponseHandler<List<Map<String, String>>>() {
				@Override
				public void handleResponse(List<Map<String, String>> response, Exception exception) {
					if (exception != null) {
						setVariables(new Variable[0]);
					} else {
						// Create Variable array from response
						Variable[] variables = new Variable[response.size()];
						for (int i = 0; i < variables.length; i++) {
							Map<String, String> variable = response.get(i);
							variables[i] = new Variable(variable.get("name"), variable.get("type"), variable
									.get("value"));
						}
						// Fill variable table
						setVariables(variables);
					}
				}
			});
		} else {
			// If there is no kernel running we can not have variables defined
			setVariables(new Variable[0]);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void runReset() {
		if (m_kernelManager != null) {
			m_kernelManager.resetWorkspace(new PythonKernelResponseHandler<Void>() {
				@Override
				public void handleResponse(Void response, Exception exception) {
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
		if (m_kernelManager != null) {
			m_kernelManager.autoComplete(sourceCode, line, column,
					new PythonKernelResponseHandler<List<Map<String, String>>>() {
						@Override
						public void handleResponse(List<Map<String, String>> response, Exception exception) {
							if (exception == null) {
								for (Map<String, String> completion : response) {
									String name = completion.get("name");
									String type = completion.get("type");
									String doc = completion.get("doc").trim();
									if (type.equals("function")) {
										name += "()";
									}
									doc = "<html><body><pre>" + doc.replace("\n", "<br />") + "</pre></body></html>";
									completions.add(new BasicCompletion(provider, name, type, doc));
								}
							}
							synchronized (completions) {
								completions.notify();
							}
						}
					});
			// We have to wait for the other thread to fill the list
			synchronized (completions) {
				try {
					// Since this is run in Swings UI thread, we don't want to
					// wait for to long
					completions.wait(2000);
				} catch (InterruptedException e) {
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
	 * Puts the input data into python (if input data is available and the
	 * python kernel is running).
	 */
	private void putDataIntoPython() {
		if (m_kernelManager != null) {
			setStopCallback(new Runnable() {
				@Override
				public void run() {
					// The kernel restarts is necessary to know
					// if a response belongs to an old kernel
					// instance
					m_lock.lock();
					m_kernelRestarts++;
					if (m_progressMonitor != null) {
						m_progressMonitor.setCanceled(true);
					}
					m_kernelManager.close();
					// Disable interactivity while we restart
					// the kernel
					setInteractive(false);
					setStatusMessage("Stopped python");
					setRunning(false);
					m_lock.unlock();
				}
			});
			setRunning(true);
			setStatusMessage("Loading input data into python");
			m_progressMonitor = new JProgressBarProgressMonitor(getProgressBar());
			m_kernelManager.putData(getVariableNames().getInputTables(), m_inputData, getVariableNames()
					.getFlowVariables(), getFlowVariables(), getVariableNames().getInputObjects(), m_inputObjects,
					new PythonKernelResponseHandler<Void>() {
						@Override
						public void handleResponse(Void response, Exception exception) {
							if (exception != null) {
								if (m_progressMonitor != null) {
									m_progressMonitor.setCanceled(true);
								}
								if (m_kernelManager != null) {
									m_kernelManager.close();
									m_kernelManager = null;
								}
								setInteractive(false);
								if (!m_progressMonitor.isCanceled()) {
									logError(exception, "Error while loading input data into python");
								}
							} else {
								setStatusMessage("Successfully loaded input data into python");
							}
							setRunning(false);
						}
					}, new ExecutionMonitor(m_progressMonitor), getRowLimit());
		}
	}

	/**
	 * Logs the given error in the console as error and
	 * optionally sets a status message.
	 * 
	 * @param exception
	 *            The exception to log
	 * @param statusMessage
	 *            The new status message or null if it should not be changed
	 */
	private void logError(final Exception exception, final String statusMessage) {
		if (exception instanceof SocketException || exception instanceof InvalidProtocolBufferException) {
			setInteractive(false);
			close();
			errorToConsole("Connection to Python lost");
		} else {
			if (exception.getMessage() != null && !exception.getMessage().isEmpty()) {
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
	protected ImageContainer getOutImage(String name) {
		try {
			return m_kernelManager.getImage(name);
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String createVariableAccessString(String variable, String field) {
		return variable + "['" + field.replace("\\", "\\\\").replace("'", "\\'") + "']";
	}

}
