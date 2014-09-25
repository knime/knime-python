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
package org.knime.python;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

/**
 * Preference page for python related configurations.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonPreferencePage extends PreferencePage implements IWorkbenchPreferencePage {

	/**
	 * Use the command 'python' without a specified location as default
	 */
	private static final String DEFAULT_PYTHON_PATH = "python";

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPreferencePage.class);

	private Display m_display;

	private ScrolledComposite m_sc;

	private Composite m_container;

	private FileFieldEditor m_pathEditor;

	private Label m_info;

	private Label m_error;

	/**
	 * Gets the currently configured python path.
	 * 
	 * @return Path to the python executable
	 */
	public static String getPythonPath() {
		return Platform.getPreferencesService().getString("org.knime.python", "pythonPath", DEFAULT_PYTHON_PATH, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(IWorkbench workbench) {
		//
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean performOk() {
		setPythonPath(m_pathEditor.getStringValue());
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void performApply() {
		setPythonPath(m_pathEditor.getStringValue());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void performDefaults() {
		m_pathEditor.setStringValue(DEFAULT_PYTHON_PATH);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Control createContents(Composite parent) {
		m_display = parent.getDisplay();
		m_sc = new ScrolledComposite(parent, SWT.H_SCROLL | SWT.V_SCROLL);
		m_container = new Composite(m_sc, SWT.NONE);
		m_container.setLayout(new GridLayout());
		m_pathEditor = new FileFieldEditor("org.knime.python", "Path to Python executable", m_container);
		m_pathEditor.setStringValue(getPythonPath());
		GridData gridData = new GridData();
		gridData.horizontalSpan = 3;
		m_info = new Label(m_container, SWT.NONE);
		m_info.setLayoutData(gridData);
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		m_error = new Label(m_container, SWT.NONE);
		m_error.setLayoutData(gridData);
		final Color red = new Color(parent.getDisplay(), 255, 0, 0);
		m_error.setForeground(red);
		m_error.addDisposeListener(new DisposeListener() {
			@Override
			public void widgetDisposed(DisposeEvent e) {
				red.dispose();
			}
		});
		m_sc.setContent(m_container);
		m_sc.setExpandHorizontal(true);
		m_sc.setExpandVertical(true);
		testPythonInstallation(false);
		return m_sc;
	}

	/**
	 * Saves the given python path.
	 * 
	 * @param pythonPath
	 *            Path to the python executable
	 */
	private void setPythonPath(final String pythonPath) {
		// If python path has changed retest the underling python installation
		boolean retest = !pythonPath.equals(getPythonPath());
		IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode("org.knime.python");
		prefs.put("pythonPath", pythonPath);
		try {
			prefs.flush();
		} catch (BackingStoreException e) {
			LOGGER.error("Could not save preferences: " + e.getMessage(), e);
		}
		if (retest) {
			setInfo("Testing python installation...");
			setError("");
			// Test the python installation now so we don't have to do it later
			testPythonInstallation(true);
		}
	}

	/**
	 * Runs the python test in a separate thread.
	 * 
	 * If the path has not changed since the last test the test will not be
	 * rerun unless the force parameter is true.
	 * 
	 * @param force
	 *            If true the python installation will always be newly tested
	 */
	private void testPythonInstallation(final boolean force) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				final PythonKernelTestResult result = force ? Activator.retestPythonInstallation()
						: Activator.testPythonInstallation();
				m_display.asyncExec(new Runnable() {
					public void run() {
						setResult(result);
					}
				});
			}
		}).start();
	}

	/**
	 * Displays the given message as info.
	 * 
	 * @param message
	 *            The message
	 */
	private void setInfo(final String message) {
		m_info.setText(message);
		refreshSizes();
	}

	/**
	 * Displays the given message as error.
	 * 
	 * @param message
	 *            The message
	 */
	private void setError(final String message) {
		m_error.setText(message);
		refreshSizes();
	}

	/**
	 * Updates the result information.
	 * 
	 * @param result
	 *            The test result
	 */
	private void setResult(final PythonKernelTestResult result) {
		setInfo(result.getVersion() != null ? result.getVersion() : "");
		setError(result.getMessage());
	}

	/**
	 * Refreshes the pages layout and size.
	 */
	private void refreshSizes() {
		m_container.layout();
		m_sc.setMinSize(m_container.computeSize(SWT.DEFAULT, SWT.DEFAULT));
	}

}
