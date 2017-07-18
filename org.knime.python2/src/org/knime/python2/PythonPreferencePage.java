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
package org.knime.python2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.knime.core.node.NodeLogger;
import org.knime.python2.PythonPathEditor.PythonVersionId;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtension;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.osgi.service.prefs.BackingStoreException;

/**
 * Preference page for python related configurations.
 *
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonPreferencePage extends PreferencePage
implements IWorkbenchPreferencePage, DefaultPythonVersionObserver, ExecutableObserver {

    /**
     * Python 2 executable path configuration string
     */
    public static final String PYTHON_2_PATH_CFG = "python2Path";

    /**
     * Python 3 executable path configuration string
     */
    public static final String PYTHON_3_PATH_CFG = "python3Path";

    /**
     * Serializer id configuration string
     */
    public static final String SERIALIZER_ID_CFG = "serializerId";

    /**
     * Default python version configuration string
     */
    public static final String DEFAULT_PYTHON_OPTION_CFG = "defaultPythonOption";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPreferencePage.class);

    private Display m_display;

    private ScrolledComposite m_sc;

    private Composite m_container;

    private PythonPathEditor m_python2;

    private PythonPathEditor m_python3;

    private Combo m_serializer;

    private static List<String> m_serializerIds;

    private static String[] m_serializerNames;

    private final List<DefaultPythonVersionOption> m_defaultPythonVersionOptions =
            new ArrayList<DefaultPythonVersionOption>();

    /**
     * Gets the currently configured python 2 path.
     *
     * @return Path to the python 2 executable
     */
    public static String getPython2Path() {
        return Platform.getPreferencesService().getString(Activator.PLUGIN_ID, PYTHON_2_PATH_CFG,
            getDefaultPython2Path(), null);
    }

    /**
     * Gets the currently configured python 3 path.
     *
     * @return Path to the python 3 executable
     */
    public static String getPython3Path() {
        return Platform.getPreferencesService().getString(Activator.PLUGIN_ID, PYTHON_3_PATH_CFG,
            getDefaultPython3Path(), null);
    }

    /**
     * Gets the currently configured serializatin library id.
     *
     * @return serializatin library id
     */
    public static String getSerializerId() {
        return Platform.getPreferencesService().getString(Activator.PLUGIN_ID, SERIALIZER_ID_CFG,
            getDefaultSerializerId(), null);
    }

    /**
     * Gets the currently configured default python version.
     *
     * @return the default python version
     */
    public static String getDefaultPythonOption() {
        return Platform.getPreferencesService().getString(Activator.PLUGIN_ID, DEFAULT_PYTHON_OPTION_CFG,
            getDefaultDefaultPythonOption(), null);
    }

    /**
     * @return a list of the ids of all serialization libraries which are available to the user
     */
    public static List<String> getAvailableSerializerIds() {
        initListOfSerializers(false);
        return m_serializerIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbench workbench) {
        //
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean performOk() {
        applyOptions();
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void performApply() {
        applyOptions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void performDefaults() {
        setPython2Path(getDefaultPython2Path());
        setPython3Path(getDefaultPython3Path());
        setSerializerId(getDefaultSerializerId());
        setDefaultPythonOption(getSelectedDefaultPythonOption());
        testPythonInstallation();
    }

    /**
     * Set the configuration according to the dialog state and test the python installation.
     */
    private void applyOptions() {
        setPython2Path(m_python2.getPythonPath());
        setPython3Path(m_python3.getPythonPath());
        setSerializerId(getSelectedSerializer());
        setDefaultPythonOption(getSelectedDefaultPythonOption());
        testPythonInstallation();
    }

    private static String getDefaultPython2Path() {
        return DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID).get(PYTHON_2_PATH_CFG,
            PythonPreferenceInitializer.DEFAULT_PYTHON_2_PATH);
    }

    private static String getDefaultPython3Path() {
        return DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID).get(PYTHON_3_PATH_CFG,
            PythonPreferenceInitializer.DEFAULT_PYTHON_3_PATH);
    }

    private static String getDefaultSerializerId() {
        return DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID).get(SERIALIZER_ID_CFG,
            PythonPreferenceInitializer.DEFAULT_SERIALIZER_ID);
    }

    private static String getDefaultDefaultPythonOption() {
        return DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID).get(DEFAULT_PYTHON_OPTION_CFG,
            PythonPreferenceInitializer.DEFAULT_DEFAULT_PYTHON_OPTION_CFG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Control createContents(final Composite parent) {
        m_display = parent.getDisplay();
        m_sc = new ScrolledComposite(parent, SWT.H_SCROLL | SWT.V_SCROLL);
        m_container = new Composite(m_sc, SWT.NONE);
        final GridLayout gridLayout = new GridLayout();
        gridLayout.numColumns = 2;
        m_container.setLayout(gridLayout);
        GridData gridData = new GridData();
        gridData.horizontalSpan = 2;
        final Link startScriptInfo = new Link(m_container, SWT.NONE);
        startScriptInfo.setLayoutData(gridData);
        final String message =
                "See the <a href=\"http://tech.knime.org/faq#q28\">FAQ</a> for details on how to use a start script";
        startScriptInfo.setText(message);
        final Color gray = new Color(parent.getDisplay(), 100, 100, 100);
        startScriptInfo.setForeground(gray);
        startScriptInfo.addDisposeListener(new DisposeListener() {
            @Override
            public void widgetDisposed(final DisposeEvent e) {
                gray.dispose();
            }
        });
        startScriptInfo.setFont(JFaceResources.getFontRegistry().getItalic(""));
        startScriptInfo.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(final SelectionEvent e) {
                try {
                    PlatformUI.getWorkbench().getBrowserSupport().getExternalBrowser().openURL(new URL(e.text));
                } catch (PartInitException | MalformedURLException ex) {
                    LOGGER.error(ex);
                }
            }
        });
        m_python2 = new PythonPathEditor(PythonVersionId.PYTHON2, "Path to Python 2 executable", m_container);
        m_python2.setPythonPath(getPython2Path());
        m_python2.setExecutableObserver(this);
        gridData = new GridData();
        gridData.horizontalSpan = 2;
        gridData.grabExcessHorizontalSpace = true;
        gridData.grabExcessVerticalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        gridData.verticalAlignment = SWT.FILL;
        m_python2.setLayoutData(gridData);
        m_python3 = new PythonPathEditor(PythonVersionId.PYTHON3, "Path to Python 3 executable", m_container);
        m_python3.setPythonPath(getPython3Path());
        m_python3.setExecutableObserver(this);
        addOption(m_python2);
        addOption(m_python3);
        setSelectedDefaultPythonOption(getDefaultPythonOption());
        gridData = new GridData();
        gridData.horizontalSpan = 2;
        gridData.grabExcessHorizontalSpace = true;
        gridData.grabExcessVerticalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        gridData.verticalAlignment = SWT.FILL;
        m_python3.setLayoutData(gridData);
        final Label serializerLabel = new Label(m_container, SWT.NONE);
        serializerLabel.setText("Serialization Library");
        gridData = new GridData();
        serializerLabel.setLayoutData(gridData);
        gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        m_serializer = new Combo(m_container, SWT.NONE);
        m_serializer.setLayoutData(gridData);
        m_sc.setContent(m_container);
        m_sc.setExpandHorizontal(true);
        m_sc.setExpandVertical(true);
        initListOfSerializers(false);
        m_serializer.setItems(m_serializerNames);
        setSelectedSerializer(getSerializerId());
        testPythonInstallation();
        return m_sc;
    }

    /**
     * Initialize the list of available serialization libraries using the "serializationLibrary" extension point. Hidden
     * serialization libraries are not added.
     *
     * @param forceReinit - flag indicating if a reinitailization should be forced if the list is already initialized
     */
    private static void initListOfSerializers(final boolean forceReinit) {
        if ((m_serializerIds == null) || forceReinit) {
            final Collection<SerializationLibraryExtension> extensions = SerializationLibraryExtensions.getExtensions();
            final Map<String, String> sortedExtensions = new TreeMap<String, String>();
            for (final SerializationLibraryExtension extension : extensions) {
                if (!extension.isHidden()) {
                    sortedExtensions.put(extension.getJavaSerializationLibraryFactory().getName(), extension.getId());
                }
            }
            m_serializerNames = new String[sortedExtensions.size()];
            m_serializerIds = new ArrayList<String>();
            int i = 0;
            for (final Entry<String, String> extension : sortedExtensions.entrySet()) {
                m_serializerNames[i] = extension.getKey();
                m_serializerIds.add(extension.getValue());
                i++;
            }
        }
    }

    private void setSelectedSerializer(final String serializerId) {
        m_serializer.select(m_serializerIds.indexOf(serializerId));
    }

    private String getSelectedSerializer() {
        return m_serializerIds.get(m_serializer.getSelectionIndex());
    }

    /**
     * Saves the given python 2 path.
     *
     * @param python2Path Path to the python 2 executable
     */
    private void setPython2Path(final String python2Path) {
        // If python 2 path has changed retest the underling python installation
        final IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode(Activator.PLUGIN_ID);
        prefs.put(PYTHON_2_PATH_CFG, python2Path);
        try {
            prefs.flush();
        } catch (final BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
        m_python2.setPythonPath(python2Path);
    }

    /**
     * Saves the given python 3 path.
     *
     * @param python3Path Path to the python 3 executable
     */
    private void setPython3Path(final String python3Path) {
        // If python 3 path has changed retest the underling python installation
        final IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode(Activator.PLUGIN_ID);
        prefs.put(PYTHON_3_PATH_CFG, python3Path);
        try {
            prefs.flush();
        } catch (final BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
        m_python3.setPythonPath(python3Path);
    }

    /**
     * Saves the selected serialization library's id.
     *
     * @param serializerId the id of the selected serialization library
     */
    private void setSerializerId(final String serializerId) {
        final IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode(Activator.PLUGIN_ID);
        prefs.put(SERIALIZER_ID_CFG, serializerId);
        try {
            prefs.flush();
        } catch (final BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
        setSelectedSerializer(serializerId);
    }

    /**
     * Saves the selected default python version.
     *
     * @param option either "python2" or "python3"
     */
    private void setDefaultPythonOption(final String option) {
        final IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode(Activator.PLUGIN_ID);
        prefs.put(DEFAULT_PYTHON_OPTION_CFG, option);
        try {
            prefs.flush();
        } catch (final BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
        setSelectedDefaultPythonOption(option);
    }

    /**
     * Runs the python test in a separate thread.
     */
    private void testPythonInstallation() {
        m_python2.setInfo("Testing Python 2 installation...");
        m_python2.setError("");
        m_python3.setInfo("Testing Python 3 installation...");
        m_python3.setError("");
        new Thread(new Runnable() {
            @Override
            public void run() {
                final PythonKernelTestResult python2Result = Activator.retestPython2Installation(Collections.emptyList());
                final PythonKernelTestResult python3Result = Activator.retestPython3Installation(Collections.emptyList());
                m_display.asyncExec(new Runnable() {
                    @Override
                    public void run() {
                        if (!getControl().isDisposed()) {
                            setResult(python2Result, m_python2);
                            setResult(python3Result, m_python3);
                            if (python3Result.hasError()) {
                                notifyChange(m_python2);
                            } else if (python2Result.hasError()) {
                                notifyChange(m_python3);
                            }
                            refreshSizes();
                        }
                    }
                });
            }
        }).start();
    }

    /**
     * Updates the result information.
     *
     * @param result The test result
     */
    private static void setResult(final PythonKernelTestResult result, final PythonPathEditor pythonPathEditor) {
        pythonPathEditor.setInfo(result.getVersion() != null ? result.getVersion() : "");
        pythonPathEditor.setError(result.getMessage());
    }

    /**
     * Refreshes the pages layout and size.
     */
    private void refreshSizes() {
        m_container.layout();
        m_sc.setMinSize(m_container.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyChange(final DefaultPythonVersionOption option) {
        for (final DefaultPythonVersionOption anoption : m_defaultPythonVersionOptions) {
            anoption.updateDefaultPythonVersion(option);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addOption(final DefaultPythonVersionOption option) {
        m_defaultPythonVersionOptions.add(option);
        option.setObserver(this);
    }

    private String getSelectedDefaultPythonOption() {
        if (m_python2.isSelected()) {
            return "python2";
        } else if (m_python3.isSelected()) {
            return "python3";
        }
        return "undefined";
    }

    private void setSelectedDefaultPythonOption(final String option) {
        if (option.contentEquals("python2")) {
            notifyChange(m_python2);
        } else if (option.contentEquals("python3")) {
            notifyChange(m_python3);
        }
    }

    /**
     * Validate if the executable paths point to correct python executables as soon as one of those paths is changed.
     */
    @Override
    public void executableUpdated() {
        applyOptions();
    }

}
