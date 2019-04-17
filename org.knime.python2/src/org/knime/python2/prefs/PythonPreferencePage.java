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
package org.knime.python2.prefs;

import static org.knime.python2.prefs.PythonPreferenceUtils.performActionOnWidgetInUiThread;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.knime.core.node.NodeLogger;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractPythonConfigsObserver.PythonConfigsInstallationTestStatusChangeListener;
import org.knime.python2.config.CondaEnvironmentCreationObserver;
import org.knime.python2.config.CondaEnvironmentsConfig;
import org.knime.python2.config.ManualEnvironmentsConfig;
import org.knime.python2.config.PythonConfig;
import org.knime.python2.config.PythonConfigStorage;
import org.knime.python2.config.PythonConfigsObserver;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.config.PythonVersionConfig;
import org.knime.python2.config.SerializerConfig;

/**
 * Preference page for configurations related to the org.knime.python2 plug-in.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public final class PythonPreferencePage extends PreferencePage implements IWorkbenchPreferencePage {

    private ScrolledComposite m_containerScrolledView;

    private Composite m_container;

    private List<PythonConfig> m_configs;

    private PythonEnvironmentTypeConfig m_environmentTypeConfig;

    private StackLayout m_environmentConfigurationLayout;

    private CondaEnvironmentsPreferencePanel m_condaEnvironmentPanel;

    private ManualEnvironmentsPreferencePanel m_manualEnvironmentPanel;

    private PythonConfigsObserver m_configObserver;

    @Override
    public void init(final IWorkbench workbench) {
        // no op
    }

    @Override
    protected Control createContents(final Composite parent) {
        createPageBody(parent);
        createInfoHeader(parent);

        m_configs = new ArrayList<>(5);

        // Python version selection:

        final PythonVersionConfig pythonVersionConfig = new PythonVersionConfig();
        m_configs.add(pythonVersionConfig);
        @SuppressWarnings("unused") // Reference to object is not needed here; everything is done in its constructor.
        final Object unused0 = new PythonVersionPreferencePanel(pythonVersionConfig, m_container);

        // Environment configuration:

        final Group environmentConfigurationGroup = new Group(m_container, SWT.NONE);
        environmentConfigurationGroup.setText("Python environment configuration");
        environmentConfigurationGroup.setLayout(new GridLayout());
        environmentConfigurationGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        // Environment type selection:

        m_environmentTypeConfig = new PythonEnvironmentTypeConfig();
        m_configs.add(m_environmentTypeConfig);
        @SuppressWarnings("unused") // Reference to object is not needed here; everything is done in its constructor.
        final Object unused1 =
            new PythonEnvironmentTypePreferencePanel(m_environmentTypeConfig, environmentConfigurationGroup);
        final Label separator = new Label(environmentConfigurationGroup, SWT.SEPARATOR | SWT.HORIZONTAL);
        separator.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        final Composite environmentConfigurationPanel = new Composite(environmentConfigurationGroup, SWT.NONE);
        m_environmentConfigurationLayout = new StackLayout();
        environmentConfigurationPanel.setLayout(m_environmentConfigurationLayout);
        environmentConfigurationPanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        // Conda environment configuration, including environment creation dialogs:

        final CondaEnvironmentsConfig condaEnvironmentsConfig = new CondaEnvironmentsConfig();
        m_configs.add(condaEnvironmentsConfig);

        final CondaEnvironmentCreationObserver python2EnvironmentCreator = new CondaEnvironmentCreationObserver(
            PythonVersion.PYTHON2, condaEnvironmentsConfig.getCondaDirectoryPath());
        final CondaEnvironmentCreationObserver python3EnvironmentCreator = new CondaEnvironmentCreationObserver(
            PythonVersion.PYTHON3, condaEnvironmentsConfig.getCondaDirectoryPath());

        m_condaEnvironmentPanel = new CondaEnvironmentsPreferencePanel(condaEnvironmentsConfig,
            python2EnvironmentCreator, python3EnvironmentCreator, environmentConfigurationPanel);

        // Manual environment configuration:

        final ManualEnvironmentsConfig manualEnvironmentsConfig = new ManualEnvironmentsConfig();
        m_configs.add(manualEnvironmentsConfig);
        m_manualEnvironmentPanel =
            new ManualEnvironmentsPreferencePanel(manualEnvironmentsConfig, environmentConfigurationPanel);

        // Serializer selection:

        final SerializerConfig serializerConfig = new SerializerConfig();
        m_configs.add(serializerConfig);
        @SuppressWarnings("unused") // Reference to object is not needed here; everything is done in its constructor.
        Object unused2 = new SerializerPreferencePanel(serializerConfig, m_container);

        // Load saved configs from preferences and initialize initial view:

        loadConfigurations();

        displayPanelForEnvironmentType(m_environmentTypeConfig.getEnvironmentType().getStringValue());

        updateDisplayMinSize();

        // Hooks:

        m_environmentTypeConfig.getEnvironmentType().addChangeListener(
            e -> displayPanelForEnvironmentType(m_environmentTypeConfig.getEnvironmentType().getStringValue()));

        m_configObserver =
            new PythonConfigsObserver(pythonVersionConfig, m_environmentTypeConfig, condaEnvironmentsConfig,
                python2EnvironmentCreator, python3EnvironmentCreator, manualEnvironmentsConfig, serializerConfig);

        // Displaying installation test results may require resizing the scroll view.
        m_configObserver.addConfigsTestStatusListener(new PythonConfigsInstallationTestStatusChangeListener() {

            @Override
            public void condaInstallationTestStarting() {
                updateDisplayMinSize();
            }

            @Override
            public void condaInstallationTestFinished(final String errorMessage) {
                updateDisplayMinSize();
            }

            @Override
            public void environmentInstallationTestStarting(final PythonEnvironmentType environmentType,
                final PythonVersion pythonVersion) {
                updateDisplayMinSize();
            }

            @Override
            public void environmentInstallationTestFinished(final PythonEnvironmentType environmentType,
                final PythonVersion pythonVersion, final PythonKernelTestResult testResult) {
                updateDisplayMinSize();
            }
        });

        // Initial installation test:

        m_configObserver.testCurrentPreferences();

        return m_containerScrolledView;
    }

    private void createPageBody(final Composite parent) {
        m_containerScrolledView = new ScrolledComposite(parent, SWT.H_SCROLL | SWT.V_SCROLL);
        m_container = new Composite(m_containerScrolledView, SWT.NONE);
        final GridLayout gridLayout = new GridLayout();
        gridLayout.marginWidth = 0;
        gridLayout.marginHeight = 0;
        m_container.setLayout(gridLayout);

        m_containerScrolledView.setContent(m_container);
        m_containerScrolledView.setExpandHorizontal(true);
        m_containerScrolledView.setExpandVertical(true);
    }

    private void createInfoHeader(final Composite parent) {
        final Link startScriptInfo = new Link(m_container, SWT.NONE);
        startScriptInfo.setLayoutData(new GridData());
        final String message = "See <a href=\"https://docs.knime.com/latest/python_installation_guide/index.html\">"
            + "this guide</a> for details on how to install Python for use with KNIME.";
        startScriptInfo.setText(message);
        final Color gray = new Color(parent.getDisplay(), 100, 100, 100);
        startScriptInfo.setForeground(gray);
        startScriptInfo.addDisposeListener(e -> gray.dispose());
        startScriptInfo.setFont(JFaceResources.getFontRegistry().getItalic(""));
        startScriptInfo.addSelectionListener(new SelectionAdapter() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                try {
                    PlatformUI.getWorkbench().getBrowserSupport().getExternalBrowser().openURL(new URL(e.text));
                } catch (PartInitException | MalformedURLException ex) {
                    NodeLogger.getLogger(PythonPreferencePage.class).error(ex);
                }
            }
        });
    }

    private void displayPanelForEnvironmentType(final String environmentTypeId) {
        final PythonEnvironmentType environmentType = PythonEnvironmentType.fromId(environmentTypeId);
        if (PythonEnvironmentType.CONDA.equals(environmentType)) {
            m_environmentConfigurationLayout.topControl = m_condaEnvironmentPanel.getPanel();
        } else if (PythonEnvironmentType.MANUAL.equals(environmentType)) {
            m_environmentConfigurationLayout.topControl = m_manualEnvironmentPanel.getPanel();
        } else {
            throw new IllegalStateException(
                "Selected Python environment type is neither Conda nor manual. This is an implementation error.");
        }
        updateDisplayMinSize();
    }

    private void updateDisplayMinSize() {
        performActionOnWidgetInUiThread(getControl(), () -> {
            m_container.layout(true, true);
            m_containerScrolledView.setMinSize(m_container.computeSize(SWT.DEFAULT, SWT.DEFAULT));
            return null;
        }, false);
    }

    @Override
    public boolean performOk() {
        saveConfigurations();
        return true;
    }

    @Override
    protected void performApply() {
        saveConfigurations();
        m_configObserver.testCurrentPreferences();
    }

    @Override
    protected void performDefaults() {
        final PythonConfigStorage defaultPreferences = PythonPreferences.DEFAULT;
        for (final PythonConfig config : m_configs) {
            config.loadConfigFrom(defaultPreferences);
        }
    }

    /**
     * Saves the preference page's configurations to the preferences.
     */
    private void saveConfigurations() {
        final PythonConfigStorage currentPreferences = PythonPreferences.CURRENT;
        for (final PythonConfig config : m_configs) {
            config.saveConfigTo(currentPreferences);
        }
    }

    /**
     * Loads the preference page's configuration from the stored preferences.
     */
    private void loadConfigurations() {
        final PythonConfigStorage currentPreferences = PythonPreferences.CURRENT;
        for (final PythonConfig config : m_configs) {
            config.loadConfigFrom(currentPreferences);
        }
    }
}
