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
package org.knime.python3.scripting.nodes.prefs;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.jface.util.IPropertyChangeListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.knime.conda.prefs.CondaPreferences;
import org.knime.core.node.NodeLogger;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractPythonConfigsObserver.PythonConfigsInstallationTestStatusChangeListener;
import org.knime.python2.config.CondaEnvironmentCreationObserver;
import org.knime.python2.config.CondaEnvironmentsConfig;
import org.knime.python2.config.ManualEnvironmentsConfig;
import org.knime.python2.config.PythonConfig;
import org.knime.python2.config.PythonConfigsObserver;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.prefs.AbstractPythonPreferencePage;
import org.knime.python2.prefs.CondaEnvironmentsPreferencePanel;
import org.knime.python2.prefs.ManualEnvironmentsPreferencePanel;

/**
 * Preference page for configurations related to the org.knime.python3.scripting.nodes plug-in.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class Python3ScriptingPreferencePage extends AbstractPythonPreferencePage {

    private PythonEnvironmentTypeConfig m_environmentTypeConfig;

    private StackLayout m_environmentConfigurationLayout;

    private CondaEnvironmentsConfig m_condaEnvironmentsConfig;

    private CondaEnvironmentCreationObserver m_python3EnvironmentCreator;

    private CondaEnvironmentsPreferencePanel m_condaEnvironmentPanel;

    private ManualEnvironmentsConfig m_manualEnvironmentsConfig;

    private ManualEnvironmentsPreferencePanel m_manualEnvironmentPanel;

    private BundledCondaEnvironmentConfig m_bundledCondaEnvironmentConfig;

    private BundledCondaEnvironmentPreferencesPanel m_bundledCondaEnvironmentPanel;

    private PythonConfigsObserver m_configObserver;

    private final IPropertyChangeListener m_condaDirPropertyChangeListener = event -> {
        if ("condaDirectoryPath".equals(event.getProperty()) && m_configObserver != null) {
            m_configObserver.testCurrentPreferences();
        }
    };

    /**
     * Constructs the main preference page of the KNIME Python (Labs) integration.
     */
    public Python3ScriptingPreferencePage() {
        super(Python3ScriptingPreferences.CURRENT, Python3ScriptingPreferences.DEFAULT);
    }

    @Override
    @SuppressWarnings("unused") // References to some panels are not needed; everything is done in their constructor.
    protected void populatePageBody(final Composite container, final List<PythonConfig> configs) {
        createInfoHeader(container);

        // Construct the BundledCondaEnv config first because depending on whether this is available, we adjust the UI
        m_bundledCondaEnvironmentConfig =
            new BundledCondaEnvironmentConfig(Python3ScriptingPreferences.BUNDLED_PYTHON_ENV_ID);
        if (!m_bundledCondaEnvironmentConfig.isAvailable()) {
            addBundledEnvInstallationInfo(container);
        }

        // Environment configuration:
        final Group environmentConfigurationGroup = new Group(container, SWT.NONE);
        environmentConfigurationGroup.setText("Python environment configuration");
        environmentConfigurationGroup.setLayout(new GridLayout());
        environmentConfigurationGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        // Environment type selection:
        m_environmentTypeConfig = Python3ScriptingPreferencesInitializer.getDefaultPythonEnvironmentTypeConfig();
        configs.add(m_environmentTypeConfig);
        new PythonBundledEnvironmentTypePreferencePanel(m_environmentTypeConfig, environmentConfigurationGroup,
            m_bundledCondaEnvironmentConfig.isAvailable());
        final Label separator = new Label(environmentConfigurationGroup, SWT.SEPARATOR | SWT.HORIZONTAL);
        separator.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        final Composite environmentConfigurationPanel = new Composite(environmentConfigurationGroup, SWT.NONE);
        m_environmentConfigurationLayout = new StackLayout();
        environmentConfigurationPanel.setLayout(m_environmentConfigurationLayout);
        environmentConfigurationPanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        // Conda environment:
        m_condaEnvironmentsConfig = Python3ScriptingPreferencesInitializer.getDefaultCondaEnvironmentsConfig();
        configs.add(m_condaEnvironmentsConfig);
        m_python3EnvironmentCreator = new CondaEnvironmentCreationObserver(PythonVersion.PYTHON3);
        m_condaEnvironmentPanel = new CondaEnvironmentsPreferencePanel(m_condaEnvironmentsConfig, null,
            m_python3EnvironmentCreator, environmentConfigurationPanel);

        // Manual environment:
        m_manualEnvironmentsConfig = Python3ScriptingPreferencesInitializer.getDefaultManualEnvironmentsConfig();
        configs.add(m_manualEnvironmentsConfig);
        m_manualEnvironmentPanel =
            new ManualEnvironmentsPreferencePanel(m_manualEnvironmentsConfig, environmentConfigurationPanel, false);

        // Bundled Conda environment:
        configs.add(m_bundledCondaEnvironmentConfig);
        m_bundledCondaEnvironmentPanel =
            new BundledCondaEnvironmentPreferencesPanel(m_bundledCondaEnvironmentConfig, environmentConfigurationPanel);
    }

    private static void createInfoHeader(final Composite parent) {
        final Link installationGuideInfo = new Link(parent, SWT.NONE);
        installationGuideInfo.setLayoutData(new GridData());
        final String message = "See <a href=\"https://docs.knime.com/latest/python_installation_guide/index.html\">"
            + "this guide</a> for details on how to install Python for use with KNIME.";
        installationGuideInfo.setText(message);
        final Color gray = new Color(parent.getDisplay(), 100, 100, 100);
        installationGuideInfo.setForeground(gray);
        installationGuideInfo.addDisposeListener(e -> gray.dispose());
        installationGuideInfo.setFont(JFaceResources.getFontRegistry().getItalic(""));
        installationGuideInfo.addSelectionListener(new SelectionAdapter() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                try {
                    PlatformUI.getWorkbench().getBrowserSupport().getExternalBrowser().openURL(new URL(e.text));
                } catch (PartInitException | MalformedURLException ex) {
                    NodeLogger.getLogger(Python3ScriptingPreferencePage.class).error(ex);
                }
            }
        });
    }

    private static void addBundledEnvInstallationInfo(final Composite parent) {
        final Label bundledEnvInstallationInfo = new Label(parent, SWT.NONE);
        bundledEnvInstallationInfo.setLayoutData(new GridData());
        // TODO: adjust the feature name if we decide to change that in AP-18855
        final String message = "To enable the Python environment bundled with KNIME, please install the "
            + "'KNIME Conda channel pythonscripting' feature.";
        bundledEnvInstallationInfo.setText(message);
        final Color gray = new Color(parent.getDisplay(), 100, 100, 100);
        bundledEnvInstallationInfo.setForeground(gray);
        bundledEnvInstallationInfo.addDisposeListener(e -> gray.dispose());
        bundledEnvInstallationInfo.setFont(JFaceResources.getFontRegistry().getItalic(""));
    }

    @Override
    protected void reflectLoadedConfigurations() {
        var pythonEnvType = PythonEnvironmentType.fromId(m_environmentTypeConfig.getEnvironmentType().getStringValue());

        var showBundledToCondaWarning = false;
        var showCondaToBundledWarning = false;
        if (PythonEnvironmentType.BUNDLED == pythonEnvType && !m_bundledCondaEnvironmentConfig.isAvailable()) {
            showBundledToCondaWarning = true;
            m_environmentTypeConfig.getEnvironmentType().setStringValue(PythonEnvironmentType.CONDA.getId());
        } else if (PythonEnvironmentType.CONDA == pythonEnvType //
            && m_bundledCondaEnvironmentConfig.isAvailable() //
            && Python3ScriptingPreferencesInitializer.isPlaceholderCondaEnvSelected()) {
            showCondaToBundledWarning = true;
            m_environmentTypeConfig.getEnvironmentType().setStringValue(PythonEnvironmentType.BUNDLED.getId());
        }

        displayPanelForEnvironmentType(m_environmentTypeConfig.getEnvironmentType().getStringValue());

        if (showBundledToCondaWarning) {
            setMessage(
                "You had previously selected the 'Bundled' option, but no bundled Python environment is available. "
                    + "Switched to 'Conda'.",
                WARNING);
        } else if (showCondaToBundledWarning) {
            setMessage("You had previously selected the 'Conda' option, but Conda is not configured properly. "
                + "Switched to 'Bundled'.", WARNING);
        }
    }

    private void displayPanelForEnvironmentType(final String environmentTypeId) {
        // As soon as the settings are changed, the warning message can be set to the title of the
        // preference page; as the preference page title points to this message
        setMessage("Python (Labs)", NONE);
        final var environmentType = PythonEnvironmentType.fromId(environmentTypeId);
        if (PythonEnvironmentType.CONDA == environmentType) {
            m_environmentConfigurationLayout.topControl = m_condaEnvironmentPanel.getPanel();
        } else if (PythonEnvironmentType.MANUAL == environmentType) {
            m_environmentConfigurationLayout.topControl = m_manualEnvironmentPanel.getPanel();
        } else if (PythonEnvironmentType.BUNDLED == environmentType) {
            m_environmentConfigurationLayout.topControl = m_bundledCondaEnvironmentPanel.getPanel();
        } else {
            throw new IllegalStateException(
                "Selected Python environment type is neither Bundled, Conda, nor Manual. This is an implementation error.");
        }
        updateDisplayMinSize();
    }

    @Override
    protected void setupHooks() {
        m_environmentTypeConfig.getEnvironmentType().addChangeListener(
            e -> displayPanelForEnvironmentType(m_environmentTypeConfig.getEnvironmentType().getStringValue()));

        m_configObserver = new Python3ScriptingConfigsObserver(m_environmentTypeConfig, m_condaEnvironmentsConfig,
            m_python3EnvironmentCreator, m_manualEnvironmentsConfig, m_bundledCondaEnvironmentConfig);

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

        // Trigger installation test if the Conda directory path changes
        CondaPreferences.addPropertyChangeListener(m_condaDirPropertyChangeListener);

        // Trigger initial installation test.
        m_configObserver.testCurrentPreferences();
    }

    @Override
    protected void performApply() {
        super.performApply();
        m_configObserver.testCurrentPreferences();
    }

    @Override
    public void dispose() {
        super.dispose();
        CondaPreferences.removePropertyChangeListener(m_condaDirPropertyChangeListener);
    }
}
