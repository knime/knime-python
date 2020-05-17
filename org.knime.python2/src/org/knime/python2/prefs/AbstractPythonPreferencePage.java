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
 *   Apr 17, 2020 (marcel): created
 */
package org.knime.python2.prefs;

import static org.knime.python2.prefs.PythonPreferenceUtils.performActionOnWidgetInUiThread;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.python2.config.PythonConfig;
import org.knime.python2.config.PythonConfigStorage;

/**
 * Abstract base class for the preference pages of the KNIME Python integration.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractPythonPreferencePage extends PreferencePage implements IWorkbenchPreferencePage {

    private final PythonConfigStorage m_currentPreferences;

    private final PythonConfigStorage m_defaultPreferences;

    private ScrolledComposite m_containerScrolledView;

    private Composite m_container;

    private List<PythonConfig> m_configs;

    /**
     * Constructs a new base preference page for the KNIME Python integration.
     *
     * @param currentPreferences The storage from/to which this preference page reads/writes its values.
     * @param defaultPreferences The storage from which this preference page reads its default values.
     */
    public AbstractPythonPreferencePage(final PythonConfigStorage currentPreferences,
        final PythonConfigStorage defaultPreferences) {
        m_currentPreferences = currentPreferences;
        m_defaultPreferences = defaultPreferences;
    }

    @Override
    public void init(final IWorkbench workbench) {
        // Nothing to do, by default.
    }

    @Override
    protected Control createContents(final Composite parent) {
        createPageBody(parent);
        m_configs = new ArrayList<>(1);
        populatePageBody(m_container, m_configs);
        loadConfigurations();
        reflectLoadedConfigurations();
        updateDisplayMinSize();
        setupHooks();
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

    /**
     * Called once to allow implementations to populate the body of the preference page with implementation-specific
     * configuration widgets.
     *
     * @param container The container to which widgets can be added.
     * @param configs The list of configurations entries to which entries that correspond to the widgets of the
     *            preference page implementation can be added. These entries are automatically loaded, saved, and reset
     *            to their defaults by this abstract base class.
     */
    protected abstract void populatePageBody(Composite container, List<PythonConfig> configs);

    /**
     * Called once after the preference page has been populated and its configuration entries have been loaded. Allows
     * implementations to initialize their widgets according to the loaded configuration.
     */
    protected abstract void reflectLoadedConfigurations();

    /**
     * Called once after the preference page has been populated and initialized. Allows implementations to setup event
     * handlers and other hooks that react to user actions but not to the initial initialization.
     */
    protected abstract void setupHooks();

    /**
     * Updates the size of preference page such that all currently visible widgets fully fit into the page.
     */
    protected void updateDisplayMinSize() {
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
    }

    @Override
    protected void performDefaults() {
        for (final PythonConfig config : m_configs) {
            config.loadConfigFrom(m_defaultPreferences);
        }
    }

    /**
     * Saves the preference page's configurations to the preferences.
     */
    private void saveConfigurations() {
        for (final PythonConfig config : m_configs) {
            config.saveConfigTo(m_currentPreferences);
        }
    }

    /**
     * Loads the preference page's configuration from the stored preferences.
     */
    private void loadConfigurations() {
        for (final PythonConfig config : m_configs) {
            config.loadConfigFrom(m_currentPreferences);
        }
    }
}
