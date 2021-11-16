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
 *   Nov 11, 2021 (marcel): created
 */
package org.knime.python2.config;

import java.awt.Desktop;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ItemEvent;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.JComboBox;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.HyperlinkEvent;

import org.eclipse.jface.window.Window;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.dialogs.PreferencesUtil;
import org.knime.python2.PythonCommand;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;
import org.knime.python2.prefs.PythonPreferences;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
final class PythonKernelBackendSelectionPanel extends JPanel {

    private static final String PYTHON2_LABEL = "pandas.DataFrame";

    private static final String PYTHON3_LABEL = "KNIME Table (experimental)";

    private final JComboBox<String> m_selectionBox = new JComboBox<>();

    private final JEditorPane m_backendInfoText = createBackendInfoText("");

    private final CopyOnWriteArrayList<Consumer<PythonKernelBackendType>> m_backendChangeListeners =
        new CopyOnWriteArrayList<>();

    private final CopyOnWriteArrayList<Consumer<PythonCommand>> m_commandPreferenceChangeListeners =
        new CopyOnWriteArrayList<>();

    public PythonKernelBackendSelectionPanel() {
        setLayout(new GridBagLayout());
        setBorder(BorderFactory.createTitledBorder("Table API selection"));

        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;

        m_selectionBox.addItem(PYTHON2_LABEL);
        m_selectionBox.addItem(PYTHON3_LABEL);
        // No item selected at the beginning, makes sure that loadSettingsFrom triggers a change event in any case.
        m_selectionBox.setSelectedItem(null);
        m_selectionBox.addItemListener(e -> {
            if (e.getStateChange() == ItemEvent.SELECTED) {
                onKernelBackendChanged();
            }
        });
        add(m_selectionBox, gbc);
        gbc.gridy++;
        m_backendInfoText.addHyperlinkListener(this::processHyperlinkEvent);
        add(m_backendInfoText, gbc);
    }

    static JEditorPane createBackendInfoText(final String text) {
        final var infoText = new JEditorPane();
        infoText.setContentType("text/html");
        infoText.setEditable(false);
        infoText.putClientProperty(JEditorPane.HONOR_DISPLAY_PROPERTIES, true);
        final var temp = new JLabel();
        infoText.setFont(temp.getFont());
        infoText.setForeground(temp.getForeground());
        infoText.setBackground(temp.getBackground());
        infoText.setText(text);
        return infoText;
    }

    /**
     * @return The type of the configured {@code link PythonKernelBackend}.
     */
    public PythonKernelBackendType getKernelBackendType() {
        if (PYTHON2_LABEL.equals(m_selectionBox.getSelectedItem())) {
            return PythonKernelBackendType.PYTHON2;
        } else {
            return PythonKernelBackendType.PYTHON3;
        }
    }

    /**
     * @param listener The listener to add. The value passed to the listener is the type of the back end that has been
     *            selected.
     */
    public void addKernelBackendChangeListener(final Consumer<PythonKernelBackendType> listener) {
        m_backendChangeListeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeKernelBackendChangeListener(final Consumer<PythonKernelBackendType> listener) {
        m_backendChangeListeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onKernelBackendChanged() {
        final PythonKernelBackendType backendType = getKernelBackendType();
        for (final Consumer<PythonKernelBackendType> listener : m_backendChangeListeners) {
            listener.accept(backendType);
        }
        if (backendType == PythonKernelBackendType.PYTHON2) {
            showPython2BackendInfoText();
        } else {
            showPython3BackendInfoText();
        }
    }

    private void showPython2BackendInfoText() {
        m_backendInfoText.setText("<html><table width=800px><tr>"
            + "The default table API. Each input table is provided as pandas.DataFrame and each output table is "
            + "expected to be a pandas.DataFrame. This API supports many of KNIME's data types. It is however "
            + "significantly slower than the new experimental KNIME Table API. It moreover requires that all input and "
            + "output tables together fit into memory." //
            + "</tr></table>");
    }

    private void showPython3BackendInfoText() {
        m_backendInfoText.setText("<table width=800px><tr>"
            + "<p>The KNIME Table API brings significant performance improvements over the pandas.DataFrame API and "
            + "enables working with larger-than-memory data." //
            + "<br><br></p>" //
            + "<p><font color=\"FF0000\">The API is currently part of KNIME Labs. It is not yet advised to use "
            + "it in production. Please consider the following prerequisites and limitations before employing it:"
            + "</font></p>" //
            + "<ul>" //
            + "<li>Python&nbsp;2 is not supported.</li>"
            + "<li>Additional packages, namely py4j and pyarrow, are required to be installed in your Python&nbsp;3 "
            + "environment. You can create a new environment that contains these packages via the "
            + "<a href=\"knime://org.knime.python2.PythonPreferencePage\">Preferences</a>.</li>" //
            + "<li><a href=\"https://www.knime.com/blog/improved-performance-with-new-table-backend\">Columnar "
            + "Storage</a> should be enabled for best performance.</li>" //
            + "<li>Extension data types such as images from KNIME Image Processing or molecules from RDKit Nodes for "
            + "KNIME are not supported yet.</li>" //
            + "<li>The API is under active development and may be subject to breaking changes in future releases until "
            + "being promoted from KNIME Labs.</li>" //
            + "</ul>" //
            + "<p>More details on how to transition to the new API can be found "
            // TODO: update link once material is available
            + "<a href=\"https://docs.knime.com/latest/python_installation_guide/\">here</>. There are also example "
            + "workflows that further illustrate its use "
            // TODO: update link once material is available
            + "<a href=\"https://hub.knime.com/knime/spaces/Examples/latest/07_Scripting/03_Python/\">here</a>.</p>"
            + "</tr></table>");
    }

    /**
     * Makes sure that the hyperlinks contained in the info text (see {@link #showPython3BackendInfoText()}) are opened
     * properly.
     */
    private void processHyperlinkEvent(final HyperlinkEvent event) {
        if (event.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
            final var url = event.getURL();
            if (url.getProtocol().equalsIgnoreCase("knime")) {
                // We divert the knime protocol from its intended use a little here: the URL points to the Python
                // preference page which allows the user to create a new Python environment that contains the packages
                // required by the new back end.
                openPreferencePage(url);
            } else {
                // Otherwise, the URL points to the Python documentation or example workflows.
                openBrowser(url);
            }
        }
    }

    private void openPreferencePage(final URL url) {
        // TODO: this only works for the Python integration itself at the moment, not for deep learning.
        final PythonCommand oldPreferredCommand = PythonPreferences.getPython3CommandPreference();
        Display.getDefault().syncExec(() -> { // NOSONAR
            final var shell = Display.getCurrent().getActiveShell();
            final String pythonPreferencePageId = url.getHost();
            final var pythonPreferencePage = PreferencesUtil.createPreferenceDialogOn(shell, pythonPreferencePageId,
                new String[]{pythonPreferencePageId}, null, PreferencesUtil.OPTION_FILTER_LOCKED);
            if (pythonPreferencePage.open() == Window.OK) {
                final PythonCommand newPreferredCommand = PythonPreferences.getPython3CommandPreference();
                if (!newPreferredCommand.equals(oldPreferredCommand)) {
                    onCommandPreferenceChanged(newPreferredCommand);
                }
            }
        });
    }

    private static void openBrowser(final URL url) {
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            try {
                Desktop.getDesktop().browse(url.toURI());
            } catch (final IOException | URISyntaxException ex) { // NOSONAR
                // Ignore - purely optional feature.
            }
        }
    }

    /**
     * @param listener The listener to add. The value passed to the listener is the new preferred command.
     */
    public void addCommandPreferenceChangeListener(final Consumer<PythonCommand> listener) {
        m_commandPreferenceChangeListeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeCommandPreferenceChangeListener(final Consumer<PythonCommand> listener) {
        m_commandPreferenceChangeListeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onCommandPreferenceChanged(final PythonCommand preferredCommand) {
        for (final Consumer<PythonCommand> listener : m_commandPreferenceChangeListeners) {
            listener.accept(preferredCommand);
        }
    }

    /**
     * @param config The configuration from which to load the selected kernel back end.
     */
    public void loadSettingsFrom(final PythonSourceCodeConfig config) {
        if (config.getKernelBackendType() == PythonKernelBackendType.PYTHON2) {
            m_selectionBox.setSelectedItem(PYTHON2_LABEL);
        } else {
            m_selectionBox.setSelectedItem(PYTHON3_LABEL);
        }
    }

    /**
     * @param config The configuration to which to save the selected kernel back end.
     */
    public void saveSettingsTo(final PythonSourceCodeConfig config) {
        config.setKernelBackendType(getKernelBackendType());
    }
}
