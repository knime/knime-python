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
 *   Mar 10, 2023 (benjamin): created
 */
package org.knime.python3.nodes.preferences;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.knime.conda.envbundling.environment.CondaPackageCollectionUtil;
import org.knime.core.node.NodeLogger;

/**
 * A preference page for downloading all Conda and pip packages that are used by the installed Python-based extensions.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class PythonExtensionsPreferencePage extends PreferencePage implements IWorkbenchPreferencePage {

    private static final String INFO_TEXT = """
            This preference page allows you to create a folder with all Conda and pip packages that are used by the \
            installed Python-based extensions. This folder enables you to install the extensions on a system without \
            internet access.""";

    private static final String LINK_TO_DOCS =
        "https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#_offline_installation";

    private static final String LINK_TO_DOCS_TEXT =
        "For more information see the <a href=\"" + LINK_TO_DOCS + "\">KNIME Documentation</a>.";

    private static final String DOWNLOAD_BUTTON_TEXT = "Download required packages for offline installation to";

    private Button m_downloadButton;

    private ProgressBar m_progressBar;

    private Text m_logTextBox;

    /** Constructor */
    public PythonExtensionsPreferencePage() {
        super("Python-based Extensions");
    }

    @Override
    public void init(final IWorkbench workbench) {
        setDescription(INFO_TEXT);
    }

    @Override
    protected Control createContents(final Composite parent) {
        noDefaultAndApplyButton();
        createLinkToDocs(parent);

        final var contents = new Composite(parent, SWT.NULL);
        final var layout = new GridLayout();
        layout.numColumns = 1;
        layout.marginHeight = 0;
        layout.marginWidth = 0;
        contents.setLayout(layout);
        contents.setFont(parent.getFont());

        m_downloadButton = new Button(contents, SWT.PUSH | SWT.CENTER);
        m_downloadButton.setText(DOWNLOAD_BUTTON_TEXT);
        m_downloadButton.addSelectionListener(SelectionListener.widgetSelectedAdapter(c -> onDownloadClicked()));

        m_progressBar = new ProgressBar(contents, SWT.SMOOTH | SWT.INDETERMINATE);
        m_progressBar.setLayoutData(new GridData(GridData.FILL, GridData.CENTER, true, false));
        m_progressBar.setVisible(false);

        m_logTextBox = new Text(contents, SWT.READ_ONLY | SWT.MULTI | SWT.WRAP | SWT.V_SCROLL | SWT.BORDER);
        m_logTextBox.setLayoutData(new GridData(GridData.FILL, GridData.FILL, true, true));
        m_logTextBox.setVisible(false);

        return contents;
    }

    private void onDownloadClicked() {
        chooseDirectory().ifPresent(this::downloadPackages);
    }

    private Optional<Path> chooseDirectory() {
        final var fileDialog = new DirectoryDialog(getShell(), SWT.OPEN | SWT.SHEET);
        final var dir = fileDialog.open();
        if (dir == null || dir.isBlank()) {
            // Nothing selected
            return Optional.empty();
        }
        return Optional.of(Path.of(dir));
    }

    private void downloadPackages(final Path target) {
        // Prepare the UI
        performActionOnWidgetInUiThread(m_downloadButton, () -> m_downloadButton.setEnabled(false), false);
        performActionOnWidgetInUiThread(m_progressBar, () -> m_progressBar.setVisible(true), false);
        performActionOnWidgetInUiThread(m_logTextBox, () -> {
            m_logTextBox.setText("");
            m_logTextBox.setVisible(true);
        }, false);

        // Run the collection and download in a separate thread
        new Thread(() -> {
            CondaPackageCollectionUtil.collectAndDownloadPackages(target,
                l -> performActionOnWidgetInUiThread(m_logTextBox, () -> m_logTextBox.append(l + "\n"), true));

            // Done: Reset the UI but leave the log text visible
            performActionOnWidgetInUiThread(m_downloadButton, () -> m_downloadButton.setEnabled(true), false);
            performActionOnWidgetInUiThread(m_progressBar, () -> m_progressBar.setVisible(false), false);
        }).start();
    }

    private static void createLinkToDocs(final Composite parent) {
        var linkToDocs = new Link(parent, SWT.NONE);
        linkToDocs.setLayoutData(new GridData());
        linkToDocs.setText(LINK_TO_DOCS_TEXT);
        var gray = new Color(parent.getDisplay(), 100, 100, 100);
        linkToDocs.setForeground(gray);
        linkToDocs.addDisposeListener(e -> gray.dispose());
        linkToDocs.setFont(JFaceResources.getFontRegistry().getItalic(""));
        linkToDocs.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(final SelectionEvent e) {
                try {
                    PlatformUI.getWorkbench().getBrowserSupport().getExternalBrowser().openURL(new URL(e.text));
                } catch (PartInitException | MalformedURLException ex) {
                    NodeLogger.getLogger(PythonExtensionsPreferencePage.class).error(ex);
                }
            }
        });
    }

    /**
     * @throws RuntimeException If any exception occurs while executing {@link action}. {@link SWTException SWT
     *             exceptions} caused by disposed SWT components may be suppressed.
     */
    private static void performActionOnWidgetInUiThread(final Widget widget, final Runnable action,
        final boolean performAsync) {
        final AtomicReference<RuntimeException> exception = new AtomicReference<>();
        try {
            final Consumer<Runnable> executionMethod = performAsync //
                ? widget.getDisplay()::asyncExec //
                : widget.getDisplay()::syncExec;
            executionMethod.accept(() -> {
                if (!widget.isDisposed()) {
                    try {
                        action.run();
                    } catch (final Exception ex) {
                        exception.set(new RuntimeException(ex));
                    }
                }
            });
        } catch (final SWTException ex) {
            // Display or control have been disposed - ignore.
        }
        if (exception.get() != null) {
            throw exception.get();
        }
    }
}
