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
 *   Oct 27, 2020 (marcel): created
 */
package org.knime.python2.generic;

import java.awt.BorderLayout;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.text.JTextComponent;

import org.apache.commons.lang.StringUtils;
import org.fife.ui.autocomplete.AutoCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.autocomplete.DefaultCompletionProvider;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rtextarea.RTextScrollPane;

/**
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
abstract class EditorPanel {

    private final RSyntaxTextArea m_editor;

    private final JPanel m_panel = new JPanel(new BorderLayout());

    private final CompletionProvider m_completionProvider;

    private AutoCompletion m_autoCompletion;

    public EditorPanel(final String syntaxStyle) {
        m_editor = SourceCodePanel.createEditor(syntaxStyle);
        final RTextScrollPane editorScrollPane = new RTextScrollPane(m_editor);
        editorScrollPane.setFoldIndicatorEnabled(true);
        m_panel.add(editorScrollPane, BorderLayout.CENTER);

        // Configure autocompletion.
        m_completionProvider = createCompletionProvider();
        installAutoCompletion();
    }

    /**
     * Creates a completion provider that will be used to suggest completions.
     * <P>
     * The created provider will utilize {@link #getCompletionsFor(CompletionProvider, String, int, int)} to retrieve
     * possible completions.
     *
     * @return The {@link CompletionProvider}
     */
    private CompletionProvider createCompletionProvider() {
        final DefaultCompletionProvider provider = new DefaultCompletionProvider() {
            @Override
            public List<Completion> getCompletions(final JTextComponent comp) {
                final List<Completion> providedCompletions = super.getCompletions(comp);
                // Get source code from editor
                final String sourceCode = comp.getText();
                // Caret position only gives as the number of characters before
                // the cursor
                // We need to inspect the code before the cursor to figure out
                // the line and column numbers
                final String codeBeforeCaret = sourceCode.substring(0, comp.getCaretPosition());
                // Line = how many newlines are in the code?
                final int line = StringUtils.countMatches(codeBeforeCaret, "\n");
                // Column = how long is the last line of the code?
                final int column = codeBeforeCaret.substring(codeBeforeCaret.lastIndexOf("\n") + 1).length();
                // Add completions from getCompletionsFor()
                providedCompletions.addAll(getCompletionsFor(this, sourceCode, line, column));
                return providedCompletions;
            }
        };
        // Automatically suggest after '.'
        provider.setAutoActivationRules(false, ".");
        return provider;
    }

    protected abstract List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column);

    public JPanel getPanel() {
        return m_panel;
    }

    public RSyntaxTextArea getEditor() {
        return m_editor;
    }

    public String getSelectedLines() {
        final String text = m_editor.getText();
        final int start = m_editor.getSelectionStart();
        final int end = m_editor.getSelectionEnd();
        // Check if selection is valid (if no cursor is set start will be bigger than the last index).
        if (start > (text.length() - 1)) {
            return null;
        }
        // Cut lines before selection.
        int cutStart = 0;
        for (int i = 0; i < start; i++) {
            if (text.charAt(i) == '\n') {
                // +1 because we want to cut the \n also.
                cutStart = i + 1;
            }
        }
        // Cut lines after selection.
        int cutEnd = text.length();
        for (int i = text.length() - 1; i >= end; i--) {
            if (text.charAt(i) == '\n') {
                cutEnd = i;
            }
        }
        return text.substring(cutStart, cutEnd);
    }

    public void installAutoCompletion() {
        if (m_autoCompletion != null) {
            m_autoCompletion.uninstall();
        }
        m_autoCompletion = new AutoCompletion(m_completionProvider);
        m_autoCompletion.setAutoActivationDelay(100);
        m_autoCompletion.setAutoActivationEnabled(true);
        m_autoCompletion.setShowDescWindow(true);
        m_autoCompletion.setDescriptionWindowSize(580, 300);
        m_autoCompletion.setParameterAssistanceEnabled(true);
        m_autoCompletion.install(m_editor);
    }
}
