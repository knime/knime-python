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
 *   Oct 26, 2020 (marcel): created
 */
package org.knime.python2.generic;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;

import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.text.BadLocationException;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.knime.core.node.NodeLogger;
import org.knime.python2.Activator;

/**
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class ConsolePanel {

    private final JTextPane m_console = new JTextPane();

    private final JButton m_clearConsole = new JButton();

    private final JPanel m_panel;

    private final Style m_infoStyle;

    private final Style m_warningStyle;

    private final Style m_errorStyle;

    public ConsolePanel() {
        final JPanel consoleButtons = new JPanel(new FlowLayout(FlowLayout.LEADING));
        consoleButtons.add(m_clearConsole);
        m_panel = new JPanel(new BorderLayout());
        m_panel.add(new JScrollPane(m_console), BorderLayout.CENTER);
        m_panel.add(consoleButtons, BorderLayout.EAST);

        final Icon clearIcon = new ImageIcon(Activator.getFile(Activator.PLUGIN_ID, "res/clear.gif").getAbsolutePath());
        m_clearConsole.setIcon(clearIcon);
        m_clearConsole.setToolTipText("Clear console");
        m_clearConsole.setPreferredSize(
            new Dimension(m_clearConsole.getPreferredSize().height, m_clearConsole.getPreferredSize().height));
        m_clearConsole.addActionListener(e -> clear());

        m_infoStyle = m_console.addStyle("normalstyle", null);
        StyleConstants.setForeground(m_infoStyle, Color.black);

        m_warningStyle = m_console.addStyle("warningstyle", null);
        StyleConstants.setForeground(m_warningStyle, Color.blue);

        m_errorStyle = m_console.addStyle("errorstyle", null);
        StyleConstants.setForeground(m_errorStyle, Color.red);

        m_console.setEditable(false);
        m_console.setDragEnabled(true);
        m_console.setText("");
    }

    public JPanel getPanel() {
        return m_panel;
    }

    public Font getFont() {
        return m_console.getFont();
    }

    public void setFont(final Font font) {
        m_console.setFont(font);
    }

    public void print(final String text, final Level level) {
        final Style style;
        if (level == Level.INFO) {
            style = m_infoStyle;
        } else if (level == Level.WARNING) {
            style = m_warningStyle;
        } else if (level == Level.ERROR) {
            style = m_errorStyle;
        } else {
            throw new IllegalStateException("Implementation error.");
        }

        final StyledDocument doc = m_console.getStyledDocument();
        final String string = ScriptingNodeUtils.shortenString(text);
        try {
            doc.insertString(doc.getLength(), string + "\n", style);
        } catch (final BadLocationException e) {
            NodeLogger.getLogger(ConsolePanel.class).debug(e);
            // Ignore
        }
    }

    public void clear() {
        m_console.setText("");
    }

    public enum Level {
            INFO, WARNING, ERROR
    }
}
