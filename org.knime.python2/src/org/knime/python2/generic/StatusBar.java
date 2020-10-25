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

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionListener;

import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.border.BevelBorder;

import org.knime.python2.Activator;

/**
 * A status bar containing a status message, a progress bar for running processes, and a stop button.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
final class StatusBar extends JPanel {

    private static final long serialVersionUID = 3398321765916443444L;

    private final JLabel m_message = new JLabel();

    private final JProgressBar m_runningBar = new JProgressBar();

    private final JButton m_stopButton = new JButton();

    public StatusBar() {
        setBorder(new BevelBorder(BevelBorder.LOWERED));
        setLayout(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        add(m_message, gbc);
        gbc.gridx++;
        gbc.weightx = 0;
        gbc.insets = new Insets(3, 5, 3, 0);
        add(m_runningBar, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 5, 0, 5);
        add(m_stopButton, gbc);
        final Icon stopIcon = new ImageIcon(Activator.getFile(Activator.PLUGIN_ID, "res/stop.gif").getAbsolutePath());
        m_stopButton.setIcon(stopIcon);
        m_stopButton.setToolTipText("Stop execution");
        m_stopButton.setPreferredSize(
            new Dimension(m_stopButton.getPreferredSize().height, m_stopButton.getPreferredSize().height));
        m_runningBar.setPreferredSize(new Dimension(150, m_stopButton.getPreferredSize().height - 6));
        m_runningBar.setIndeterminate(true);
        setRunning(false);
    }

    /**
     * @return The progress bar.
     */
    public JProgressBar getProgressBar() {
        return m_runningBar;
    }

    /**
     * Sets the status message.
     *
     * @param statusMessage The current status message.
     */
    public void setStatusMessage(final String statusMessage) {
        m_message.setText(statusMessage);
    }

    /**
     * Sets the progress bar and stop button visible / invisible.
     *
     * @param running {@code true} if the process is currently running, {@code false} otherwise.
     */
    public void setRunning(final boolean running) {
        m_runningBar.setVisible(running);
        m_stopButton.setVisible(running);
    }

    /**
     * Sets a callback to be called when the stop button is pressed.
     * <P>
     * Note: the callback will be executed in an extra thread and not block the UI thread.
     *
     * @param callback The callback to execute when the stop button is pressed.
     */
    public void setStopCallback(final Runnable callback) {
        final ActionListener[] actionListeners = m_stopButton.getActionListeners();
        for (final ActionListener listener : actionListeners) {
            m_stopButton.removeActionListener(listener);
        }
        m_stopButton.addActionListener(e -> {
            if (callback != null) {
                new Thread(callback).start();
            }
        });
    }
}
