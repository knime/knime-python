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
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.border.EmptyBorder;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class OutputVariablesList {

    private final JPanel m_panel;

    public OutputVariablesList(final VariableNames variableNames, final RSyntaxTextArea editor,
        final JPanel workspaceButtons) {
        final DefaultListModel<String> model = new DefaultListModel<>();
        for (final String outTable : variableNames.getOutputTables()) {
            model.addElement(outTable);
        }
        for (final String outImage : variableNames.getOutputImages()) {
            model.addElement(outImage);
        }
        for (final String outObject : variableNames.getOutputObjects()) {
            model.addElement(outObject);
        }
        for (final String outMisc : variableNames.getGeneralOutputObjects()) {
            model.addElement(outMisc);
        }

        m_panel = new JPanel(new BorderLayout());
        final JLabel label = new JLabel("Output variables");
        label.setBorder(new EmptyBorder(5, 5, 5, 5));
        m_panel.add(label, BorderLayout.NORTH);
        final JList<String> list = new JList<>(model);
        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        list.addMouseListener(new MouseAdapter() {

            @Override
            public void mouseClicked(final MouseEvent evt) {
                if (evt.getClickCount() == 2) {
                    final int index = list.locationToIndex(evt.getPoint());
                    final String output = model.get(index);
                    editor.replaceSelection(output + " = ");
                    editor.requestFocus();
                }
            }
        });
        m_panel.add(new JScrollPane(list), BorderLayout.CENTER);
        m_panel.add(workspaceButtons, BorderLayout.SOUTH);
    }

    public JPanel getPanel() {
        return m_panel;
    }
}
