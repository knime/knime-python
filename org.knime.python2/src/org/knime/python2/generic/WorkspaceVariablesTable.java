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
import java.awt.Font;
import java.awt.Point;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import org.knime.python2.generic.ConsolePanel.Level;
import org.knime.python2.generic.SourceCodePanel.Variable;

/**
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class WorkspaceVariablesTable {

    private static final String[] VARIABLES_COLUMN_NAMES = new String[]{"Name", "Type", "Value"};

    private final DefaultTableModel m_model = new DefaultTableModel(VARIABLES_COLUMN_NAMES, 0) {
        private static final long serialVersionUID = -8702103117733835073L;

        @Override
        public boolean isCellEditable(final int row, final int column) {
            // Read-only table
            return false;
        }
    };

    private final JTable m_table = new JTable(m_model);

    private final JPanel m_panel;

    public WorkspaceVariablesTable(final Font font, final ConsolePanel console) {
        m_table.setFont(font);
        m_table.addMouseListener(new MouseAdapter() {

            @Override
            public void mousePressed(final MouseEvent me) {
                final JTable table = (JTable)me.getSource();
                final Point p = me.getPoint();
                final int row = table.rowAtPoint(p);
                if (me.getClickCount() == 2) {
                    final String value = m_model.getValueAt(row, 2).toString();
                    if (!value.isEmpty()) {
                        final String name = m_model.getValueAt(row, 0).toString();
                        console.print(name + ":\n" + value, Level.INFO);
                    }
                }
            }
        });
        m_panel = new JPanel(new BorderLayout());
        m_panel.add(new JScrollPane(m_table), BorderLayout.CENTER);
    }

    public JPanel getPanel() {
        return m_panel;
    }

    public void setVariables(final Variable[] variables) {
        final Object[][] variablesVector = new Object[variables.length][];
        for (int i = 0; i < variables.length; i++) {
            variablesVector[i] = new Object[3];
            variablesVector[i][0] = variables[i].getName();
            variablesVector[i][1] = variables[i].getType();
            variablesVector[i][2] = variables[i].getValue();
        }
        m_model.setDataVector(variablesVector, VARIABLES_COLUMN_NAMES);
    }

    public void clear() {
        m_model.setRowCount(0);
    }
}
