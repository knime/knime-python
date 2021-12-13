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
import java.awt.Component;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.function.BinaryOperator;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.util.SharedIcons;

final class InputVariablesTree {

    private final VariableNames m_variableNames;

    private final MutableTreeNode m_root = new DefaultMutableTreeNode();

    private final JTree m_tree = new JTree(m_root);

    private final DefaultTreeModel m_model = (DefaultTreeModel)m_tree.getModel();

    private final JPanel m_panel;

    public InputVariablesTree(final VariableNames variableNames, final RSyntaxTextArea editor,
        final BinaryOperator<String> createVariableAccessStringStrategy, final boolean enableInputColumnAccess) {
        m_variableNames = variableNames;

        m_tree.setRootVisible(false);
        m_tree.setShowsRootHandles(false);
        m_tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        m_tree.setCellRenderer(new InputVariablesTreeCellRenderer());
        m_tree.addMouseListener(new InputVariablesTreeMouseListener(m_tree, editor, createVariableAccessStringStrategy,
            enableInputColumnAccess));

        m_panel = new JPanel(new BorderLayout());
        final JLabel label = new JLabel("Input variables");
        label.setBorder(new EmptyBorder(5, 5, 5, 5));
        m_panel.add(label, BorderLayout.NORTH);
        m_panel.add(new JScrollPane(m_tree), BorderLayout.CENTER);
    }

    public JPanel getPanel() {
        return m_panel;
    }

    public boolean hasEntries() {
        return (m_variableNames.getGeneralInputObjects().length //
            + m_variableNames.getInputObjects().length //
            + m_variableNames.getInputTables().length) > 0;
    }

    public void updateInputs(final DataTableSpec[] specs) {
        for (int i = m_root.getChildCount() - 1; i >= 0; i--) {
            m_root.remove(i);
        }
        int treeIndex = 0;
        final String[] inMisc = m_variableNames.getGeneralInputObjects();
        for (int i = 0; i < inMisc.length; i++) {
            m_root.insert(new DefaultMutableTreeNode(inMisc[i], false), treeIndex++);
        }
        final String[] inObjects = m_variableNames.getInputObjects();
        for (int i = 0; i < inObjects.length; i++) {
            m_root.insert(new DefaultMutableTreeNode(inObjects[i], false), treeIndex++);
        }
        final String[] inTables = m_variableNames.getInputTables();
        for (int i = 0; i < inTables.length; i++) {
            final MutableTreeNode tableNode = new DefaultMutableTreeNode(inTables[i]);
            final DataTableSpec tableSpec = specs[i];
            if (tableSpec != null) {
                for (int j = 0; j < tableSpec.getNumColumns(); j++) {
                    tableNode.insert(new DefaultMutableTreeNode(tableSpec.getColumnSpec(j), false), j);
                }
            }
            m_root.insert(tableNode, treeIndex++);
        }
        m_model.reload(m_root);
        for (int i = 0; i < m_tree.getRowCount(); i++) {
            m_tree.expandRow(i);
        }
    }

    @SuppressWarnings("serial") // Not intended for serialization.
    private static final class InputVariablesTreeCellRenderer extends DefaultTreeCellRenderer {

        @Override
        public Component getTreeCellRendererComponent(final JTree tree, final Object value, final boolean sel,
            final boolean expanded, final boolean leaf, final int row,
            @SuppressWarnings("hiding") final boolean hasFocus) {
            final JLabel cell =
                (JLabel)super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
            final DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
            final Object userObject = node.getUserObject();
            if (userObject instanceof DataColumnSpec) {
                // Column
                final DataColumnSpec spec = (DataColumnSpec)userObject;
                cell.setText(spec.getName());
                cell.setIcon(spec.getType().getIcon());
                cell.setBorder(new EmptyBorder(1, 0, 1, 0));
            } else if (!node.isRoot()) {
                if (node.getChildCount() > 0) {
                    // Non-empty table
                    cell.setIcon((expanded ? SharedIcons.SMALL_ARROW_DOWN : SharedIcons.SMALL_ARROW_RIGHT).get());
                    cell.setBorder(new EmptyBorder(1, 0, 1, 0));
                } else {
                    // Object or empty table
                    cell.setIcon(null);
                    cell.setBorder(new EmptyBorder(1, 5, 1, 0));
                }
            }
            return cell;
        }
    }

    private static final class InputVariablesTreeMouseListener extends MouseAdapter {

        private final JTree m_tree;

        private final RSyntaxTextArea m_editor;

        private final BinaryOperator<String> m_createVariableAccessStringStrategy;

        private final boolean m_enableInputColumnAccess;

        public InputVariablesTreeMouseListener(final JTree tree, final RSyntaxTextArea editor,
            final BinaryOperator<String> createVariableAccessStringStrategy, final boolean enableInputColumnAccess) {
            m_tree = tree;
            m_editor = editor;
            m_createVariableAccessStringStrategy = createVariableAccessStringStrategy;
            m_enableInputColumnAccess = enableInputColumnAccess;
        }

        @Override
        public void mouseClicked(final MouseEvent e) {
            if (e.getClickCount() == 2) {
                final TreePath path = m_tree.getPathForLocation(e.getX(), e.getY());
                final DefaultMutableTreeNode node = (DefaultMutableTreeNode)path.getLastPathComponent();
                final Object userObject = node.getUserObject();
                String text = null;
                if (userObject instanceof DataColumnSpec) {
                    // Column
                    if (m_enableInputColumnAccess) {
                        final String tableName = path.getParentPath().getLastPathComponent().toString();
                        final String columnName = ((DataColumnSpec)userObject).getName();
                        text = m_createVariableAccessStringStrategy.apply(tableName, columnName);
                    }
                } else if (node.getChildCount() == 0 || !SwingUtilities.isLeftMouseButton(e)) {
                    // Object or table. Exclude left-clicks if it is a non-empty table to avoid conflicts with the
                    // toggling of the tree node.
                    text = userObject.toString();
                }
                if (text != null) {
                    m_editor.replaceSelection(text);
                    m_editor.requestFocus();
                }
            }
        }
    }
}
