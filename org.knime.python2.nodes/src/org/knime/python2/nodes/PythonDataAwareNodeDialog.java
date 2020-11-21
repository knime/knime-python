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
 *   Nov 16, 2020 (marcel): created
 */
package org.knime.python2.nodes;

import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Base class for {@link DataAwareNodeDialogPane data-aware} dialogs of Python scripting nodes.
 *
 * @see PythonDataUnawareNodeDialog
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class PythonDataAwareNodeDialog extends DataAwareNodeDialogPane {

    private PythonNodeDialogContent m_content;

    /**
     * Must be called before this instance can be used. Initializing the content via this method instead of via a
     * constructor is required because {@code content} indirectly requires a reference to this instance which cannot be
     * provided during the construction of the instance.
     *
     * @param content This dialog pane's content.
     */
    protected void initializeContent(final PythonNodeDialogContent content) {
        if (m_content == null) {
            m_content = content;
            addTab("Script", m_content.getScriptPanel(), false);
            addTab("Options", m_content.getOptionsPanel(), true);
            addTab("Templates", m_content.getTemplatesPanel(), true);
        } else {
            throw new IllegalStateException("Content has already been initialized.");
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_content.loadSettingsFrom(settings, specs, getCredentialsProvider());
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input)
        throws NotConfigurableException {
        m_content.loadSettingsFrom(settings, input, getCredentialsProvider());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_content.saveSettingsTo(settings);
    }

    @Override
    public boolean closeOnESC() {
        return PythonNodeDialogContent.closeDialogOnESC();
    }

    @Override
    public void onOpen() {
        m_content.onDialogOpen();
    }

    @Override
    public void onClose() {
        m_content.onDialogClose();
    }
}
