/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * ------------------------------------------------------------------------
 */

package org.knime.python2.kernel;

import java.util.Map;

import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonPreferencePage;

/**
 * Encapsulates all options that may be set for the python nodes via FlowVariables. Offers a factory method for parsing
 * the list of available flow variables for a node into a {@link FlowVariableOptions} object.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */
public class FlowVariableOptions {

    private boolean m_overrulePreferencePage = false;

    private String m_serializerId = null;

    /**
     * Default constructor.
     */
    public FlowVariableOptions() {

    }

    /**
     * Indicates if the preference page options should be overruled.
     *
     * @return yes/no
     */
    public boolean getOverrulePreferencePage() {
        return m_overrulePreferencePage;
    }

    /**
     * Indicate if the preference page options should be overruled.
     *
     * @param overrulePreferencePage overrule yes/no
     */
    public void setOverrulePreferencePage(final boolean overrulePreferencePage) {
        this.m_overrulePreferencePage = overrulePreferencePage;
    }

    /**
     * Gets the serialization library id.
     *
     * @return the serialization library id
     */
    public String getSerializerId() {
        return m_serializerId;
    }

    /**
     * Sets the serialization library id.
     *
     * @param serializerId the new serialization library id
     */
    public void setSerializerId(final String serializerId) {
        this.m_serializerId = serializerId;
    }

    /**
     * Parse the list of available flow variables and process those setting available flow variable options.
     *
     * @param map - the available flow variables as key/value pairs
     * @return a initialized {@link FlowVariableOptions} object
     */
    public static FlowVariableOptions parse(final Map<String, FlowVariable> map) {
        final FlowVariableOptions options = new FlowVariableOptions();
        final FlowVariable fv = map.get("python_serialization_library");
        final String serLib = (fv == null) ? null : fv.getStringValue();
        if ((serLib != null) && PythonPreferencePage.getAvailableSerializerIds().contains(serLib)) {
            options.setOverrulePreferencePage(true);
            options.setSerializerId(serLib);
        } else if (serLib != null) {
            throw new IllegalArgumentException("Serialization library " + serLib + " does not exist.");
        }
        return options;
    }

}
