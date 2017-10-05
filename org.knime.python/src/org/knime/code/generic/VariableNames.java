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
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.code.generic;

public class VariableNames {

    private final String m_flowVariables;
    private final String[] m_inputTables;
    private final String[] m_outputTables;
    private final String[] m_outputImages;
    private final String[] m_pythonInputObjects;
    private final String[] m_pythonOutputObjects;
    private final String[] m_generalInputObjects;
    private final String[] m_generalOutputObjects;
    public VariableNames(final String flowVariables, final String[] inputTables, final String[] outputTables,
        final String[] outputImages, final String[] inputObjects, final String[] outputObjects) {
        this(flowVariables, inputTables, outputTables, outputImages, inputObjects, outputObjects, null, null);
    }
    public VariableNames(final String flowVariables, final String[] inputTables, final String[] outputTables,
        final String[] outputImages, final String[] pythonInputObjects, final String[] pythonOutputObjects,
        final String[] generalInputObjects, final String[] generalOutputObjects) {
        if (flowVariables == null) {
            throw new IllegalArgumentException("flow variables may not be null");
        }
        m_flowVariables = flowVariables;
        m_inputTables = inputTables != null ? inputTables : new String[0];
        m_outputTables = outputTables != null ? outputTables : new String[0];
        m_outputImages = outputImages != null ? outputImages : new String[0];
        m_pythonInputObjects = pythonInputObjects != null ? pythonInputObjects : new String[0];
        m_pythonOutputObjects = pythonOutputObjects != null ? pythonOutputObjects : new String[0];
        m_generalInputObjects = generalInputObjects != null ? generalInputObjects : new String[0];
        m_generalOutputObjects = generalOutputObjects != null ? generalOutputObjects : new String[0];
    }

    public String getFlowVariables() {
        return m_flowVariables;
    }

    public String[] getInputTables() {
        return m_inputTables;
    }

    public String[] getOutputTables() {
        return m_outputTables;
    }

    public String[] getOutputImages() {
        return m_outputImages;
    }

    public String[] getInputObjects() {
        return m_pythonInputObjects;
    }

    public String[] getOutputObjects() {
        return m_pythonOutputObjects;
    }

    public String[] getGeneralInputObjects() {
        return m_generalInputObjects;
    }

    public String[] getGeneralOutputObjects() {
        return m_generalOutputObjects;
    }

}
