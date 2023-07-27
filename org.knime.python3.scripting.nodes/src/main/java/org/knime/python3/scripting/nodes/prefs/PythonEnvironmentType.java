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
 *   Feb 11, 2019 (marcel): created
 */
package org.knime.python3.scripting.nodes.prefs;

/**
 * The type of a Python environment.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @since 5.2
 */
public enum PythonEnvironmentType {

        /**
         * Conda environment configuration.
         */
        CONDA("conda", "Conda"),
        /**
         * Manual environment configuration.
         */
        MANUAL("manual", "Manual"),
        /**
         * Bundled conda environment.
         */
        BUNDLED("bundled", "Bundled");

    /**
     * @param environmentTypeId The {@link #getId() id} of the {@link PythonEnvironmentType} to return.
     * @return The type of the given id.
     */
    public static PythonEnvironmentType fromId(final String environmentTypeId) {
        final PythonEnvironmentType type;
        if (CONDA.getId().equals(environmentTypeId)) {
            type = CONDA;
        } else if (MANUAL.getId().equals(environmentTypeId)) {
            type = MANUAL;
        } else if (BUNDLED.getId().equals(environmentTypeId)) {
            type = BUNDLED;
        } else {
            throw new IllegalStateException("Python environment type '" + environmentTypeId
                + "' is neither bundled, conda nor manual. This is an implementation error.");
        }
        return type;
    }

    /**
     * @param environmentTypeName The {@link #getName() name} of the {@link PythonEnvironmentType} to return.
     * @return The type of the given name.
     */
    public static PythonEnvironmentType fromName(final String environmentTypeName) {
        final PythonEnvironmentType type;
        if (CONDA.getName().equals(environmentTypeName)) {
            type = CONDA;
        } else if (MANUAL.getName().equals(environmentTypeName)) {
            type = MANUAL;
        } else if (BUNDLED.getName().equals(environmentTypeName)) {
            type = BUNDLED;
        } else {
            throw new IllegalStateException("Python environment type '" + environmentTypeName
                + "' is neither Bundled, Conda nor Manual. This is an implementation error.");
        }
        return type;
    }

    private final String m_id;

    private final String m_name;

    PythonEnvironmentType(final String id, final String name) {
        m_id = id;
        m_name = name;
    }

    /**
     * @return The id of this environment type. Suitable for serialization etc.
     */
    public String getId() {
        return m_id;
    }

    /**
     * @return The name of this environment type. Suitable for use in a user interface.
     */
    public String getName() {
        return m_name;
    }
}
