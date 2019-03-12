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
 *   Feb 3, 2019 (marcel): created
 */
package org.knime.python2;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public enum PythonVersion {

        /**
         * Python 2
         */
        PYTHON2("python2", "Python 2"),

        /**
         * Python 3
         */
        PYTHON3("python3", "Python 3");

    /**
     * @param versionId The {@link #getId() id} of the {@link PythonVersion} to return.
     * @return The type of the given id.
     */
    public static PythonVersion fromId(final String versionId) {
        final PythonVersion type;
        if (PYTHON2.getId().equals(versionId)) {
            type = PYTHON2;
        } else if (PYTHON3.getId().equals(versionId)) {
            type = PYTHON3;
        } else {
            throw new IllegalStateException("Python version '" + versionId + "' is neither Python 2 nor Python 3. This "
                + "is an implementation error.");
        }
        return type;
    }

    /**
     * @param versionName The {@link #getName() name} of the {@link PythonVersion} to return.
     * @return The type of the given name.
     */
    public static PythonVersion fromName(final String versionName) {
        final PythonVersion type;
        if (PYTHON2.getName().equals(versionName)) {
            type = PYTHON2;
        } else if (PYTHON3.getName().equals(versionName)) {
            type = PYTHON3;
        } else {
            throw new IllegalStateException("Python version '" + versionName + "' is neither Python 2 nor Python 3. "
                + "This is an implementation error.");
        }
        return type;
    }

    private final String m_id;

    private final String m_name;

    private PythonVersion(final String id, final String name) {
        m_id = id;
        m_name = name;
    }

    /**
     * @return The id of this Python version. Suitable for serialization etc.
     */
    public String getId() {
        return m_id;
    }

    /**
     * @return The friendly name of this Python version. Suitable for use in a user interface.
     */
    public String getName() {
        return m_name;
    }
}
