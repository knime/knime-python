/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */

package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * Contains the possible column types.
 *
 * @author Patrick Winter
 */
@SuppressWarnings("javadoc")
public enum Type {

    BOOLEAN(1), BOOLEAN_LIST(2), BOOLEAN_SET(3), INTEGER(4), INTEGER_LIST(5), INTEGER_SET(6), LONG(7), LONG_LIST(8),
    LONG_SET(9), DOUBLE(10), DOUBLE_LIST(11), DOUBLE_SET(12), STRING(13), STRING_LIST(14), STRING_SET(15),
    BYTES(16), BYTES_LIST(17), BYTES_SET(18), FLOAT(19), FLOAT_LIST(20), FLOAT_SET(21);

    private final int m_id;

    /**
     * Constructor.
     *
     * @param id a numeric id for a column type
     */
    private Type(final int id) {
        m_id = id;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public int getId() {
        return m_id;
    }

    /**
     * Get the type associated with a specific id.
     *
     * @param id an id
     * @return the {@link Type} or null if id is unknown
     */
    public static Type getTypeForId(final int id) {
        for (final Type type : Type.values()) {
            if (type.getId() == id) {
                return type;
            }
        }
        return null;
    }
}
