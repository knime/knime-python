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

package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.util.Map;

import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 * The spec of a table iterable by a {@link TableIterator}.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class TableSpecImpl implements TableSpec {

    private final Type[] m_types;

    private final String[] m_names;

    private final Map<String, String> m_columnSerializers;

    /**
     * Constructor.
     *
     * @param types array of column types
     * @param names array of column names
     * @param columnSerializers a map containing column names as keys and the id of the type extension use for
     *            serializing that coulmn as value
     */
    public TableSpecImpl(final Type[] types, final String[] names, final Map<String, String> columnSerializers) {
        m_types = types;
        m_names = names;
        m_columnSerializers = columnSerializers;
    }

    @Override
    public Type[] getColumnTypes() {
        return m_types;
    }

    @Override
    public String[] getColumnNames() {
        return m_names;
    }

    @Override
    public int getNumberColumns() {
        return m_types.length;
    }

    @Override
    public int findColumn(final String name) {
        int index = -1;
        for (int i = 0; i < m_names.length; i++) {
            if (m_names[i].equals(name)) {
                index = i;
            }
        }
        return index;
    }

    @Override
    public Map<String, String> getColumnSerializers() {
        return m_columnSerializers;
    }

}
