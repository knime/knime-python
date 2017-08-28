/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package org.knime.python.port;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

public class PickledObject {

    private static final String CFG_PICKLED_OBJECT = "pickledObject";
    private static final String CFG_TYPE = "type";
    private static final String CFG_STRING_REPRESENTATION = "stringRepresentation";
    private final byte[] m_pickledObject;
    private final String m_type;
    private final String m_stringRepresentation;

    public PickledObject(final byte[] pickledObject, final String type, final String stringRepresentation) {
        m_pickledObject = pickledObject;
        m_type = type;
        m_stringRepresentation = stringRepresentation;
    }

    public PickledObject(final ModelContentRO model) throws InvalidSettingsException {
        m_pickledObject = model.getByteArray(CFG_PICKLED_OBJECT);
        m_type = model.getString(CFG_TYPE);
        m_stringRepresentation = model.getString(CFG_STRING_REPRESENTATION);
    }

    public byte[] getPickledObject() {
        return m_pickledObject;
    }

    public String getType() {
        return m_type;
    }

    public String getStringRepresentation() {
        return m_stringRepresentation;
    }

    public boolean isNone() {
        return m_type.equals("NoneType");
    }

    public void save(final ModelContentWO model) {
        model.addByteArray(CFG_PICKLED_OBJECT, m_pickledObject);
        model.addString(CFG_TYPE, m_type);
        model.addString(CFG_STRING_REPRESENTATION, m_stringRepresentation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PickledObject)) {
            return false;
        }
        final PickledObject con = (PickledObject) obj;
        final EqualsBuilder eb = new EqualsBuilder();
        eb.append(m_pickledObject, con.m_pickledObject);
        eb.append(m_type, con.m_type);
        eb.append(m_stringRepresentation, con.m_stringRepresentation);
        return eb.isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(m_pickledObject);
        hcb.append(m_type);
        hcb.append(m_stringRepresentation);
        return hcb.hashCode();
    }

    @Override
    public String toString() {
        return m_type + "\n" + m_stringRepresentation;
    }

}
