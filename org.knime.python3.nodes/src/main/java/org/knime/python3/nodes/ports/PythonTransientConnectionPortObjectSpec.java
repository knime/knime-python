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
 *   27 Mar 2023 (chaubold): created
 */
package org.knime.python3.nodes.ports;

import java.util.Objects;

import javax.swing.JComponent;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Specification for the {@link PythonTransientConnectionPortObjectSpec}.
 *
 * @since 5.1
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonTransientConnectionPortObjectSpec extends PythonBinaryBlobPortObjectSpec {
    /**
     * The serializer for the PickeledObject portspec type
     */
    public static final class Serializer
        extends AbstractSimplePortObjectSpecSerializer<PythonTransientConnectionPortObjectSpec> {
    }

    // effectively final
    private String m_nodeId;

    // effectively final
    private int m_portIdx;

    /**
     * Deserialization constructor. Fields will be populated in load()
     */
    public PythonTransientConnectionPortObjectSpec() {
    }

    /**
     * Constructor.
     *
     * @param id An ID describing the type of data inside the binary blob
     */
    PythonTransientConnectionPortObjectSpec(final String id, final String data, final String nodeId,
        final int portIdx) {
        super(id, data);
        m_nodeId = nodeId;
        m_portIdx = portIdx;
    }

    /**
     * @param factory The factory to use when creating JSON nodes
     * @return A JSON representation of this {@link PythonTransientConnectionPortObjectSpec}
     */
    @Override
    public JsonNode toJson(final JsonNodeFactory factory) {
        var node = (ObjectNode)super.toJson(factory);
        node.put("node_id", m_nodeId);
        node.put("port_idx", m_portIdx);
        return node;
    }

    /**
     * Construct a {@link PythonBinaryBlobPortObjectSpec} from its JSON representation
     *
     * @param data the JSON data
     * @return a new {@link PythonBinaryBlobPortObjectSpec} object
     */
    public static PythonTransientConnectionPortObjectSpec fromJson(final JsonNode data) { // NOSONAR
        var specData = data.get("data");
        return new PythonTransientConnectionPortObjectSpec(//
            data.get("id").asText(), //
            specData == null ? null : specData.toString(), //
            data.get("node_id").asText(), //
            data.get("port_idx").asInt() //
        );
    }

    @Override
    protected void save(final ModelContentWO model) {
        super.save(model);
        model.addString("node_id", m_nodeId);
        model.addInt("port_idx", m_portIdx);
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        super.load(model);
        m_nodeId = model.getString("node_id", null);
        m_portIdx = model.getInt("port_idx");
    }

    @Override
    public boolean equals(final Object ospec) {
        if (this == ospec) {
            return true;
        }
        if (ospec == null || ospec.getClass() != this.getClass()) {
            return false;
        }
        final PythonTransientConnectionPortObjectSpec spec = (PythonTransientConnectionPortObjectSpec)ospec;

        // Note: we cannot use super.equals for inherited fields as it checks for matching classes
        return Objects.equals(m_id, spec.m_id) //
            && Objects.equals(m_data, spec.m_data) //
            && Objects.equals(m_nodeId, spec.m_nodeId) //
            && m_portIdx == spec.m_portIdx;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_id, m_data, m_nodeId, m_portIdx);
    }

    @Override
    public JComponent[] getViews() {
        String text;
        if (m_id != null) {
            text = "<html><b>" + m_id + " at " + m_nodeId + ":" + m_portIdx + "</b></html>";
        } else {
            text = "No object available";
        }
        return PortObjectSpecUtils.stringViewForSpec(text);
    }
}
