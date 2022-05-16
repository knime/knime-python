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
 *   4 May 2022 (chaubold): created
 */
package org.knime.python3.nodes.ports;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Objects;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Specification for the {@link PythonBinaryBlobPortObjectSpec}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonBinaryBlobPortObjectSpec extends AbstractSimplePortObjectSpec {
    /**
     * The serializer for the PickeledObject portspec type
     */
    public static final class Serializer extends AbstractSimplePortObjectSpecSerializer<PythonBinaryBlobPortObjectSpec> {
    }

    // effectively final
    private String m_id = null;

    /**
     * Deserialization constructor. Fields will be populated in load()
     */
    public PythonBinaryBlobPortObjectSpec() {
    }

    /**
     * Constructor.
     *
     * @param id An ID describing the type of data inside the binary blob
     */
    public PythonBinaryBlobPortObjectSpec(final String id) {
        m_id = id;
    }

    String getId() {
        return m_id;
    }

    /**
     * @param factory The factory to use when creating JSON nodes
     * @return A JSON representation of this {@link PythonBinaryBlobPortObjectSpec}
     */
    public JsonNode toJson(final JsonNodeFactory factory) {
        final var node = factory.objectNode();
        node.put("id", m_id);
        return node;
    }

    /**
     * Construct a {@link PythonBinaryBlobPortObjectSpec} from its JSON representation
     * @param data the JSON data
     * @return a new {@link PythonBinaryBlobPortObjectSpec} object
     */
    public static PythonBinaryBlobPortObjectSpec fromJson(final JsonNode data) {
        return new PythonBinaryBlobPortObjectSpec(data.get("id").asText());
    }

    @Override
    protected void save(final ModelContentWO model) {
        model.addString("id", m_id);
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        m_id = model.getString("id", null);
    }

    @Override
    public boolean equals(final Object ospec) {
        if (this == ospec) {
            return true;
        }
        if (!(ospec instanceof PythonBinaryBlobPortObjectSpec)) {
            return false;
        }
        final PythonBinaryBlobPortObjectSpec spec = (PythonBinaryBlobPortObjectSpec)ospec;
        return m_id.equals(spec.m_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        String text;
        if (m_id != null) {
            text = "<html><b>" + m_id + "</b></html>";
        } else {
            text = "No object available";
        }
        final JLabel label = new JLabel(text);
        final Font font = label.getFont();
        final Font plainFont = new Font(font.getFontName(), Font.PLAIN, font.getSize());
        label.setFont(plainFont);
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(label, gbc);
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridy++;
        gbc.weighty = Double.MIN_VALUE;
        gbc.weightx = Double.MIN_VALUE;
        panel.add(new JLabel(), gbc);
        final JComponent f = new JScrollPane(panel);
        f.setName("Python Binary Data");
        return new JComponent[]{f};
    }

}