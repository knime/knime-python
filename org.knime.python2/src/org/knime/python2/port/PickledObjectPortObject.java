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
 *
 * History
 *   Mar 19, 2014 ("Patrick Winter"): created
 */
package org.knime.python2.port;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * Port object containing a {@link PickledObject}.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @deprecated since 3.6.1 - use {@link PickledObjectFileStorePortObject} for performance reasons
 */
@Deprecated
public final class PickledObjectPortObject extends AbstractSimplePortObject {
    /**
     * The serializer for the PickeledObject port type
     */
    public static final class Serializer extends AbstractSimplePortObjectSerializer<PickledObjectPortObject> {
    }

    private PickledObjectPortObjectSpec m_spec;

    private PickledObject m_pickledObject;

    /**
     * The type of this port.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(PickledObjectPortObject.class);

    /**
     * Constructor used by the framework.
     */
    public PickledObjectPortObject() {
        m_pickledObject = null;
    }

    /**
     * Constructor.
     *
     * @param pickledObject a pickeled object
     */
    public PickledObjectPortObject(final PickledObject pickledObject) {
        m_pickledObject = pickledObject;
        m_spec = new PickledObjectPortObjectSpec(m_pickledObject.getType(), m_pickledObject.getStringRepresentation());
    }

    /**
     * @return The contained PickledObject
     */
    public PickledObject getPickledObject() {
        return m_pickledObject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        return m_pickledObject.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObjectSpec getSpec() {
        return m_spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void save(final ModelContentWO model, final ExecutionMonitor exec) throws CanceledExecutionException {
        m_pickledObject.save(model);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void load(final ModelContentRO model, final PortObjectSpec spec, final ExecutionMonitor exec)
            throws InvalidSettingsException, CanceledExecutionException {
        m_spec = (PickledObjectPortObjectSpec)spec;
        m_pickledObject = new PickledObject(model);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        String text;
        if (getPickledObject() != null) {
            String pickledObject = getPickledObject().getStringRepresentation();
            pickledObject = shortenString(pickledObject, 1000, "\n...");
            text = "<html><b>" + getPickledObject().getType() + "</b><br><br><code>"
                    + pickledObject.replace("\n", "<br>") + "</code></html>";
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
        f.setName("Pickled object");
        return new JComponent[]{f};
    }

    private static String shortenString(String string, final int maxLength, final String suffix) {
        if (string.length() > maxLength) {
            string = string.substring(0, maxLength - suffix.length()) + suffix;
        }
        return string;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PickledObjectPortObject)) {
            return false;
        }
        final PickledObjectPortObject portObject = (PickledObjectPortObject)o;
        return m_pickledObject.equals(portObject.m_pickledObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_pickledObject != null ? m_pickledObject.hashCode() : 0;
    }

}
