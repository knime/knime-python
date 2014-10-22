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
 *   Mar 19, 2014 ("Patrick Winter"): created
 */
package org.knime.python.port;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec;

/**
 * Specification for the {@link PickledObjectPortObject}.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public final class PickledObjectPortObjectSpec extends AbstractSimplePortObjectSpec {

	private String m_pickledObjectType;
	private String m_pickledObjectString;

	/**
	 * Constructor for a port object spec that holds no {@link PickledObject}.
	 */
	public PickledObjectPortObjectSpec() {
		m_pickledObjectType = null;
		m_pickledObjectString = null;
	}

	/**
	 * @param pickledObject
	 *            The type of the {@link PickledObject} that will be contained by this port
	 *            object
	 */
	public PickledObjectPortObjectSpec(final String pickledObjectType, final String pickledObjectString) {
		m_pickledObjectType = pickledObjectType;
		m_pickledObjectString = pickledObjectString;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void save(final ModelContentWO model) {
		model.addString("pickledObjectType", m_pickledObjectType);
		model.addString("pickledObjectString", m_pickledObjectString);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void load(final ModelContentRO model) throws InvalidSettingsException {
		m_pickledObjectType = model.getString("pickledObjectType", null);
		m_pickledObjectString = model.getString("pickledObjectString", null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object ospec) {
		if (this == ospec) {
			return true;
		}
		if (!(ospec instanceof PickledObjectPortObjectSpec)) {
			return false;
		}
		PickledObjectPortObjectSpec spec = (PickledObjectPortObjectSpec) ospec;
		return m_pickledObjectType.equals(spec.m_pickledObjectType) && m_pickledObjectString.equals(spec.m_pickledObjectString);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder();
		hcb.append(m_pickledObjectType);
		hcb.append(m_pickledObjectString);
		return hcb.hashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JComponent[] getViews() {
		String text;
		if (m_pickledObjectType != null) {
			text = "<html><b>" + m_pickledObjectType
					+ "</b><br><br><code>"
					+ m_pickledObjectString.replace("\n", "<br>") + "</code></html>";
		} else {
			text = "No object available";
		}
		JLabel label = new JLabel(text);
		Font font = label.getFont();
		Font plainFont = new Font(font.getFontName(), Font.PLAIN, font.getSize());
		label.setFont(plainFont);
		JPanel panel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
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
		JComponent f = new JScrollPane(panel);
		f.setName("Pickled object");
		return new JComponent[] { f };
	}

}
