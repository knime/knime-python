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
	private String m_pickledObject;
	private String m_type;
	private String m_stringRepresentation;

	public PickledObject(final String pickledObject, final String type, final String stringRepresentation) {
		m_pickledObject = pickledObject;
		m_type = type;
		m_stringRepresentation = stringRepresentation;
	}

	public PickledObject(final ModelContentRO model) throws InvalidSettingsException {
		m_pickledObject = model.getString(CFG_PICKLED_OBJECT);
		m_type = model.getString(CFG_TYPE);
		m_stringRepresentation = model.getString(CFG_STRING_REPRESENTATION);
	}

	public String getPickledObject() {
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
		model.addString(CFG_PICKLED_OBJECT, m_pickledObject);
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
		PickledObject con = (PickledObject) obj;
		EqualsBuilder eb = new EqualsBuilder();
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
		HashCodeBuilder hcb = new HashCodeBuilder();
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
