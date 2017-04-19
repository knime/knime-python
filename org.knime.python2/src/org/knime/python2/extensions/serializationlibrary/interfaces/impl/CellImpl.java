package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

public class CellImpl implements Cell {
	
	private Type m_type;
	private Object m_value;
	
	/**
	 * Constructor for use with a Missing Value cell.
	 */
	public CellImpl() {
		m_type = null;
		m_value = null;
	}
	
	public CellImpl(final Boolean value) {
		m_type = Type.BOOLEAN;
		m_value = value;
	}
	
	public CellImpl(final Boolean[] value, final boolean isSet) {
		m_type = isSet ? Type.BOOLEAN_SET : Type.BOOLEAN_LIST;
		m_value = value;
	}
	
	public CellImpl(final Integer value) {
		m_type = Type.INTEGER;
		m_value = value;
	}
	
	public CellImpl(final Integer[] value, final boolean isSet) {
		m_type = isSet ? Type.INTEGER_SET : Type.INTEGER_LIST;
		m_value = value;
	}
	
	public CellImpl(final Long value) {
		m_type = Type.LONG;
		m_value = value;
	}
	
	public CellImpl(final Long[] value, final boolean isSet) {
		m_type = isSet ? Type.LONG_SET : Type.LONG_LIST;
		m_value = value;
	}
	
	public CellImpl(final Double value) {
		m_type = Type.DOUBLE;
		m_value = value;
	}
	
	public CellImpl(final Double[] value, final boolean isSet) {
		m_type = isSet ? Type.DOUBLE_SET : Type.DOUBLE_LIST;
		m_value = value;
	}
	
	public CellImpl(final String value) {
		m_type = Type.STRING;
		m_value = value;
	}
	
	public CellImpl(final String[] value, final boolean isSet) {
		m_type = isSet ? Type.STRING_SET : Type.STRING_LIST;
		m_value = value;
	}
	
	public CellImpl(final Byte[] value) {
		m_type = Type.BYTES;
		m_value = value;
	}
	
	public CellImpl(final Byte[][] value, final boolean isSet) {
		m_type = isSet ? Type.BYTES_SET : Type.BYTES_LIST;
		m_value = value;
	}

	@Override
	public Type getColumnType() {
		return m_type;
	}

	@Override
	public boolean isMissing() {
		return m_type == null;
	}

	@Override
	public Boolean getBooleanValue() throws IllegalStateException {
		if (!(m_value instanceof Boolean)) {
			throw new IllegalStateException("Requested boolean value from cell with type: " + m_type);
		}
		return (Boolean)m_value;
	}

	@Override
	public Boolean[] getBooleanArrayValue() throws IllegalStateException {
		if (!(m_value instanceof Boolean[])) {
			throw new IllegalStateException("Requested boolean array value from cell with type: " + m_type);
		}
		return (Boolean[])m_value;
	}

	@Override
	public Integer getIntegerValue() throws IllegalStateException {
		if (!(m_value instanceof Integer)) {
			throw new IllegalStateException("Requested integer value from cell with type: " + m_type);
		}
		return (Integer)m_value;
	}

	@Override
	public Integer[] getIntegerArrayValue() throws IllegalStateException {
		if (!(m_value instanceof Integer[])) {
			throw new IllegalStateException("Requested integer array value from cell with type: " + m_type);
		}
		return (Integer[])m_value;
	}

	@Override
	public Long getLongValue() throws IllegalStateException {
		if (!(m_value instanceof Long)) {
			throw new IllegalStateException("Requested long value from cell with type: " + m_type);
		}
		return (Long)m_value;
	}

	@Override
	public Long[] getLongArrayValue() throws IllegalStateException {
		if (!(m_value instanceof Long[])) {
			throw new IllegalStateException("Requested long array value from cell with type: " + m_type);
		}
		return (Long[])m_value;
	}

	@Override
	public Double getDoubleValue() throws IllegalStateException {
		if (!(m_value instanceof Double)) {
			throw new IllegalStateException("Requested double value from cell with type: " + m_type);
		}
		return (Double)m_value;
	}

	@Override
	public Double[] getDoubleArrayValue() throws IllegalStateException {
		if (!(m_value instanceof Double[])) {
			throw new IllegalStateException("Requested double array value from cell with type: " + m_type);
		}
		return (Double[])m_value;
	}

	@Override
	public String getStringValue() throws IllegalStateException {
		if (!(m_value instanceof String)) {
			throw new IllegalStateException("Requested string value from cell with type: " + m_type);
		}
		return (String)m_value;
	}

	@Override
	public String[] getStringArrayValue() throws IllegalStateException {
		if (!(m_value instanceof String[])) {
			throw new IllegalStateException("Requested string array value from cell with type: " + m_type);
		}
		return (String[])m_value;
	}

	@Override
	public Byte[] getBytesValue() throws IllegalStateException {
		if (!(m_value instanceof Byte[])) {
			throw new IllegalStateException("Requested bytes value from cell with type: " + m_type);
		}
		return (Byte[])m_value;
	}

	@Override
	public Byte[][] getBytesArrayValue() throws IllegalStateException {
		if (!(m_value instanceof Byte[][])) {
			throw new IllegalStateException("Requested bytes array value from cell with type: " + m_type);
		}
		return (Byte[][])m_value;
	}
	
	@Override
	public String toString(){
		return "Type: " + m_type.toString() + ", Value: " + m_value.toString();
	}

}
