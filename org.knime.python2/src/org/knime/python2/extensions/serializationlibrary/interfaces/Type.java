package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * Contains the possible column types.
 * 
 * @author Patrick Winter
 */
public enum Type {
	
	BOOLEAN(1),
	BOOLEAN_LIST(2),
	BOOLEAN_SET(3),
	INTEGER(4),
	INTEGER_LIST(5),
	INTEGER_SET(6),
	LONG(7),
	LONG_LIST(8),
	LONG_SET(9),
	DOUBLE(10),
	DOUBLE_LIST(11),
	DOUBLE_SET(12),
	STRING(13),
	STRING_LIST(14),
	STRING_SET(15),
	BYTES(16),
	BYTES_LIST(17),
	BYTES_SET(18);
	
	private final int m_id;
	
	private Type(final int id) {
		m_id = id;
	}
	
	public int getId() {
		return m_id;
	}
	
	public static Type getTypeForId(final int id) {
		for (Type type : Type.values()) {
			if (type.getId() == id) {
				return type;
			}
		}
		return null;
	}

}
