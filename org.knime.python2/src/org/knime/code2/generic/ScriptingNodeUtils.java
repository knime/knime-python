package org.knime.code2.generic;

/**
 * Utility methods for scripting nodes.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class ScriptingNodeUtils {

	/**
	 * The maximum length a string that will be added to the console can have
	 * (everything else will be truncated).
	 */
	private static final int DEFAULT_MAX_STRING_LENGTH = 100000;
	
	/**
	 * Truncate the given string if necessary.
	 * 
	 * Works the same as {@link #shortenString(String, int)} with maxLength of 100000.
	 * 
	 * @param originalString
	 *            The string that may be to long
	 * @return The original string or a truncated version if the original's
	 *         length is bigger than the defined maximum
	 */
	public static String shortenString(final String originalString) {
		return shortenString(originalString, DEFAULT_MAX_STRING_LENGTH);
	}

	/**
	 * Truncate the given string if necessary.
	 * 
	 * @param originalString
	 *            The string that may be to long
	 * @param maxLength The maximum number of characters
	 * @return The original string or a truncated version if the original's
	 *         length is bigger than the defined maximum
	 */
	public static String shortenString(final String originalString, final int maxLength) {
		String string = originalString;
		if (originalString.length() > maxLength) {
			string = originalString.substring(0, maxLength);
			string += "\nReached maximum output limit, omitted "
					+ (originalString.length() - maxLength) + " characters";
		}
		return string;
	}

}
